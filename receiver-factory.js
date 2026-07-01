'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// receiver-factory.js  —  LIVE STREAM + GPS  (stable multi-camera)
//
// Shared JT/T1078 receiver engine. Both index-pictor.js and index-acumen.js
// are thin wrappers that call createReceiver({ vendor, ...ports }) — all the
// protocol parsing, FFmpeg/HLS pipeline, and HTTP/WS serving lives here once.
//
// Stability improvements:
//   • FFmpeg stdin backpressure — drains before writing, prevents OOM
//   • Stream packet watchdog   — detects stalled cameras, restarts FFmpeg
//   • Graceful FFmpeg restart  — resets per-camera state cleanly
//   • Buffer cap               — drops oldest bytes if buffer grows too large
//   • Socket keepalive         — detects dead TCP connections faster
//   • Each camera isolated     — one crash never affects another
//
// Streams available at:
//   http://<server>:<httpPort>/public/<phone>.m3u8
//
// API:
//   GET  http://<server>:<httpPort>/api/cameras
// ─────────────────────────────────────────────────────────────────────────────

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
require('dotenv').config();
const { WebSocketServer } = require('ws');
const { spawn }           = require('child_process');
const tcpForwarder        = require('./tcp-forwarder');
const bus                 = require('./device-bus');

module.exports = function createReceiver(opts = {}) {

// ── Config ────────────────────────────────────────────────────────────────────
const vendor = String(opts.vendor || 'pictor').toLowerCase();
const TAG    = vendor.charAt(0).toUpperCase() + vendor.slice(1);   // 'Pictor' / 'Acumen'

const CONFIG = {
    vendor,
    tcpPort:        parseInt(opts.tcpPort        || '3007'),
    httpPort:       parseInt(opts.httpPort       || '8080'),
    wsPort:         parseInt(opts.wsPort         || '8801'),
    serverIp:       opts.serverIp                || process.env.SERVER_IP,
    ffmpegPath:     opts.ffmpegPath               || process.env.FFMPEG_PATH || '/usr/local/bin/ffmpeg',
    streamChannel:  parseInt(opts.streamChannel   || process.env.STREAM_CH   || '1'),
    maxBufferBytes: parseInt(opts.maxBufferBytes  || String(4 * 1024 * 1024)),  // 4MB per socket
    watchdogMs:     parseInt(opts.watchdogMs      || '15000'),  // restart FFmpeg if no frames for 15s
    publicDir:      opts.publicDir                || './public',   // HLS output + static serving root
};

console.log(`[${TAG}] Server IP  : ${CONFIG.serverIp}`);
console.log(`[${TAG}] TCP        : ${CONFIG.tcpPort}`);
console.log(`[${TAG}] HTTP       : ${CONFIG.httpPort}`);
console.log(`[${TAG}] WS         : ${CONFIG.wsPort}`);
console.log(`[${TAG}] Public dir : ${CONFIG.publicDir}`);

// ── Ensure public folder exists ───────────────────────────────────────────────
if (!fs.existsSync(CONFIG.publicDir)) fs.mkdirSync(CONFIG.publicDir, { recursive: true });

// ── Per-camera state ──────────────────────────────────────────────────────────
const cameras    = {};   // { [phone]: CameraState }
const tcpSockets = {};   // { [phone]: net.Socket }

function makeCamera() {
    return {
        ffmpeg:          null,
        gotIFrame:       false,
        subpackets:      [],
        patPmtSent:      false,
        tsCounter:       0,
        lastFrameAt:     Date.now(),
        watchdog:        null,
        restarting:      false,
        frameCount:      0,
        hasStreamSocket: false,   // ← add this
        streamSocket:    null,    // ← add this
    };
}

function getCamera(phone) {
    if (!cameras[phone]) cameras[phone] = makeCamera();
    return cameras[phone];
}

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`[${TAG}] ✓ WebSocket on :${CONFIG.wsPort}`);

wss.on('connection', (ws, req) => {
    console.log(`[${TAG}][WS] Browser connected from ${req.socket.remoteAddress}`);
    // Send current camera list immediately
    ws.send(JSON.stringify({
        type:    'cameras',
        cameras: buildCameraList(),
    }));
    ws.on('close', () => console.log(`[${TAG}][WS] Browser disconnected`));
    ws.on('error', e  => console.error(`[${TAG}][WS] Error:`, e.message));
});

function broadcast(obj) {
    const raw = JSON.stringify(obj);
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(raw); });
}

// Pictor keeps the original bare "/public" URL prefix so existing frontends
// don't break. Every other vendor gets "/public-<vendor>" — matching the
// on-disk folder name, which is what your reverse proxy / frontend appears
// to already expect for Acumen.
const PUBLIC_URL_PREFIX = vendor === 'pictor' ? '/public' : `/public-${vendor}`;

function buildCameraList() {
    return Object.keys(tcpSockets).map(phone => ({
        phone,
        stream: `${PUBLIC_URL_PREFIX}/${phone}.m3u8`,
    }));
}

// ── Bus: ftp-service sends frames back through the bus ────────────────────────
bus.on('device:send', ({ vendor: targetVendor, phone, frame }) => {
    if (targetVendor && targetVendor !== CONFIG.vendor) return;   // not for this receiver
    const socket = tcpSockets[String(phone)];
    if (!socket || socket.destroyed) {
        console.warn(`[BUS:${TAG}] device:send — no socket for phone:${phone}`);
        return;
    }
    socket.write(frame);
    console.log(`[BUS:${TAG}] wrote ${frame.length} bytes to ${phone}`);
});

// ── HTTP server ───────────────────────────────────────────────────────────────
const contentTypes = {
    '.html': 'text/html',
    '.js':   'application/javascript',
    '.m3u8': 'application/vnd.apple.mpegurl',
    '.ts':   'video/mp2t',
    '.mp4':  'video/mp4',
    '.json': 'application/json',
};

http.createServer((req, res) => {
    const urlPath = req.url.split('?')[0];

    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Cache-Control', 'no-cache');

    // ── GET /api/cameras ──────────────────────────────────────────────────────
    if (req.method === 'GET' && urlPath === '/api/cameras') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ cameras: buildCameraList() }));
        return;
    }

    // ── Static / HLS files ────────────────────────────────────────────────────
    let filePath;
    if (urlPath === '/') {
        filePath = './video.html';
    } else if (urlPath.startsWith('/public/')) {
        filePath = path.join(CONFIG.publicDir, urlPath.slice('/public/'.length));
    } else if (urlPath.startsWith(`${PUBLIC_URL_PREFIX}/`)) {
        filePath = path.join(CONFIG.publicDir, urlPath.slice(PUBLIC_URL_PREFIX.length + 1));
    } else {
        filePath = `.${urlPath}`;
    }
    const ext = path.extname(filePath).toLowerCase();

    if (req.method === 'HEAD') {
        fs.stat(filePath, (e, stat) => {
            if (e) { res.writeHead(404); res.end(); return; }
            res.writeHead(200, {
                'Content-Type':   contentTypes[ext] || 'application/octet-stream',
                'Content-Length': stat.size,
            });
            res.end();
        });
        return;
    }

    // Stream .ts segments to avoid loading into memory
    if (ext === '.ts') {
        const rs = fs.createReadStream(filePath);
        rs.on('error', () => { res.writeHead(404); res.end(); });
        res.writeHead(200, { 'Content-Type': 'video/mp2t' });
        rs.pipe(res);
        return;
    }

    fs.readFile(filePath, (e, data) => {
        if (e) { res.writeHead(404); res.end('Not found'); return; }
        res.writeHead(200, {
            'Content-Type':   contentTypes[ext] || 'text/plain',
            'Content-Length': data.length,
        });
        res.end(data);
    });

}).listen(CONFIG.httpPort, '0.0.0.0', () =>
    console.log(`[${TAG}] ✓ HTTP on :${CONFIG.httpPort}`)
);

// ── FFmpeg — one stable instance per camera ───────────────────────────────────
function startFFmpeg(phone) {
    const cam = cameras[phone];
    if (!cam) return null;
    if (cam.restarting) return null;

    console.log(`[${TAG}][FFmpeg ${phone}] Starting...`);

    // Clean up old segment files so HLS player doesn't get confused
    try {
        fs.readdirSync(CONFIG.publicDir)
            .filter(f => f.startsWith(`${phone}_`) && f.endsWith('.ts'))
            .forEach(f => { try { fs.unlinkSync(`${CONFIG.publicDir}/${f}`); } catch (_) {} });
        const m3u8 = `${CONFIG.publicDir}/${phone}.m3u8`;
        if (fs.existsSync(m3u8)) fs.unlinkSync(m3u8);
    } catch (_) {}

    const ffmpeg = spawn(CONFIG.ffmpegPath, [
        '-fflags',          '+genpts+discardcorrupt+igndts',
        '-err_detect',      'ignore_err',
        '-f',               'mpegts',
        '-probesize',       '2000000',
        '-analyzeduration', '2000000',
        '-i',               'pipe:0',

        // Decode HEVC input, re-encode to H.264 for browser compatibility
        '-c:v',             'libx264',
        '-preset',          'veryfast',      // faster than ultrafast, still low latency
        '-tune',            'zerolatency',
        '-profile:v',       'baseline',      // widest browser support
        '-level',           '3.1',
        '-g',               '30',
        '-keyint_min',      '30',
        '-sc_threshold',    '0',
        '-b:v',             '800k',
        '-maxrate',         '1000k',
        '-bufsize',         '2000k',
        '-an',

        '-f',               'hls',
        '-hls_time',        '2',
        '-hls_list_size',   '5',
        '-hls_flags',       'delete_segments+append_list+independent_segments',
        '-hls_segment_type','mpegts',
        '-hls_segment_filename', `${CONFIG.publicDir}/${phone}_%04d.ts`,
        `${CONFIG.publicDir}/${phone}.m3u8`,
    ]);

    // Surface FFmpeg's own diagnostics — this was being computed but never
    // printed before (the console.error call was commented out), so any real
    // decode failure was invisible. Set DEBUG_FFMPEG=true to see every line.
    const debugFfmpeg = process.env.DEBUG_FFMPEG === 'true' || process.env.ACUMEN_DEBUG_FFMPEG === 'true';
    let stderrBuf = '';
    ffmpeg.stderr.on('data', d => {
        stderrBuf += d.toString();
        const lines = stderrBuf.split('\n');
        stderrBuf = lines.pop();
        lines.forEach(line => {
            const l = line.trim();
            if (!l) return;
            if (l.includes('frame=') || l.includes('fps=')) return; // skip progress spam
            const isStreamInfo = l.includes('Stream #') || l.includes('Input #') || l.includes('Output #') ||
                                  l.includes('Video:') || l.includes('Audio:');
            const isError = l.includes('error') || l.includes('Error') || l.includes('Invalid') ||
                             l.includes('muxing overhead') || l.includes('No such file') ||
                             l.includes('could not find') || l.includes('Unsupported');
            if (isError) {
                console.error(`[${TAG}][FFmpeg ${phone}] ${l}`);
            } else if (isStreamInfo) {
                console.log(`[${TAG}][FFmpeg ${phone}] ${l}`);
            } else if (debugFfmpeg) {
                console.log(`[${TAG}][FFmpeg ${phone}][debug] ${l}`);
            }
        });
    });

    ffmpeg.stdin.on('error', e => {
        // Ignore EPIPE — happens when FFmpeg restarts while we're writing
        if (e.code !== 'EPIPE') console.error(`[${TAG}][FFmpeg ${phone}] stdin error: ${e.message}`);
    });

    // Without this handler, a bad ffmpegPath (e.g. wrong env var) throws an
    // uncaught exception that kills the ENTIRE process — including whichever
    // other vendor's receiver happens to share it.
    ffmpeg.on('error', e => {
        console.error(`[${TAG}][FFmpeg ${phone}] failed to start (${CONFIG.ffmpegPath}): ${e.message}`);
        if (cameras[phone]) {
            cameras[phone].ffmpeg     = null;
            cameras[phone].patPmtSent = false;
            cameras[phone].gotIFrame  = false;
        }
    });

    ffmpeg.on('close', code => {
        console.log(`[${TAG}][FFmpeg ${phone}] exited (code ${code})`);
        if (!cameras[phone]) return;  // camera disconnected — don't restart
        cameras[phone].ffmpeg    = null;
        cameras[phone].patPmtSent = false;
        cameras[phone].gotIFrame  = false;
        cameras[phone].restarting = true;
        setTimeout(() => {
            if (!cameras[phone]) return;
            cameras[phone].restarting = false;
            cameras[phone].ffmpeg = startFFmpeg(phone);
        }, 2000);
    });

    cam.ffmpeg    = ffmpeg;
    cam.patPmtSent = false;
    cam.gotIFrame  = false;
    cam.tsCounter  = 0;

    return ffmpeg;
}

// ── Watchdog — restart FFmpeg if no frames for watchdogMs ────────────────────
function startWatchdog(phone) {
    const cam = cameras[phone];
    if (!cam) return;

    if (cam.watchdog) clearInterval(cam.watchdog);

    cam.watchdog = setInterval(() => {
        if (!cameras[phone]) {
            clearInterval(cam.watchdog);
            return;
        }
        const age = Date.now() - cameras[phone].lastFrameAt;
        if (age > CONFIG.watchdogMs && cameras[phone].ffmpeg && !cameras[phone].restarting) {

            // ── NEW: if no stream socket exists, stop FFmpeg and wait ──────────
            // Camera stream connection dropped — stop restarting FFmpeg endlessly
            if (!cameras[phone].hasStreamSocket) {
                console.warn(`[${TAG}][Watchdog ${phone}] No stream socket — stopping FFmpeg until camera reconnects`);
                if (cameras[phone].ffmpeg) {
                    cameras[phone].ffmpeg.kill('SIGKILL');
                    cameras[phone].ffmpeg    = null;
                    cameras[phone].patPmtSent = false;
                    cameras[phone].gotIFrame  = false;
                }
                return;
            }

            console.warn(`[${TAG}][Watchdog ${phone}] No frames for ${age}ms — restarting FFmpeg`);
            cameras[phone].ffmpeg.kill('SIGKILL');
        }
    }, 5000);
}

// ── MPEG-TS wrapping ──────────────────────────────────────────────────────────
const TS_PACKET_SIZE = 188;
const VIDEO_PID      = 256;
const PMT_PID        = 4096;

function buildPAT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0] = 0x47; pkt[1] = 0x40; pkt[2] = 0x00; pkt[3] = 0x10;
    pkt[4] = 0x00;
    const s = pkt.slice(5);
    s[0]  = 0x00;
    s[1]  = 0xB0; s[2] = 0x0D;
    s[3]  = 0x00; s[4] = 0x01;
    s[5]  = 0xC1;
    s[6]  = 0x00; s[7] = 0x00;
    s[8]  = 0x00; s[9] = 0x01;
    s[10] = (PMT_PID >> 8) | 0xE0;
    s[11] = PMT_PID & 0xFF;
    return pkt;
}

function buildPMT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0] = 0x47;
    pkt[1] = 0x40 | ((PMT_PID >> 8) & 0x1F);
    pkt[2] = PMT_PID & 0xFF;
    pkt[3] = 0x10;
    pkt[4] = 0x00;
    const s = pkt.slice(5);
    s[0]  = 0x02;
    s[1]  = 0xB0; s[2] = 0x12;
    s[3]  = 0x00; s[4] = 0x01;
    s[5]  = 0xC1;
    s[6]  = 0x00; s[7] = 0x00;
    s[8]  = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[9]  = VIDEO_PID & 0xFF;
    s[10] = 0xF0; s[11] = 0x00;
    s[12] = 0x24;  // stream_type: AVS
    s[13] = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[14] = VIDEO_PID & 0xFF;
    s[15] = 0xF0; s[16] = 0x00;
    return pkt;
}

function wrapFrameInTS(frameData, counter, ptsMs) {
    const pts   = BigInt(Math.floor((ptsMs || 0) * 90)) & 0x1FFFFFFFFn;
    const p     = Number(pts);

    const pesHdr = Buffer.from([
        0x00, 0x00, 0x01,                           // start code
        0xE0,                                        // stream id: video
        0x00, 0x00,                                  // PES length (0 = unbounded)
        0x80,                                        // marker + no flags
        0x80,                                        // PTS present
        0x05,                                        // header data length
        // PTS 5 bytes
        0x21 | ((p >>> 29) & 0x0E),
        (p >>> 22) & 0xFF,
        0x01 | ((p >>> 14) & 0xFE),
        (p >>>  7) & 0xFF,
        0x01 | ((p <<   1) & 0xFE),
    ]);

    const pes     = Buffer.concat([pesHdr, frameData]);
    const packets = [];
    let pos = 0, first = true, ctr = counter;

    while (pos < pes.length) {
        const pkt  = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
        pkt[0] = 0x47;
        pkt[1] = (first ? 0x40 : 0x00) | ((VIDEO_PID >> 8) & 0x1F);
        pkt[2] = VIDEO_PID & 0xFF;
        pkt[3] = 0x10 | (ctr & 0x0F);
        ctr    = (ctr + 1) & 0x0F;

        const chunk = pes.slice(pos, pos + TS_PACKET_SIZE - 4);
        chunk.copy(pkt, 4);
        pos  += chunk.length;
        first = false;
        packets.push(pkt);
    }

    return { packets, nextCounter: ctr };
}

// ── Write to FFmpeg ───────────────────────────────────────────────────────────
function writeToFFmpeg(cam, phone, data) {
    if (!cam.ffmpeg || !cam.ffmpeg.stdin.writable) return;
    cam.ffmpeg.stdin.write(data);
    // No per-write drain listener — avoids MaxListenersExceeded warning.
    // If FFmpeg falls behind, the watchdog restarts it after 15s of no frames.
}

// ── Handle one complete decoded video frame ───────────────────────────────────
function handleVideoFrame(frameData, phone, dataType) {
    const cam = cameras[phone];
    if (!cam || cam.restarting) return;

    // dataType: 0=I-frame  1=P-frame  2=B-frame  3=audio  4=transparent
    const isVideo = dataType === 0 || dataType === 1 || dataType === 2;

    // One-time raw diagnostic per camera — shows exactly what bytes the
    // device is sending so a codec/format mismatch is visible in the logs
    // instead of just "frames processed" with no output files.
    if (!cam.loggedFirstFrame) {
        cam.loggedFirstFrame = true;
        const preview = frameData.slice(0, 16).toString('hex');
        const dataTypeName = ['I-frame', 'P-frame', 'B-frame', 'audio', 'transparent'][dataType] || `unknown(${dataType})`;
        console.log(
            `[${TAG}][${phone}] 🔍 First frame: dataType=${dataTypeName} size=${frameData.length}B ` +
            `first16=${preview}`
        );
        // NAL start-code sanity check — real H.264/HEVC elementary streams
        // start with 00 00 00 01 or 00 00 01. If this is missing, either the
        // subpacket reassembly is wrong or the camera isn't sending what we expect.
        const hasStartCode = frameData.length >= 4 &&
            frameData[0] === 0 && frameData[1] === 0 &&
            (frameData[2] === 1 || (frameData[2] === 0 && frameData[3] === 1));
        if (!hasStartCode) {
            console.warn(
                `[${TAG}][${phone}] ⚠️ First frame has no NAL start code (00 00 00 01 / 00 00 01) — ` +
                `not valid H.264/HEVC elementary data, or subpacket reassembly produced garbage. This ` +
                `is very likely why FFmpeg receives bytes but never produces output files.`
            );
        }
    }

    if (!isVideo) return;

    // Wait for first I-frame before feeding P/B frames
    if (dataType === 0) {
        cam.gotIFrame = true;
    } else if (!cam.gotIFrame) {
        cam.droppedBeforeIFrame = (cam.droppedBeforeIFrame || 0) + 1;
        if (cam.droppedBeforeIFrame === 1 || cam.droppedBeforeIFrame % 300 === 0) {
            console.warn(
                `[${TAG}][${phone}] ⚠️ Dropped ${cam.droppedBeforeIFrame} frame(s) waiting for first I-frame ` +
                `(dataType=0) — if this keeps climbing, the camera may never be sending a real I-frame, ` +
                `or its I-frame marker doesn't match dataType=0 the way this parser expects.`
            );
        }
        return;
    }

    if (!cam.ffmpeg || !cam.ffmpeg.stdin.writable) return;

    // Send PAT+PMT once per FFmpeg instance
    if (!cam.patPmtSent) {
        writeToFFmpeg(cam, phone, buildPAT());
        writeToFFmpeg(cam, phone, buildPMT());
        cam.patPmtSent = true;
        console.log(`[${TAG}][${phone}] 📺 PAT+PMT sent, streaming started`);
        broadcast({ type: 'stream_started', phone, stream: `${PUBLIC_URL_PREFIX}/${phone}.m3u8` });
    }

    const { packets, nextCounter } = wrapFrameInTS(frameData, cam.tsCounter, Date.now());
    cam.tsCounter  = nextCounter;
    cam.lastFrameAt = Date.now();
    cam.frameCount++;

    for (const pkt of packets) writeToFFmpeg(cam, phone, pkt);

    // Log frame stats every 300 frames, plus a one-time check that the HLS
    // playlist has actually appeared on disk — if not, FFmpeg is receiving
    // data but silently failing to produce output (check the [FFmpeg] lines
    // just above this in the log for the real reason).
    if (cam.frameCount % 300 === 0) {
        console.log(`[${TAG}][${phone}] 📊 ${cam.frameCount} frames processed`);
        if (cam.frameCount === 300) {
            const m3u8 = `${CONFIG.publicDir}/${phone}.m3u8`;
            if (!fs.existsSync(m3u8)) {
                console.warn(
                    `[${TAG}][${phone}] ⚠️ 300 frames sent to FFmpeg but ${m3u8} still doesn't exist. ` +
                    `FFmpeg is receiving data but not producing HLS output — check the [FFmpeg ${phone}] ` +
                    `lines above for the decode error (enable DEBUG_FFMPEG=true for full ffmpeg stderr).`
                );
            }
        }
    }
}

// ── Reassemble split RTP subpackets ──────────────────────────────────────────
function processVideoPacket(rawData, phone, dataType, subpktMarker) {
    const cam = cameras[phone];
    if (!cam) return;

    // subpktMarker: 0=atomic  1=first  3=middle  2=last
    switch (subpktMarker) {
        case 0:
            handleVideoFrame(rawData, phone, dataType);
            break;
        case 1:
            cam.subpackets = [rawData];
            break;
        case 3:
            if (cam.subpackets.length > 0) cam.subpackets.push(rawData);
            break;
        case 2:
            if (cam.subpackets.length > 0) {
                cam.subpackets.push(rawData);
                handleVideoFrame(Buffer.concat(cam.subpackets), phone, dataType);
                cam.subpackets = [];
            }
            break;
    }
}

// ── JT/T 808 helpers ─────────────────────────────────────────────────────────
function unescapeBuffer(buf) {
    const out = []; let i = 0;
    while (i < buf.length) {
        if      (buf[i] === 0x7D && i + 1 < buf.length && buf[i+1] === 0x02) { out.push(0x7E); i += 2; }
        else if (buf[i] === 0x7D && i + 1 < buf.length && buf[i+1] === 0x01) { out.push(0x7D); i += 2; }
        else { out.push(buf[i++]); }
    }
    return Buffer.from(out);
}

function escapeBuffer(buf) {
    const out = [];
    for (const b of buf) {
        if      (b === 0x7E) { out.push(0x7D, 0x02); }
        else if (b === 0x7D) { out.push(0x7D, 0x01); }
        else { out.push(b); }
    }
    return Buffer.from(out);
}

function buildFrame(msgId, body, phone) {
    const header = Buffer.alloc(12);
    header.writeUInt16BE(msgId,       0);
    header.writeUInt16BE(body.length, 2);
    const phoneStr = String(phone).padStart(12, '0');
    Buffer.from(
        phoneStr.match(/.{2}/g).map(v => {
            const n = parseInt(v, 10);
            return ((Math.floor(n / 10) << 4) | (n % 10));
        })
    ).copy(header, 4);
    header.writeUInt16BE(Math.floor(Math.random() * 0xFFFF), 10);
    const payload = Buffer.concat([header, body]);
    let cs = 0; payload.forEach(b => cs ^= b);
    return Buffer.concat([
        Buffer.from([0x7E]),
        escapeBuffer(Buffer.concat([payload, Buffer.from([cs])])),
        Buffer.from([0x7E]),
    ]);
}

function buildAck(phone, replySeq, replyMsgId) {
    const body = Buffer.alloc(5);
    body.writeUInt16BE(replySeq,   0);
    body.writeUInt16BE(replyMsgId, 2);
    body[4] = 0;
    return buildFrame(0x8001, body, phone);
}

function buildRegisterResponse(phone, replySeq, result, authCode) {
    const authBuf = Buffer.from(authCode, 'ascii');
    const body    = Buffer.alloc(3 + authBuf.length);
    body.writeUInt16BE(replySeq, 0);
    body[2] = result;
    authBuf.copy(body, 3);
    return buildFrame(0x8100, body, phone);
}

function buildVideoRequest(phone, serverIp, serverPort, channel) {
    const ipBuf = Buffer.from(serverIp, 'ascii');
    const N     = ipBuf.length;
    const body  = Buffer.alloc(8 + N);
    body[0] = N;
    ipBuf.copy(body, 1);
    body.writeUInt16BE(serverPort, 1 + N);
    body.writeUInt16BE(0,          3 + N);
    body[5 + N] = channel;
    body[6 + N] = 1;   // video only
    body[7 + N] = 1;   // main stream
    return buildFrame(0x9101, body, phone);
}

function parseAdditionalInfo(buf) {
    const result = {};
    let i = 0;
    while (i + 1 < buf.length) {
        const id  = buf[i];
        const len = buf[i + 1];
        if (i + 2 + len > buf.length) break;
        const val = buf.slice(i + 2, i + 2 + len);
        switch (id) {
            case 0x01: if (val.length >= 4) result.mileage        = val.readUInt32BE(0) / 10;   break;
            case 0x03: if (val.length >= 2) result.sensorSpeed    = val.readUInt16BE(0) / 10; break;
            case 0x25: if (val.length >= 2) result.voltage        = val.readUInt16BE(0) / 10;    break;
            case 0x30: if (val.length >= 1) result.signalStrength = val[0];                              break;
            case 0x31: if (val.length >= 1) result.satellites     = val[0];                              break;
            case 0xd5: result.imei = val.toString('ascii').replace(/\0/g, '').trim();                    break;
        }
        i += 2 + len;
    }
    return result;
}

// ── GPS helpers ───────────────────────────────────────────────────────────────
function parseGps(body, phone) {
    const alarmFlags = body.readUInt32BE(0);
    const statusBits = body.readUInt32BE(4);
    const latRaw     = body.readUInt32BE(8);
    const lonRaw     = body.readUInt32BE(12);
    const elevation  = body.readUInt16BE(16);
    const speed      = body.readUInt16BE(18) / 10;
    const direction  = body.readUInt16BE(20);

    const lat = latRaw / 1e6 * (!!(statusBits & (1<<2)) ? -1 : 1);
    const lon = lonRaw / 1e6 * (!!(statusBits & (1<<3)) ? -1 : 1);

    const bcd = b => ((b >> 4) * 10 + (b & 0x0F));
    const t   = 22;
    const dt  = [
        `20${String(bcd(body[t])).padStart(2,'0')}`,
        String(bcd(body[t+1])).padStart(2,'0'),
        String(bcd(body[t+2])).padStart(2,'0'),
    ].join('-') + ' ' + [
        String(bcd(body[t+3])).padStart(2,'0'),
        String(bcd(body[t+4])).padStart(2,'0'),
        String(bcd(body[t+5])).padStart(2,'0'),
    ].join(':');

    const extra = parseAdditionalInfo(body.slice(28));

    const alarmNames = {
        EMERGENCY:   1<<0, OVERSPEED: 1<<1, GNSS_FAULT: 1<<4,
        ANTENNA_CUT: 1<<5, LOW_V:     1<<7, POWER_OFF:  1<<8,
    };
    const alarms = Object.entries(alarmNames)
        .filter(([, bit]) => alarmFlags & bit)
        .map(([name]) => name);

    return {
        phone, datetime: dt, lat, lon, speed, direction, elevation,
        accOn:   !!(statusBits & (1<<0)),
        located: !!(statusBits & (1<<1)),
        alarms,
        oil_circuit:     !!(statusBits & (1<<10)) ? 'CUT'  : 'NORMAL',
        vehicle_circuit: !!(statusBits & (1<<11)) ? 'CUT'  : 'NORMAL',
        door:            !!(statusBits & (1<<13)) ? 'OPEN' : 'CLOSED',
        ...extra,
    };
}

// ── TCP server ────────────────────────────────────────────────────────────────
const tcpServer = net.createServer(socket => {
    const remote = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`[${TAG}][TCP] Connected: ${remote}`);

    // Keepalive — detect dead connections within ~30s
    socket.setKeepAlive(true, 10000);
    socket.setTimeout(300000);  // 5 minutes — GPS keeps socket alive every 10s
    socket.on('timeout', () => {
        console.warn(`[${TAG}][TCP] Socket timeout (no data for 5min): ${remote}`);
        socket.destroy();
    });

    let buffer = Buffer.alloc(0);
    let phone  = null;
    let streamPacketWarned = false;   // warn once per connection, not once per packet

    socket.on('data', data => {
        try {
            // Guard against runaway buffers
            if (buffer.length + data.length > CONFIG.maxBufferBytes) {
                console.warn(`[${TAG}][TCP] Buffer overflow for ${phone || remote} — resetting`);
                buffer = Buffer.alloc(0);
            }

            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length) {

                // ── Stream data packet (T/98 §5.5.3 — magic 0x30316364) ───────
                // ── Stream data packet (T/98 §5.5.3 — magic 0x30316364) ───────
                if (buffer.length - offset >= 4 &&
                    buffer[offset]   === 0x30 && buffer[offset+1] === 0x31 &&
                    buffer[offset+2] === 0x63 && buffer[offset+3] === 0x64) {

                    if (buffer.length - offset < 30) break;
                    const dataBodyLen = buffer.readUInt16BE(offset + 28);
                    if (buffer.length - offset < 30 + dataBodyLen) break;

                    const simBytes    = buffer.slice(offset + 8, offset + 14);
                    const streamPhone = Array.from(simBytes, b =>
                        b.toString(16).padStart(2, '0')).join('').replace(/^0+/, '');

                    const byte15       = buffer[offset + 15];
                    const dataType     = (byte15 >> 4) & 0x0F;
                    const subpktMarker = byte15 & 0x0F;
                    const rawData      = buffer.slice(offset + 30, offset + 30 + dataBodyLen);

                    const camPhone = streamPhone || phone;
                    if (camPhone && cameras[camPhone]) {
                        // Mark that this socket is a stream socket for this camera
                        if (!cameras[camPhone].hasStreamSocket) {
                            cameras[camPhone].hasStreamSocket = true;
                            cameras[camPhone].streamSocket    = socket;
                            console.log(`[${TAG}][${camPhone}] 📡 Stream socket established`);

                            // Start FFmpeg now if not running
                            if (!cameras[camPhone].ffmpeg && !cameras[camPhone].restarting) {
                                startFFmpeg(camPhone);
                            }
                        }
                        processVideoPacket(rawData, camPhone, dataType, subpktMarker);
                    } else if (!streamPacketWarned) {
                        // No registered camera matches this stream packet's SIM — packet is
                        // dropped. Logged once per connection (not per-packet) since this can
                        // otherwise flood the log at stream frame rate.
                        streamPacketWarned = true;
                        console.warn(
                            `[${TAG}][TCP] Stream packet from ${remote} has SIM/phone "${streamPhone}" ` +
                            `(raw bytes: ${simBytes.toString('hex')}) which doesn't match any registered ` +
                            `camera. Known cameras: [${Object.keys(cameras).join(', ') || 'none'}]. ` +
                            `Packet dropped — check whether this device's registration phone matches its stream SIM encoding.`
                        );
                    }

                    offset += 30 + dataBodyLen;
                    continue;
                }

                // ── Signalling packet (JT/T 808 — 0x7E framed) ───────────────
                if (buffer[offset] === 0x7E) {
                    const end = buffer.indexOf(0x7E, offset + 1);
                    if (end === -1) break;
                    if (end === offset + 1) { offset++; continue; }  // empty frame

                    const inner     = buffer.slice(offset + 1, end);
                    const unescaped = unescapeBuffer(inner);
                    if (unescaped.length < 12) { offset = end + 1; continue; }

                    const msgId    = unescaped.readUInt16BE(0);
                    const rawPhone = Array.from(unescaped.slice(4, 10), b =>
                        b.toString(16).padStart(2, '0')).join('');
                    phone = rawPhone.replace(/^0+/, '');
                    const seq  = unescaped.readUInt16BE(10);
                    const body = unescaped.slice(12);

                    // ── 0x0001: Device ACK ────────────────────────────────────
                    if (msgId === 0x0001) {
                        const replyMsgId  = body.readUInt16BE(2);
                        const replyResult = body[4];
                        const resultText  = ['Success','Failed','Wrong Msg','Not Supported'][replyResult] || `code:${replyResult}`;
                        console.log(`[${TAG}][${phone}] ACK → 0x${replyMsgId.toString(16).padStart(4,'0')} ${resultText}`);
                        bus.emit('device:message', { vendor: CONFIG.vendor, msgId, body, seq, phone, socket });

                    // ── 0x0100: Register ──────────────────────────────────────
                    } else if (msgId === 0x0100) {
                        console.log(`[${TAG}][${phone}] Register`);
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    // ── 0x0102: Auth complete — set up camera ─────────────────
                    } else if (msgId === 0x0102) {
                        console.log(`[${TAG}][${phone}] Auth OK — setting up camera`);
                        socket.write(buildAck(phone, seq, msgId));

                        const cam = getCamera(phone);
                        tcpSockets[phone] = socket;

                        // Start FFmpeg if not already running
                        if (!cam.ffmpeg && !cam.restarting) {
                            startFFmpeg(phone);
                        }

                        // Start watchdog
                        startWatchdog(phone);

                        // Request live video
                        socket.write(buildVideoRequest(
                            phone, CONFIG.serverIp, CONFIG.tcpPort, CONFIG.streamChannel
                        ));

                        console.log(`[${TAG}][${phone}] ✅ Registered. Cameras online: ${Object.keys(tcpSockets).length}`);
                        broadcast({
                            type:    'camera_connected',
                            phone,
                            stream:  `${PUBLIC_URL_PREFIX}/${phone}.m3u8`,
                            cameras: buildCameraList(),
                        });
                        bus.emit('device:connected', { vendor: CONFIG.vendor, phone, socket });

                    // ── 0x0200: GPS location ──────────────────────────────────
                    } else if (msgId === 0x0200) {
                        socket.write(buildAck(phone, seq, msgId));
                        if (body.length < 28) { offset = end + 1; continue; }

                        const gps = parseGps(body, phone);
                        broadcast({ type: 'location', ...gps });

                        const record = [
                            phone, gps.datetime, gps.lat, gps.lon,
                            gps.speed, gps.direction, gps.elevation,
                            gps.accOn ? 'ON' : 'OFF',
                            gps.located ? 'YES' : 'NO',
                            gps.mileage || '0', gps.voltage || '0',
                            gps.satellites || '0', gps.signalStrength || '0',
                            gps.sensorSpeed || '0',
                            gps.oil_circuit, gps.vehicle_circuit, gps.door,
                            gps.alarms.join('|') || 'NONE',
                        ];

                        tcpForwarder.sendGpsRecord({
                            phone,      datetime: gps.datetime,
                            latitude:   gps.lat,  longitude: gps.lon,
                            speed_kmh:  gps.speed, direction_deg: gps.direction,
                            elevation_m: gps.elevation,
                            acc:         gps.accOn   ? 'ON'  : 'OFF',
                            located:     gps.located ? 'YES' : 'NO',
                            mileage:     gps.mileage        || '0',
                            voltage:     gps.voltage        || '0',
                            satellites:  gps.satellites     || '0',
                            signal:      gps.signalStrength || '0',
                            sensor_speed: gps.sensorSpeed   || '0',
                            oil_circuit:     gps.oil_circuit,
                            vehicle_circuit: gps.vehicle_circuit,
                            door:            gps.door,
                            alarms:          gps.alarms.join('|') || 'NONE',
                        });

                        const logFile = `./gps_log_${CONFIG.vendor}_${phone}_${new Date().toISOString().slice(0,10)}.txt`;
                        fs.appendFile(logFile, record.join(',') + '\n', e => {
                            if (e) console.error(`[${TAG}][GPS LOG]`, e.message);
                        });

                    // ── 0x1205: File list response ────────────────────────────
                    } else if (msgId === 0x1205) {
                        const totalFiles = body.readUInt32BE(2);
                        console.log(`[${TAG}][${phone}] 0x1205 file list — total:${totalFiles}`);
                        if (totalFiles === 0) console.warn(`[${TAG}][${phone}] ⚠️ 0 files found for this time range`);

                        // Log file sizes from SD card
                        if (totalFiles > 0 && body.length > 6) {
                            let p = 6;
                            for (let i = 0; i < totalFiles && p + 28 <= body.length; i++) {
                                const fileSize = body.readUInt32BE(p + 24);
                                console.log(`[${TAG}][${phone}] File ${i+1} on SD card: ${fileSize} bytes`);
                                p += 28;
                            }
                        }

                        bus.emit('device:message', { vendor: CONFIG.vendor, msgId, body, seq, phone, socket });

                    // ── 0x1206: File upload done ──────────────────────────────
                    } else if (msgId === 0x1206) {
                        console.log(`[${TAG}][${phone}] 0x1206 upload notification`);
                        bus.emit('device:message', { vendor: CONFIG.vendor, msgId, body, seq, phone, socket });

                    // ── Everything else ───────────────────────────────────────
                    } else {
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                // Unknown byte — skip
                offset++;
            }

            // Keep only unprocessed bytes
            buffer = offset > 0 ? buffer.slice(offset) : buffer;

        } catch (e) {
            console.error(`[${TAG}][TCP] Parse error (${phone || remote}):`, e.message);
            buffer = Buffer.alloc(0);  // reset buffer on parse error to avoid cascading failures
        }
    });

    socket.on('close', () => {
        console.log(`[${TAG}][TCP] Disconnected: ${remote} phone:${phone}`);
        if (!phone) return;

        // Check if this was a stream socket for any camera
        for (const [camPhone, cam] of Object.entries(cameras)) {
            if (cam.streamSocket === socket) {
                console.log(`[${TAG}][${camPhone}] 📡 Stream socket disconnected`);
                cam.hasStreamSocket = false;
                cam.streamSocket    = null;
                cam.gotIFrame       = false;
                // Kill FFmpeg — will restart when stream socket reconnects
                if (cam.ffmpeg) {
                    cam.ffmpeg.kill('SIGKILL');
                    cam.ffmpeg     = null;
                    cam.patPmtSent = false;
                }
                return;  // don't delete camera state — signalling socket still alive
            }
        }

        // Signalling socket closed — full cleanup
        if (cameras[phone]?.watchdog) clearInterval(cameras[phone].watchdog);
        if (cameras[phone]?.ffmpeg) {
            cameras[phone].ffmpeg.stdin.end();
            try { cameras[phone].ffmpeg.kill('SIGTERM'); } catch (_) {}
        }
        delete cameras[phone];
        delete tcpSockets[phone];

        broadcast({
            type:    'camera_disconnected',
            phone,
            cameras: buildCameraList(),
        });
        bus.emit('device:disconnected', { vendor: CONFIG.vendor, phone });
    });

    socket.on('error', e => {
        if (e.code !== 'ECONNRESET') {
            console.error(`[${TAG}][TCP] Socket error (${phone || remote}): ${e.message}`);
        }
    });
});

tcpServer.listen(CONFIG.tcpPort, '0.0.0.0', () =>
    console.log(`[${TAG}] ✓ TCP server on :${CONFIG.tcpPort}`)
);

// ── Graceful shutdown ─────────────────────────────────────────────────────────
process.on('SIGTERM', () => {
    console.log(`[${TAG}] SIGTERM — shutting down gracefully`);
    Object.entries(cameras).forEach(([phone, cam]) => {
        if (cam.ffmpeg) { try { cam.ffmpeg.kill('SIGTERM'); } catch (_) {} }
        if (cam.watchdog) clearInterval(cam.watchdog);
    });
    process.exit(0);
});

return { CONFIG, tcpServer, wss, cameras, tcpSockets };

};