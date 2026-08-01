'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// index-pictor.js  —  LIVE STREAM + GPS  (stable multi-camera)
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
//   http://<server>:8080/public/<phone>.m3u8
//
// API:
//   GET  http://<server>:8080/api/cameras
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

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
    tcpPort:        parseInt(process.env.TCP_PORT      || '3007'),
    httpPort:       parseInt(process.env.HTTP_PORT     || '8080'),
    wsPort:         parseInt(process.env.WS_PORT       || '8801'),
    serverIp:       process.env.SERVER_IP,
    ffmpegPath:     process.env.FFMPEG_PATH            || '/usr/local/bin/ffmpeg',
    streamChannel:  parseInt(process.env.STREAM_CH     || '1'),
    maxBufferBytes: parseInt(process.env.MAX_BUF       || String(4 * 1024 * 1024)),  // 4MB per socket
    watchdogMs:     parseInt(process.env.WATCHDOG_MS   || '15000'),  // restart FFmpeg if no frames for 15s

    // Set STREAM_REQUEST_ENABLED=false in .env to stop asking cameras to start
    // streaming (0x9101 is simply never sent on auth).
    streamRequestEnabled: (process.env.STREAM_REQUEST_ENABLED || 'true').toLowerCase() !== 'false',

    // How long a camera can go with no video frames before we fire a TCP
    // alert to the remote server (via tcp-forwarder.js). Defaults to 30s.
    streamStallAlertMs: parseInt(process.env.STREAM_STALL_ALERT_MS || '30000'),

    // How long a device can go with no GPS (0x0200) reports before that
    // also counts as an interruption. Defaults to 30s.
    gpsStallAlertMs: parseInt(process.env.GPS_STALL_ALERT_MS || '30000'),

    // Which condition(s) should trigger the STREAM_INTRUPPED alert:
    //   'gps'    → only when GPS (0x0200) reports stop coming from the device (default)
    //   'stream' → only when the video stream stalls (no FFmpeg frames)
    //   'both'   → either one stalling fires the alert
    alertOn: (process.env.ALERT_ON || 'gps').toLowerCase(),
};

console.log(`[Pictor] Server IP  : ${CONFIG.serverIp}`);
console.log(`[Pictor] TCP        : ${CONFIG.tcpPort}`);
console.log(`[Pictor] HTTP       : ${CONFIG.httpPort}`);
console.log(`[Pictor] WS         : ${CONFIG.wsPort}`);

// ── Ensure public folder exists ───────────────────────────────────────────────
if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── Per-camera state ──────────────────────────────────────────────────────────
const cameras    = {};   // { [phone]: CameraState }
const tcpSockets = {};   // { [phone]: net.Socket }
const lastGps    = {};   // { [phone]: { ...gpsRecordFields, receivedAt } }

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
        stallAlertSent:  false,   // ← true once we've alerted for the current stall
    };
}

function getCamera(phone) {
    if (!cameras[phone]) cameras[phone] = makeCamera();
    return cameras[phone];
}

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`[Pictor] ✓ WebSocket on :${CONFIG.wsPort}`);

wss.on('connection', (ws, req) => {
    console.log(`[WS] Browser connected from ${req.socket.remoteAddress}`);
    // Send current camera list immediately
    ws.send(JSON.stringify({
        type:    'cameras',
        cameras: buildCameraList(),
    }));
    ws.on('close', () => console.log('[WS] Browser disconnected'));
    ws.on('error', e  => console.error('[WS] Error:', e.message));
});

function broadcast(obj) {
    const raw = JSON.stringify(obj);
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(raw); });
}

function buildCameraList() {
    return Object.keys(tcpSockets).map(phone => ({
        phone,
        stream: `/public/${phone}.m3u8`,
    }));
}

// ── Bus: ftp-service sends frames back through the bus ────────────────────────
bus.on('device:send', ({ phone, frame }) => {
    const socket = tcpSockets[String(phone)];
    if (!socket || socket.destroyed) {
        console.warn(`[BUS] device:send — no socket for phone:${phone}`);
        return;
    }
    socket.write(frame);
    console.log(`[BUS] wrote ${frame.length} bytes to ${phone}`);
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
    const filePath = urlPath === '/' ? './video.html' : `.${urlPath}`;
    const ext      = path.extname(filePath).toLowerCase();

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
    console.log(`[Pictor] ✓ HTTP on :${CONFIG.httpPort}`)
);

// ── FFmpeg — one stable instance per camera ───────────────────────────────────
function startFFmpeg(phone) {
    const cam = cameras[phone];
    if (!cam) return null;
    if (cam.restarting) return null;

    console.log(`[FFmpeg ${phone}] Starting...`);

    // Clean up old segment files so HLS player doesn't get confused
    try {
        fs.readdirSync('./public')
            .filter(f => f.startsWith(`${phone}_`) && f.endsWith('.ts'))
            .forEach(f => { try { fs.unlinkSync(`./public/${f}`); } catch (_) {} });
        const m3u8 = `./public/${phone}.m3u8`;
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
        '-hls_segment_filename', `./public/${phone}_%04d.ts`,
        `./public/${phone}.m3u8`,
    ]);

    // Log only errors from FFmpeg stderr
    let stderrBuf = '';
    ffmpeg.stderr.on('data', d => {
        stderrBuf += d.toString();
        const lines = stderrBuf.split('\n');
        stderrBuf = lines.pop();
        lines.forEach(line => {
            const l = line.trim();
            if (!l) return;
            if (l.includes('frame=') || l.includes('fps=')) return; // skip progress lines
            if (l.includes('error') || l.includes('Error') || l.includes('Invalid') ||
                l.includes('muxing overhead') || l.includes('No such file')) {
                //console.error(`[FFmpeg ${phone}] ${l}`);
            }
        });
    });

    ffmpeg.stdin.on('error', e => {
        // Ignore EPIPE — happens when FFmpeg restarts while we're writing
        if (e.code !== 'EPIPE') console.error(`[FFmpeg ${phone}] stdin error: ${e.message}`);
    });

    ffmpeg.on('close', code => {
        console.log(`[FFmpeg ${phone}] exited (code ${code})`);
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

        // ── Stall alert — fire once per stall to the remote TCP server ─────
        const streamDown = age > CONFIG.streamStallAlertMs;
        const gpsDown     = lastGps[phone]
            ? (Date.now() - lastGps[phone].receivedAt) > CONFIG.gpsStallAlertMs
            : false;

        let alertCondition = false;
        if (CONFIG.alertOn === 'stream')      alertCondition = streamDown;
        else if (CONFIG.alertOn === 'gps')    alertCondition = gpsDown;
        else /* 'both' */                     alertCondition = streamDown || gpsDown;

        if (alertCondition) {
            if (!cameras[phone].stallAlertSent) {
                cameras[phone].stallAlertSent = true;
                tcpForwarder.sendStreamAlert(phone, lastGps[phone]);
            }
        } else {
            cameras[phone].stallAlertSent = false;
        }

        if (age > CONFIG.watchdogMs && cameras[phone].ffmpeg && !cameras[phone].restarting) {

            // ── NEW: if no stream socket exists, stop FFmpeg and wait ──────────
            // Camera stream connection dropped — stop restarting FFmpeg endlessly
            if (!cameras[phone].hasStreamSocket) {
                console.warn(`[Watchdog ${phone}] No stream socket — stopping FFmpeg until camera reconnects`);
                if (cameras[phone].ffmpeg) {
                    cameras[phone].ffmpeg.kill('SIGKILL');
                    cameras[phone].ffmpeg    = null;
                    cameras[phone].patPmtSent = false;
                    cameras[phone].gotIFrame  = false;
                }
                return;
            }

            console.warn(`[Watchdog ${phone}] No frames for ${age}ms — restarting FFmpeg`);
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
    if (!isVideo) return;

    // Wait for first I-frame before feeding P/B frames
    if (dataType === 0) {
        cam.gotIFrame = true;
    } else if (!cam.gotIFrame) {
        return;
    }

    if (!cam.ffmpeg || !cam.ffmpeg.stdin.writable) return;

    // Send PAT+PMT once per FFmpeg instance
    if (!cam.patPmtSent) {
        writeToFFmpeg(cam, phone, buildPAT());
        writeToFFmpeg(cam, phone, buildPMT());
        cam.patPmtSent = true;
        console.log(`[${phone}] 📺 PAT+PMT sent, streaming started`);
        broadcast({ type: 'stream_started', phone, stream: `/public/${phone}.m3u8` });
    }

    const { packets, nextCounter } = wrapFrameInTS(frameData, cam.tsCounter, Date.now());
    cam.tsCounter  = nextCounter;
    cam.lastFrameAt = Date.now();
    cam.frameCount++;

    for (const pkt of packets) writeToFFmpeg(cam, phone, pkt);

    // Log frame stats every 300 frames
    if (cam.frameCount % 300 === 0) {
        console.log(`[${phone}] 📊 ${cam.frameCount} frames processed`);
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
    console.log(`[TCP] Connected: ${remote}`);

    // Keepalive — detect dead connections within ~30s
    socket.setKeepAlive(true, 10000);
    socket.setTimeout(300000);  // 5 minutes — GPS keeps socket alive every 10s
    socket.on('timeout', () => {
        console.warn(`[TCP] Socket timeout (no data for 5min): ${remote}`);
        socket.destroy();
    });

    let buffer = Buffer.alloc(0);
    let phone  = null;

    socket.on('data', data => {
        try {
            // Guard against runaway buffers
            if (buffer.length + data.length > CONFIG.maxBufferBytes) {
                console.warn(`[TCP] Buffer overflow for ${phone || remote} — resetting`);
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
                            console.log(`[${camPhone}] 📡 Stream socket established`);

                            // Start FFmpeg now if not running
                            if (!cameras[camPhone].ffmpeg && !cameras[camPhone].restarting) {
                                startFFmpeg(camPhone);
                            }
                        }
                        processVideoPacket(rawData, camPhone, dataType, subpktMarker);
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
                        console.log(`[${phone}] ACK → 0x${replyMsgId.toString(16).padStart(4,'0')} ${resultText}`);
                        bus.emit('device:message', { msgId, body, seq, phone, socket });

                    // ── 0x0100: Register ──────────────────────────────────────
                    } else if (msgId === 0x0100) {
                        console.log(`[${phone}] Register`);
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    // ── 0x0102: Auth complete — set up camera ─────────────────
                    } else if (msgId === 0x0102) {
                        console.log(`[${phone}] Auth OK — setting up camera`);
                        socket.write(buildAck(phone, seq, msgId));

                        const cam = getCamera(phone);
                        tcpSockets[phone] = socket;

                        // Start FFmpeg if not already running
                        if (!cam.ffmpeg && !cam.restarting) {
                            startFFmpeg(phone);
                        }

                        // Start watchdog
                        startWatchdog(phone);

                        // Request live video (can be disabled via .env: STREAM_REQUEST_ENABLED=false)
                        if (CONFIG.streamRequestEnabled) {
                            socket.write(buildVideoRequest(
                                phone, CONFIG.serverIp, CONFIG.tcpPort, CONFIG.streamChannel
                            ));
                        } else {
                            console.log(`[${phone}] Streaming disabled via STREAM_REQUEST_ENABLED=false — skipping 0x9101`);
                        }

                        console.log(`[${phone}] ✅ Registered. Cameras online: ${Object.keys(tcpSockets).length}`);
                        broadcast({
                            type:    'camera_connected',
                            phone,
                            stream:  `/public/${phone}.m3u8`,
                            cameras: buildCameraList(),
                        });
                        bus.emit('device:connected', { phone, socket });

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

                        const gpsSnapshot = {
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
                        };

                        tcpForwarder.sendGpsRecord({
                            phone,      datetime: gps.datetime,
                            ...gpsSnapshot,
                            alarms:          gps.alarms.join('|') || 'NONE',
                        });

                        // Cache for STREAM_INTRUPPED alerts + mark GPS as fresh
                        lastGps[phone] = { ...gpsSnapshot, receivedAt: Date.now() };

                        const logFile = `./gps_log_${phone}_${new Date().toISOString().slice(0,10)}.txt`;
                        fs.appendFile(logFile, record.join(',') + '\n', e => {
                            if (e) console.error('[GPS LOG]', e.message);
                        });

                    // ── 0x1205: File list response ────────────────────────────
                    } else if (msgId === 0x1205) {
                        const totalFiles = body.readUInt32BE(2);
                        console.log(`[${phone}] 0x1205 file list — total:${totalFiles}`);
                        if (totalFiles === 0) console.warn(`[${phone}] ⚠️ 0 files found for this time range`);

                        // Log file sizes from SD card
                        if (totalFiles > 0 && body.length > 6) {
                            let p = 6;
                            for (let i = 0; i < totalFiles && p + 28 <= body.length; i++) {
                                const fileSize = body.readUInt32BE(p + 24);
                                console.log(`[${phone}] File ${i+1} on SD card: ${fileSize} bytes`);
                                p += 28;
                            }
                        }

                        bus.emit('device:message', { msgId, body, seq, phone, socket });

                    // ── 0x1206: File upload done ──────────────────────────────
                    } else if (msgId === 0x1206) {
                        console.log(`[${phone}] 0x1206 upload notification`);
                        bus.emit('device:message', { msgId, body, seq, phone, socket });

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
            console.error(`[TCP] Parse error (${phone || remote}):`, e.message);
            buffer = Buffer.alloc(0);  // reset buffer on parse error to avoid cascading failures
        }
    });

    socket.on('close', () => {
        console.log(`[TCP] Disconnected: ${remote} phone:${phone}`);
        if (!phone) return;

        // Check if this was a stream socket for any camera
        for (const [camPhone, cam] of Object.entries(cameras)) {
            if (cam.streamSocket === socket) {
                console.log(`[${camPhone}] 📡 Stream socket disconnected`);
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
        bus.emit('device:disconnected', { phone });
    });

    socket.on('error', e => {
        if (e.code !== 'ECONNRESET') {
            console.error(`[TCP] Socket error (${phone || remote}): ${e.message}`);
        }
    });
});

tcpServer.listen(CONFIG.tcpPort, '0.0.0.0', () =>
    console.log(`[Pictor] ✓ TCP server on :${CONFIG.tcpPort}`)
);

// ── Graceful shutdown ─────────────────────────────────────────────────────────
process.on('SIGTERM', () => {
    console.log('[Pictor] SIGTERM — shutting down gracefully');
    Object.entries(cameras).forEach(([phone, cam]) => {
        if (cam.ffmpeg) { try { cam.ffmpeg.kill('SIGTERM'); } catch (_) {} }
        if (cam.watchdog) clearInterval(cam.watchdog);
    });
    process.exit(0);
});