'use strict';

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
require('dotenv').config();
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');
const tcpForwarder = require('./tcp-forwarder');

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
    tcpPort:  3007,
    httpPort: 8080,
    wsPort:   8801,
    serverIp: process.env.SERVER_IP
};
console.log(`Server IP: ${CONFIG.serverIp}`);

// ── Create output folder ──────────────────────────────────────────────────────
if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── Device state & FTP ───────────────────────────────────────────────────────
const tcpSockets      = {}; // { [phone]: socket }
const deviceRecordings= {}; // { [phone]: [{ch,startTime,endTime,size}] }
const activeDownloads = {}; // { [phone]: { channel, active } }
let recActive    = false;  // true while recording stream is expected
let recEndTimer  = null;   // timer to stop rec capture
// Built-in FTP server so device can upload recordings to us
// npm install ftp-srv  ←  run this once
// ── Minimal FTP Server (PASV only, no EPSV — required for Babelstar device) ──
if (!fs.existsSync('./recordings')) fs.mkdirSync('./recordings');

// const PUBLIC_IP = '20.244.41.46';
// const FTP_PORT  = 2121;
// const PASV_MIN  = 3500;
// const PASV_MAX  = 3600;
// let   pasvPort  = PASV_MIN;

// function nextPasvPort() {
//     const p = pasvPort++;
//     if (pasvPort > PASV_MAX) pasvPort = PASV_MIN;
//     return p;
// }
const PUBLIC_IP      = process.env.PUBLIC_IP || process.env.SERVER_IP;
const FTP_PORT       = 2121;
const PASV_DATA_PORT = 2122;
 
// ── Permanent PASV data server — started ONCE on boot, never closed ──────────
// The old code created/destroyed it per PASV command which caused EADDRINUSE
// on the second upload attempt, making port 2122 silently unavailable.
let pasvDataSocket = null;   // the one pending data connection slot
 
const pasvServer = net.createServer(s => {
    pasvDataSocket = s;
});
pasvServer.listen(PASV_DATA_PORT, '0.0.0.0', () => {
    console.log(`✓ FTP PASV data server on :${PASV_DATA_PORT}`);
});
pasvServer.on('error', err => console.error('[FTP] PASV server error:', err.message));
 
net.createServer(ftpSocket => {
    let dataSocket   = null;
    let uploadPath   = null;
    let uploadStream = null;
 
    const reply = (code, msg) => {
        ftpSocket.write(`${code} ${msg}\r\n`);
    };
 
    reply(220, 'FTP Server Ready');
 
    ftpSocket.on('data', data => {
        const lines = data.toString().split('\r\n').filter(Boolean);
        lines.forEach(line => {
            const [cmd, ...args] = line.trim().split(' ');
            const arg = args.join(' ');
 
            switch (cmd.toUpperCase()) {
                case 'USER':
                    reply(230, 'User logged in, proceed');
                    break;
 
                case 'PASS':
                    reply(230, 'User logged in, proceed');
                    break;
 
                case 'SYST':
                    reply(215, 'UNIX Type: L8');
                    break;
 
                case 'TYPE':
                    reply(200, 'Type set to I');
                    break;
 
                case 'PWD':
                case 'XPWD':
                    reply(257, '"/" is current directory');
                    break;
 
                case 'CWD':
                    reply(250, 'Directory changed');
                    break;
 
                case 'MKD':
                    reply(257, `"/${arg}" created`);
                    break;
 
                case 'EPSV':
                    reply(502, 'EPSV not supported, use PASV');
                    break;
 
                case 'PASV': {
                    // Reset the slot — next connection to pasvServer fills it
                    pasvDataSocket = null;
                    dataSocket     = null;
 
                    const ip = PUBLIC_IP.split('.');
                    const p1 = Math.floor(PASV_DATA_PORT / 256);
                    const p2 = PASV_DATA_PORT % 256;
                    reply(227, `Entering Passive Mode (${ip.join(',')},${p1},${p2})`);
                    break;
                }
 
                case 'LIST':
                case 'NLST':
                    reply(150, 'Here comes the directory listing');
                    if (pasvDataSocket) { pasvDataSocket.end(''); pasvDataSocket = null; }
                    reply(226, 'Directory send OK');
                    break;
 
                case 'STOR': {
                    const filename = path.basename(arg || `recording_${Date.now()}.mp4`);
                    uploadPath     = path.join('./recordings', filename);
                    uploadStream   = fs.createWriteStream(uploadPath);
                    reply(150, 'Ok to send data');
 
                    const notifyBrowser = () => {
                        const fname = path.basename(uploadPath);
                        reply(226, 'Transfer complete');
                        wss.clients.forEach(c => {
                            if (c.readyState === 1) c.send(JSON.stringify({
                                type:     'recording_ready',
                                url:      `/recordings/${fname}`,
                                filename: fname,
                            }));
                        });
                        pasvDataSocket = null;
                        dataSocket     = null;
                    };
 
                    // Wait up to 10s for the device to open the data connection
                    let tries = 0;
                    const waitForData = setInterval(() => {
                        // Grab whatever landed in the shared slot
                        if (!dataSocket && pasvDataSocket) {
                            dataSocket = pasvDataSocket;
                        }
                        if (!dataSocket) {
                            if (++tries > 100) {
                                clearInterval(waitForData);
                                reply(425, 'No data connection established');
                            }
                            return;
                        }
                        clearInterval(waitForData);
                        console.log(`[FTP] Data connection acquired, piping to ${uploadPath}`);
 
                        dataSocket.pipe(uploadStream);
                        uploadStream.on('finish', notifyBrowser);
                        dataSocket.on('end',   () => uploadStream.end());
                        dataSocket.on('close', () => uploadStream.end());
                        dataSocket.on('error', err => {
                            console.error('[FTP] Data socket error:', err.message);
                            reply(426, 'Connection closed, transfer aborted');
                        });
                    }, 100);
                    break;
                }
 
                case 'QUIT':
                    reply(221, 'Goodbye');
                    ftpSocket.end();
                    break;
 
                case 'NOOP':
                    reply(200, 'OK');
                    break;
 
                case 'FEAT':
                    ftpSocket.write('211-Features:\r\n211 End\r\n');
                    break;
 
                default:
                    reply(202, 'Command not implemented');
            }
        });
    });
 
    ftpSocket.on('close', () => {
        if (uploadStream) { try { uploadStream.end(); } catch (_) {} }
    });
 
    ftpSocket.on('error', err => console.error('[FTP] Socket error:', err.message));
 
}).listen(FTP_PORT, () => console.log(`✓ FTP control server on :${FTP_PORT}`));


// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);
wss.on('connection', (ws, req) => {
    console.log(`[WS] Browser connected from ${req.socket.remoteAddress}`);

    ws.on('message', raw => {
        let msg;
        try { msg = JSON.parse(raw); } catch(e) {
            console.warn('[WS] Non-JSON message received:', raw.toString());
            return;
        }
        // ── query_recordings: return cached list filtered by date ─────────
        if (msg.type === 'query_recordings') {
            const { startDate, endDate } = msg;
            let all = Object.values(deviceRecordings).flat();
            const startPrefix = startDate ? startDate.split(' ')[0] : null;
            const endPrefix   = endDate   ? endDate.split(' ')[0]   : null;
            if (startPrefix) all = all.filter(r => r.startTime.split(' ')[0] >= startPrefix);
            if (endPrefix)   all = all.filter(r => r.startTime.split(' ')[0] <= endPrefix);
            all.sort((a, b) => b.startTime.localeCompare(a.startTime));
            ws.send(JSON.stringify({ type: 'recordings', data: all }));

            // If empty, re-ask device
            if (all.length === 0) {
                Object.entries(tcpSockets).forEach(([ph, sock]) => {
                    if (sock && !sock.destroyed) {
                        sock.write(buildQueryRecordings(ph, startDate, endDate));
                    }
                });
            }
        }

        // ── download_recording: stream recording via 0x9201 ───────────────
        if (msg.type === 'download_recording') {
            const { ch, startTime, endTime, phone: reqPhone } = msg;
            const targetPhone = reqPhone || Object.keys(tcpSockets)[0];
            console.log(`[Rec] ▶ download ch:${ch} ${startTime}→${endTime} phone:${targetPhone}`);
            console.log(`[Rec] tcpSockets:`, Object.keys(tcpSockets));
            console.log(`[Rec] recActive before:`, recActive);
 
            if (!targetPhone || !tcpSockets[targetPhone] || tcpSockets[targetPhone].destroyed) {
                ws.send(JSON.stringify({ type: 'error', message: 'Device not connected' }));
                return;
            }
 
            // Stop any previous rec and clean old files
            recActive = false;
            if (recEndTimer) { clearTimeout(recEndTimer); recEndTimer = null; }
            const recPhone = Object.keys(activeDownloads)[0];
            if (recPhone && recChannels[recPhone]) {
                try { recChannels[recPhone].ffmpeg.stdin.end(); } catch(_) {}
                delete recChannels[recPhone];
            }
            Object.keys(activeDownloads).forEach(p => delete activeDownloads[p]);
 
            try {
                fs.readdirSync('./public')
                  .filter(f => f.startsWith(`rec_${targetPhone}`))
                  .forEach(f => { try { fs.unlinkSync(`./public/${f}`); } catch(_) {} });
                console.log(`[Rec] Cleaned old rec files for ${targetPhone}`);
            } catch(_) {}
 
            // Start fresh rec writer
            const recFfmpeg = startRecFFmpeg(targetPhone, ch);
            recChannels[targetPhone] = {
                ffmpeg: recFfmpeg, gotIFrame: false,
                subpackets: [], tsCounter: 0, patPmtSent: false,
            };
            console.log(`[Rec] recChannels keys after set:`, Object.keys(recChannels));
            console.log(`[Rec] activeDownloads keys after set:`, Object.keys(activeDownloads));
            activeDownloads[targetPhone] = { channel: ch, active: true };
 
            // Inactivity timer — stop rec if no packets for 10s
            const _resetTimer = () => {
                if (recEndTimer) clearTimeout(recEndTimer);
                recEndTimer = setTimeout(() => {
                    console.log(`[Rec] ⏹ No packets for 10s — stopping rec for ${targetPhone}`);
                    recActive = false;
                    if (recChannels[targetPhone]) {
                        try { recChannels[targetPhone].ffmpeg.stdin.end(); } catch(_) {}
                    }
                    activeDownloads[targetPhone] = { channel: ch, active: false };
                    console.log(`[Rec] rec stopped, live stream resuming`);
                }, 10000);
            };
            activeDownloads[targetPhone].resetTimer = _resetTimer;
 
            // Set notify callback BEFORE activating rec
            const _notifyPhone = targetPhone;
            recChannels[targetPhone].onReady = () => {
                // Don't notify yet — wait for file to have data (poll server-side)
                console.log(`[Rec] First I-frame received, waiting for file to grow...`);
                let sizeCheck = 0;
                const checkSize = setInterval(() => {
                    try {
                        const stat = fs.statSync(`./public/rec_${_notifyPhone}.ts`);
                        console.log(`[Rec] File size check: ${stat.size} bytes (attempt ${sizeCheck})`);
                        if (stat.size > 500000) {
                            clearInterval(checkSize);
                            console.log(`[Rec] ✅ File ready (${stat.size} bytes), converting to MP4...`);
                            const tsPath  = `./public/rec_${_notifyPhone}.ts`;
                            const mp4Path = `./public/rec_${_notifyPhone}.mp4`;
                            const ff = spawn('/usr/local/bin/ffmpeg', [
                                '-y',
                                '-fflags',          '+genpts+discardcorrupt+igndts',
                                '-err_detect',      'ignore_err',
                                '-i',               tsPath,
                                '-c:v',             'libx264',
                                '-preset',          'ultrafast',
                                '-crf',             '28',
                                '-movflags',        '+faststart',
                                '-an',
                                mp4Path
                            ]);
                            ff.stderr.on('data', d => console.log('[RecConvert]', d.toString().trim()));
                            ff.stderr.on('data', d => {
                                const m = d.toString().trim();
                                if (m.includes('error') || m.includes('Error')) {
                                    console.error('[RecConvert]', m);
                                }
                            });
                            ff.on('close', code => {
                                console.log(`[Rec] Convert done code=${code} → ${mp4Path}`);
                                if (code === 0) {
                                    wss.clients.forEach(c => {
                                        if (c.readyState === 1) c.send(JSON.stringify({
                                            type: 'recording_ready',
                                            url:  `/public/rec_${_notifyPhone}.mp4`,
                                        }));
                                    });
                                } else {
                                    // Fallback — try sending ts anyway
                                    wss.clients.forEach(c => {
                                        if (c.readyState === 1) c.send(JSON.stringify({
                                            type: 'recording_ready',
                                            url:  tsPath.replace('./', '/'),
                                        }));
                                    });
                                }
                            });
                            ff.on('error', err => console.error('[RecConvert] spawn error:', err.message));
                        }
                    } catch(_) {}
                    if (++sizeCheck > 60) clearInterval(checkSize);
                }, 1000);
            };
            // ACTIVATE rec mode THEN send command
            recActive = true;
            _resetTimer();
            console.log(`[Rec] recActive = true, sending 0x9201`);
 
            // Stop any previous playback first
            tcpSockets[targetPhone].write(buildPlaybackStop(targetPhone, ch));
            
            // Small delay then send new playback request
            setTimeout(() => {
                if (tcpSockets[targetPhone] && !tcpSockets[targetPhone].destroyed) {
                    const frame = buildVideoPlaybackRequest(targetPhone, ch, startTime, endTime);
                    tcpSockets[targetPhone].write(frame);
                    console.log(`[Rec] Sent 0x9201 for ${startTime}→${endTime}`);
                }
            }, 500);
            ws.send(JSON.stringify({ type: 'status', message: '⏳ Buffering recording...' }));
        }
 
        // ── stop_playback: browser closed modal ───────────────────────────
        if (msg.type === 'stop_playback') {
            console.log(`[Rec] stop_playback received`);
            recActive = false;
            if (recEndTimer) { clearTimeout(recEndTimer); recEndTimer = null; }
            const ph = Object.keys(tcpSockets)[0];
            if (ph && tcpSockets[ph] && !tcpSockets[ph].destroyed) {
                tcpSockets[ph].write(buildPlaybackStop(ph, 1));
                console.log(`[Rec] Sent 0x9202 stop to ${ph}`);
            }
            Object.keys(activeDownloads).forEach(p => {
                if (recChannels[p]) {
                    try { recChannels[p].ffmpeg.stdin.end(); } catch(_) {}
                    delete recChannels[p];
                }
                delete activeDownloads[p];
            });
        }
    });

    ws.on('close', () => console.log('[WS] Browser disconnected'));
    ws.on('error', err => console.error('[WS] Browser socket error:', err.message));
});
// ── HTTP server ───────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    // Strip query string
    const urlPath = req.url.split('?')[0];
    let filePath;
    if (urlPath === '/') {
        filePath = './video.html';
    } else if (urlPath.startsWith('/public/')) {
        filePath = `.${urlPath}`;
    } else if (urlPath.startsWith('/recordings/')) {
        filePath = `.${urlPath}`;
    } else {
        filePath = `.${urlPath}`;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentTypes = {
        '.html': 'text/html',
        '.js':   'application/javascript',
        '.m3u8': 'application/vnd.apple.mpegurl',
        '.ts':   'video/mp2t',
        '.mp4':  'video/mp4',
        '.avi':  'video/x-msvideo',
    };

    // For HEAD requests just return file size via stat
    if (req.method === 'HEAD') {
        fs.stat(filePath, (err, stat) => {
            if (err) { res.writeHead(404); res.end(); return; }
            res.writeHead(200, {
                'Content-Type':   contentTypes[ext] || 'application/octet-stream',
                'Content-Length': stat.size,
                'Access-Control-Allow-Origin': '*',
                'Cache-Control':  'no-cache',
            });
            res.end();
        });
        return;
    }

    fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); res.end('Not found'); return; }
        res.writeHead(200, {
            'Content-Type': contentTypes[ext] || 'text/plain',
            'Content-Length': data.length,
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-cache',
        });
        res.end(data);
    });
}).listen(CONFIG.httpPort, () => console.log(`✓ HTTP on :${CONFIG.httpPort}`));

// ── FFmpeg for HLS ────────────────────────────────────────────────────────────
// Root cause of "slice type 32 too large", "non-existing PPS 175":
// The camera sends AVS video (Chinese national standard, codec code 100 in
// T/98 protocol Table 12), NOT H.264. AVS has completely different NAL/slice
// structures that FFmpeg misreads when told -f h264.
//
// Fix: wrap raw AVS frames in MPEG-TS packets (stream type 0x42 = AVS) and
// feed that to FFmpeg as -f mpegts. FFmpeg then uses its AVS demuxer/decoder
// and transcodes to libx264 for browser-compatible HLS output.
function startFFmpeg(channel) {
    const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
        '-fflags',          '+genpts+discardcorrupt+igndts',
        '-err_detect',      'ignore_err',
        '-f',               'mpegts',        // MPEG-TS input — auto-detects AVS via stream type 0x42
        '-probesize',       '500000',        // enough bytes for AVS codec detection
        '-analyzeduration', '1000000',
        '-i',               'pipe:0',
        '-c:v',             'libx264',       // transcode AVS → H.264 for browser/HLS
        '-preset',          'ultrafast',
        '-tune',            'zerolatency',
        '-g',               '50',
        '-keyint_min',      '25',
        '-f',               'hls',
        '-hls_time',        '1',
        '-hls_list_size',   '3',
        '-hls_flags',       'delete_segments+append_list',
        '-hls_segment_filename', `./public/ch${channel}_%03d.ts`,
        `./public/ch${channel}.m3u8`,
    ]);

    ffmpeg.stderr.on('data', d => {
        const msg = d.toString().trim();
        if (msg.includes('Error') || msg.includes('Invalid') || msg.includes('error')) {
            console.error(`FFmpeg ch${channel}:`, msg);
        }
    });

    ffmpeg.on('close', code => {
        console.log(`FFmpeg ch${channel} closed (code ${code}), restarting in 1s...`);
        setTimeout(() => { channels[channel].ffmpeg = startFFmpeg(channel); }, 1000);
    });

    return ffmpeg;
}

const channels = {
    1: { ffmpeg: null, gotIFrame: false, subpackets: [] },
};

// ── Recording playback FFmpeg instances ───────────────────────────────────────
const recChannels = {}; // { [phone]: { ffmpeg, gotIFrame, subpackets, tsCounter, patPmtSent } }

function startRecFFmpeg(phone, channel) {
    const outPath = `./public/rec_${phone}.ts`;
    const writeStream = fs.createWriteStream(outPath);
    console.log(`[Rec] ⏺ Raw TS writer started: ${outPath}`);
    writeStream.on('error', err => console.error(`[Rec] Write error:`, err.message));
    // NOTE: PAT/PMT written by handleRecFrame on first frame (patPmtSent flag)
    return {
        stdin: {
            writable: true,
            write: (data) => { writeStream.write(data); return true; },
            end: () => {
                writeStream.end(() => console.log(`[Rec] ✅ TS file complete: ${outPath}`));
            }
        },
        stderr: { on: () => {} },
        on: () => {}
    };
}

function handleRecFrame(frameData, phone, dataType) {
    const rc = recChannels[phone];
    if (!rc) {
        console.warn(`[Rec] handleRecFrame: no recChannel for phone=${phone}`);
        return;
    }
    const isVideo = (dataType === 0 || dataType === 1 || dataType === 2);
    if (!isVideo) return;
 
    if (dataType === 0) {
        // Log first I-frame bytes to identify codec
        if (!rc.gotIFrame) {
            console.log(`[Rec] First I-frame: ${frameData.slice(0,8).toString('hex')} len=${frameData.length}`);
        }
        if (!rc.gotIFrame && rc.onReady) {
            rc.onReady();
            rc.onReady = null;
        }
        rc.gotIFrame = true;
    } else if (!rc.gotIFrame) {
        return; // drop P/B until first I-frame
    }
 
    if (!rc.ffmpeg || !rc.ffmpeg.stdin.writable) {
        console.warn(`[Rec] handleRecFrame: ffmpeg not writable for phone=${phone}`);
        return;
    }
 
    // Write PAT+PMT once — use same AVS type as live stream
    if (!rc.patPmtSent) {
        rc.ffmpeg.stdin.write(buildPAT());
        rc.ffmpeg.stdin.write(buildPMT()); // AVS 0x42 — same as live
        rc.patPmtSent = true;
        console.log(`[Rec] Wrote PAT+PMT to TS file`);
    }
 
    const { packets, nextCounter } = wrapFrameInTS(frameData, rc.tsCounter);
    rc.tsCounter = nextCounter;
    for (const pkt of packets) rc.ffmpeg.stdin.write(pkt);
}

function processRecPacket(rawData, phone, dataType, subpktMarker) {
    const rc = recChannels[phone];
    if (!rc) {
        console.warn(`[Rec] processRecPacket: no recChannel for phone=${phone}, keys=${JSON.stringify(Object.keys(recChannels))}`);
        return;
    }
    if (subpktMarker === 0) {
        handleRecFrame(rawData, phone, dataType);
    } else if (subpktMarker === 1) {
        rc.subpackets = [rawData];
    } else if (subpktMarker === 3) {
        if (rc.subpackets.length > 0) rc.subpackets.push(rawData);
    } else if (subpktMarker === 2) {
        if (rc.subpackets.length > 0) {
            rc.subpackets.push(rawData);
            const complete = Buffer.concat(rc.subpackets);
            rc.subpackets = [];
            handleRecFrame(complete, phone, dataType);
        }
    }
}

channels[1].ffmpeg = startFFmpeg(1);

// ── MPEG-TS wrapping for AVS frames ──────────────────────────────────────────
// We build a minimal but valid MPEG-TS stream:
//   PAT  (PID 0)    — maps program 1 to PMT PID 0x1000
//   PMT  (PID 4096) — declares stream type 0x42 (AVS) on PID 256
//   PES  (PID 256)  — one PES per video frame, sliced into 188-byte TS packets

const TS_PACKET_SIZE = 188;
const VIDEO_PID      = 256;   // elementary video stream PID
const PMT_PID        = 4096;  // 0x1000

function buildPAT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0] = 0x47; pkt[1] = 0x40; pkt[2] = 0x00; pkt[3] = 0x10; // PID=0, PUSI
    pkt[4] = 0x00; // pointer field
    // PAT section
    const s = pkt.slice(5);
    s[0]  = 0x00;              // table_id = PAT
    s[1]  = 0xB0; s[2] = 0x0D;// section_syntax_indicator + length=13
    s[3]  = 0x00; s[4] = 0x01; // transport_stream_id=1
    s[5]  = 0xC1;              // version=0, current=1
    s[6]  = 0x00; s[7] = 0x00; // section 0 of 0
    s[8]  = 0x00; s[9] = 0x01; // program_number=1
    s[10] = (PMT_PID >> 8) | 0xE0;
    s[11] = PMT_PID & 0xFF;    // PMT PID
    // CRC32 omitted — FFmpeg tolerates missing CRC with err_detect ignore_err
    return pkt;
}

function buildPMT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0] = 0x47;
    pkt[1] = 0x40 | ((PMT_PID >> 8) & 0x1F);
    pkt[2] = PMT_PID & 0xFF;
    pkt[3] = 0x10; // PID, PUSI, no adaptation
    pkt[4] = 0x00; // pointer field
    const s = pkt.slice(5);
    s[0]  = 0x02;              // table_id = PMT
    s[1]  = 0xB0; s[2] = 0x12;// length=18
    s[3]  = 0x00; s[4] = 0x01; // program_number=1
    s[5]  = 0xC1;
    s[6]  = 0x00; s[7] = 0x00;
    s[8]  = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[9]  = VIDEO_PID & 0xFF;  // PCR PID = VIDEO_PID
    s[10] = 0xF0; s[11] = 0x00;// no program_info
    // Stream descriptor
    s[12] = 0x42;              // stream_type: AVS (GB/T 20090-1, ISO 13818-1 user private 0x42)
    s[13] = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[14] = VIDEO_PID & 0xFF;  // elementary PID
    s[15] = 0xF0; s[16] = 0x00;// no ES_info
    return pkt;
}

function buildPMT_HEVC() {
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
    s[12] = 0x24;              // ← HEVC stream type (was 0x42 AVS)
    s[13] = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[14] = VIDEO_PID & 0xFF;
    s[15] = 0xF0; s[16] = 0x00;
    return pkt;
}

function wrapFrameInTS(frameData, counter) {
    // Minimal PES header for video (stream_id 0xE0, no PTS)
    const pesHdr = Buffer.from([
        0x00, 0x00, 0x01, // start code
        0xE0,             // stream_id: video
        0x00, 0x00,       // PES_packet_length = 0 (unbounded)
        0x80,             // flags: no PTS/DTS
        0x00,             // PTS_DTS_flags = 0
        0x00,             // header_data_length = 0
    ]);
    const pes = Buffer.concat([pesHdr, frameData]);

    const packets = [];
    let pos = 0; let first = true; let ctr = counter;

    while (pos < pes.length) {
        const pkt  = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
        pkt[0] = 0x47;
        pkt[1] = (first ? 0x40 : 0x00) | ((VIDEO_PID >> 8) & 0x1F);
        pkt[2] = VIDEO_PID & 0xFF;
        pkt[3] = 0x10 | (ctr & 0x0F);     // payload only, continuity counter
        ctr    = (ctr + 1) & 0x0F;

        const room  = TS_PACKET_SIZE - 4;
        const chunk = pes.slice(pos, pos + room);
        chunk.copy(pkt, 4);
        pos  += chunk.length;
        first = false;
        packets.push(pkt);
    }

    return { packets, nextCounter: ctr };
}

let patPmtSent = false;
let tsCounter  = 0;

// ── Handle one complete video frame ──────────────────────────────────────────
function handleVideoFrame(frameData, channel, dataType) {
    const ch = channels[channel];
    if (!ch) return;

    const isVideo  = (dataType === 0 || dataType === 1 || dataType === 2);
    if (!isVideo) return;

    if (dataType === 0) {
        ch.gotIFrame = true;
        console.log(`ch${channel} ✅ I_FRAME size:${frameData.length}`);
    } else {
        if (!ch.gotIFrame) return;  // drop P/B until first I-frame
        console.log(`ch${channel} ${dataType === 1 ? 'P' : 'B'}_FRAME size:${frameData.length}`);
    }

    if (!ch.ffmpeg || !ch.ffmpeg.stdin.writable) return;

    // Send PAT+PMT once so FFmpeg learns the stream structure before any PES
    if (!patPmtSent) {
        ch.ffmpeg.stdin.write(buildPAT());
        ch.ffmpeg.stdin.write(buildPMT());
        patPmtSent = true;
        console.log(`ch${channel} 📺 Sent PAT+PMT (AVS stream type 0x42)`);
    }

    const { packets, nextCounter } = wrapFrameInTS(frameData, tsCounter);
    tsCounter = nextCounter;
    for (const pkt of packets) ch.ffmpeg.stdin.write(pkt);
}

// ── Reassemble subpackets (T/98 protocol section 5.5.3) ──────────────────────
// subpktMarker: 0=atomic, 1=first, 3=middle, 2=last
function processVideoPacket(rawData, channel, dataType, subpktMarker) {
    const ch = channels[channel];
    if (!ch) return;

    if (subpktMarker === 0) {
        handleVideoFrame(rawData, channel, dataType);

    } else if (subpktMarker === 1) {
        ch.subpackets = [rawData];

    } else if (subpktMarker === 3) {
        if (ch.subpackets.length > 0) ch.subpackets.push(rawData);

    } else if (subpktMarker === 2) {
        if (ch.subpackets.length > 0) {
            ch.subpackets.push(rawData);
            const complete = Buffer.concat(ch.subpackets);
            ch.subpackets  = [];
            console.log(`ch${channel} complete frame size:${complete.length}`);
            handleVideoFrame(complete, channel, dataType);
        }
    }
}

// ── JT/T 808 helpers ─────────────────────────────────────────────────────────
function unescapeBuffer(buf) {
    const out = []; let i = 0;
    while (i < buf.length) {
        if      (buf[i] === 0x7D && buf[i+1] === 0x02) { out.push(0x7E); i += 2; }
        else if (buf[i] === 0x7D && buf[i+1] === 0x01) { out.push(0x7D); i += 2; }
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

    // BCD-encode phone padded to 12 digits
    const phoneStr = String(phone).padStart(12, '0');
    Buffer.from(phoneStr.match(/.{2}/g)
        .map(v => { const n = parseInt(v,10); return ((Math.floor(n/10)<<4)|(n%10)); })
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
    body.writeUInt16BE(0,          3 + N);  // UDP port = 0 (TCP only)
    body[5 + N] = channel;
    body[6 + N] = 1;   // video only (per Table 17: 1=Video)
    body[7 + N] = 1;   // main stream
    return buildFrame(0x9101, body, phone);
}

function parseAdditionalInfo(buf) {
    const result = {};
    let i = 0;
    while (i < buf.length - 2) {
        const id  = buf[i];
        const len = buf[i+1];
        if (i + 2 + len > buf.length) break;
        const val = buf.slice(i+2, i+2+len);
        switch(id) {
            case 0x01: if (val.length >= 4) result.mileage        = val.readUInt32BE(0) / 10 + ' km';   break;
            case 0x03: if (val.length >= 2) result.sensorSpeed    = val.readUInt16BE(0) / 10 + ' km/h'; break;
            case 0x25: if (val.length >= 2) result.voltage        = val.readUInt16BE(0) / 10 + ' V';    break;
            case 0x30: if (val.length >= 1) result.signalStrength  = val[0];                            break;
            case 0x31: if (val.length >= 1) result.satellites     = val[0];                             break;
        }
        i += 2 + len;
    }
    return result;
}

// ── Build 0x8103 — parameter query (channel list) ────────────────────────────
function buildParamQuery(phone) {
    const body = Buffer.alloc(4);
    body.writeUInt32BE(0x0076, 0); // query param: audio/video channel list
    return buildFrame(0x8103, body, phone);
}

// ── Build 0x9205 — query recording list ──────────────────────────────────────
function buildQueryRecordings(phone, startDate, endDate) {
    const toBCDBytes = (yy, mo, dd, hh, mm, ss) => Buffer.from([
        ((Math.floor(yy/10)<<4)|(yy%10)),
        ((Math.floor(mo/10)<<4)|(mo%10)),
        ((Math.floor(dd/10)<<4)|(dd%10)),
        ((Math.floor(hh/10)<<4)|(hh%10)),
        ((Math.floor(mm/10)<<4)|(mm%10)),
        ((Math.floor(ss/10)<<4)|(ss%10)),
    ]);

    // Default: last 30 days to today
    const now  = new Date();
    const past = new Date(now.getTime() - 30*24*60*60*1000);

    const [sDate, sTime] = startDate ? startDate.split(' ') : [`${past.getFullYear()}-${String(past.getMonth()+1).padStart(2,'0')}-${String(past.getDate()).padStart(2,'0')}`, '00:00:00'];
    const [eDate, eTime] = endDate   ? endDate.split(' ')   : [`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`, '23:59:59'];

    const [sY,sM,sD] = sDate.split('-').map(Number);
    const [sH,sm,sS] = (sTime||'00:00:00').split(':').map(Number);
    const [eY,eM,eD] = eDate.split('-').map(Number);
    const [eH,em,eS] = (eTime||'23:59:59').split(':').map(Number);

    const body = Buffer.alloc(23);
    body[0] = 0; // all channels
    toBCDBytes(sY%100, sM, sD, sH, sm, sS).copy(body, 1);
    toBCDBytes(eY%100, eM, eD, eH, em, eS).copy(body, 7);
    body.fill(0x00, 13, 21); // no alarm filter
    body[21] = 2; // video only
    body[22] = 0; // all streams
    // console.log(`[Rec] buildQueryRecordings: ${sDate} ${sTime} → ${eDate} ${eTime}`);
    return buildFrame(0x9205, body, phone);
}

// ── Build 0x9206 — FTP upload request ────────────────────────────────────────
function buildFtpUploadRequest(phone, channel, startTime, endTime) {
    const serverIp   = CONFIG.serverIp;
    const ftpPort = 21;
    const ftpUser    = 'anonymous';
    const ftpPass    = 'anonymous';
    // const uploadPath = '/';
    const uploadPath = `/ch${channel}_${startTime.replace(/[: -]/g, '')}.mp4`;

    const toBCDBytes = (yy, mo, dd, hh, mm, ss) => Buffer.from([
        ((Math.floor(yy/10)<<4)|(yy%10)),
        ((Math.floor(mo/10)<<4)|(mo%10)),
        ((Math.floor(dd/10)<<4)|(dd%10)),
        ((Math.floor(hh/10)<<4)|(hh%10)),
        ((Math.floor(mm/10)<<4)|(mm%10)),
        ((Math.floor(ss/10)<<4)|(ss%10)),
    ]);

    const [sDate, sTime='00:00:00'] = startTime.split(' ');
    const [eDate, eTime='23:59:59'] = endTime.split(' ');
    const [sY,sM,sD] = sDate.split('-').map(Number);
    const [sH,sm,sS] = sTime.split(':').map(Number);
    const [eY,eM,eD] = eDate.split('-').map(Number);
    const [eH,em,eS] = eTime.split(':').map(Number);

    const ipBuf   = Buffer.from(serverIp, 'ascii');
    const userBuf = Buffer.from(ftpUser,  'ascii');
    const passBuf = Buffer.from(ftpPass,  'ascii');
    const pathBuf = Buffer.from(uploadPath,'ascii');

    // Table 26 layout: ipLen(1) ip(k) port(2) userLen(1) user(l)
    //                  passLen(1) pass(m) pathLen(1) path(n)
    //                  logicalCh(1) startBCD(6) endBCD(6)
    //                  alarmLogo(8) avType(1) streamType(1) storageType(1) taskCondition(1)
    const k = ipBuf.length, l = userBuf.length, m = passBuf.length, n = pathBuf.length;
    const body = Buffer.alloc(1+k+2+1+l+1+m+1+n+1+6+6+8+1+1+1+1);   // no UDP port
    let p = 0;
    body[p++] = k;                               ipBuf.copy(body, p);   p += k;
    body.writeUInt16BE(ftpPort, p);              p += 2;  // TCP FTP port only
    body[p++] = l;                               userBuf.copy(body, p); p += l;
    body[p++] = m;                               passBuf.copy(body, p); p += m;
    body[p++] = n;                               pathBuf.copy(body, p); p += n;
    body[p++] = channel;
    toBCDBytes(sY%100,sM,sD,sH,sm,sS).copy(body, p); p += 6;
    toBCDBytes(eY%100,eM,eD,eH,em,eS).copy(body, p); p += 6;
    body.fill(0x00, p, p+8);                     p += 8;
    body[p++] = 0;    // avType: all (0=all, 1=audio, 2=video)
    body[p++] = 0;    // stream: all
    body[p++] = 0;    // storage: all
    body[p++] = 0xFF; // taskCond: allow on ALL networks (4G, WiFi, any)

    let frame = buildFrame(0x9206, body, phone);  // ← use phone parameter
    // console.log(`[Rec] FTP upload request frame size: ${frame.length} bytes`);
    // console.log(`[Rec] Frame hex: ${frame.toString('hex')}`);
    return frame;
}

// ── Build 0x9201 — remote video playback request ─────────────────────────────
// ── Build 0x9201 — remote video playback request ─────────────────────────────
function buildVideoPlaybackRequest(phone, channel, startTime, endTime) {
    const serverIp   = CONFIG.serverIp;
    const serverPort = CONFIG.tcpPort;

    const toBCDBytes = (yy, mo, dd, hh, mm, ss) => Buffer.from([
        ((Math.floor(yy/10)<<4)|(yy%10)),
        ((Math.floor(mo/10)<<4)|(mo%10)),
        ((Math.floor(dd/10)<<4)|(dd%10)),
        ((Math.floor(hh/10)<<4)|(hh%10)),
        ((Math.floor(mm/10)<<4)|(mm%10)),
        ((Math.floor(ss/10)<<4)|(ss%10)),
    ]);

    const [sDate, sTime='00:00:00'] = startTime.split(' ');
    const [eDate, eTime='23:59:59'] = endTime.split(' ');
    const [sY,sM,sD] = sDate.split('-').map(Number);
    const [sH,sm,sS] = sTime.split(':').map(Number);
    const [eY,eM,eD] = eDate.split('-').map(Number);
    const [eH,em,eS] = eTime.split(':').map(Number);

    const ipBuf = Buffer.from(serverIp, 'ascii');
    const n = ipBuf.length;

    // Table 24: ipLen(1) ip(n) tcpPort(2) udpPort(2) ch(1)
    //           avType(1) streamType(1) memType(1) playMode(1) speed(1)
    //           startBCD(6) endBCD(6)
    const body = Buffer.alloc(1 + n + 2 + 2 + 1 + 1 + 1 + 1 + 1 + 1 + 6 + 6);
    let p = 0;
    body[p++] = n;
    ipBuf.copy(body, p); p += n;
    body.writeUInt16BE(serverPort, p); p += 2;
    body.writeUInt16BE(0,          p); p += 2; // UDP = 0
    body[p++] = channel;
    body[p++] = 2; // avType: video only
    body[p++] = 1; // main stream
    body[p++] = 0; // main or disaster storage
    body[p++] = 0; // normal playback
    body[p++] = 0; // speed = invalid for normal
    toBCDBytes(sY%100,sM,sD,sH,sm,sS).copy(body, p); p += 6;
    toBCDBytes(eY%100,eM,eD,eH,em,eS).copy(body, p); p += 6;

    console.log(`[Rec] 0x9201 playback ch:${channel} ${startTime}→${endTime}`);
    return buildFrame(0x9201, body, phone);
}
// ── Build 0x9202 — playback control (stop) ───────────────────────────────────
function buildPlaybackStop(phone, channel) {
    const body = Buffer.alloc(9);
    body[0] = channel;
    body[1] = 2; // 2 = end playback
    // rest is zeros
    return buildFrame(0x9202, body, phone);
}
function buildPlaybackControl(phone, channel, control) {
    const body = Buffer.alloc(9);
    body[0] = channel;
    body[1] = control; // 0=start, 1=pause, 2=end
    body[2] = 0;       // speed multiplier
    body.fill(0, 3, 9); // drag position (unused)
    return buildFrame(0x9202, body, phone);
}

// ── TCP server ────────────────────────────────────────────────────────────────
const tcpServer = net.createServer(socket => {
    const remote = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`Device connected: ${remote}`);
    let buffer = Buffer.alloc(0);
    let phone  = null;

    socket.on('data', data => {
        try {
            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length - 4) {

                // ── Stream data packet (T/98 §5.5.3 — 0x30316364 header) ────
                if (buffer[offset]   === 0x30 && buffer[offset+1] === 0x31 &&
                    buffer[offset+2] === 0x63 && buffer[offset+3] === 0x64) {

                    if (offset + 30 > buffer.length) break;
                    const dataBodyLen = buffer.readUInt16BE(offset + 28);
                    if (offset + 30 + dataBodyLen > buffer.length) break;

                    const byte15       = buffer[offset + 15];
                    const dataType     = (byte15 >> 4) & 0x0F;
                    const subpktMarker = byte15 & 0x0F;
                    const channel      = buffer[offset + 14];
                    const rawData      = buffer.slice(offset + 30, offset + 30 + dataBodyLen);

                    // Identify packet as recording vs live by comparing timestamp
                    // to current time — recording packets have historical timestamps
                    let isRec = false;
                    if (recActive && Object.keys(activeDownloads).length > 0) {
                        try {
                            const tsMs   = buffer.readBigUInt64BE(offset + 16);
                            const tsDate = Number(tsMs);
                            const nowMs  = Date.now();
                            // If timestamp is more than 60 seconds in the past → recording
                            const diffMs = nowMs - tsDate;
                            isRec = diffMs > 60000;
                            if (dataType === 0) {
                                console.log(`[PKT] socket=${remote} ts=${new Date(tsDate).toISOString()} diff=${Math.round(diffMs/1000)}s isRec=${isRec}`);
                            }
                        } catch(_) {
                            isRec = false;
                        }
                    }

                    if (isRec) {
                        const recPhone = Object.keys(activeDownloads)[0];
                        if (activeDownloads[recPhone]?.resetTimer) {
                            activeDownloads[recPhone].resetTimer();
                        }
                        processRecPacket(rawData, recPhone, dataType, subpktMarker);
                    } else {
                        processVideoPacket(rawData, channel, dataType, subpktMarker);
                    }

                    offset += 30 + dataBodyLen;
                    continue;
                }

                // ── Signalling packet (JT/T 808 — 0x7E framing) ─────────────
                if (buffer[offset] === 0x7E) {
                    // console.log("data ----------- ", data);
                    // console.log("data hex ------- ", data.toString('hex'));
                    // console.log("buffer -------- ", buffer);
                    const end = buffer.indexOf(0x7E, offset + 1);
                    // if (end !== -1) {
                    //     // Forward the complete raw 0x7E…0x7E frame to the remote server
                    //     tcpForwarder.sendSignallingPacket(buffer.slice(offset, end + 1));
                    // }
                    if (end === -1) break;

                    const inner     = buffer.slice(offset + 1, end);
                    const unescaped = unescapeBuffer(inner);
                    if (unescaped.length < 12) { offset = end + 1; continue; }

                    const msgId = unescaped.readUInt16BE(0);
                    phone = unescaped.slice(4, 10)
                        .map(b => `${(b >> 4) & 0x0F}${b & 0x0F}`)
                        .join('')
                        .replace(/^0+/, '');
                    const seq  = unescaped.readUInt16BE(10);
                    const body = unescaped.slice(12);
                    console.log(`[signalling] msgId: 0x${msgId.toString(16).padStart(4,'0')} phone: ${phone}`);
                    if (msgId === 0x0001) {
                        // General response from device to our commands
                        const replySeq    = body.readUInt16BE(0);
                        const replyMsgId  = body.readUInt16BE(2);
                        const replyResult = body[4];
                        const resultText  = ['Success','Failed','Wrong Msg','Not Supported','Alarm Confirmed','Update Required'][replyResult] || `Unknown(${replyResult})`;
                        console.log(`[ACK] 0x0001 replyTo:0x${replyMsgId.toString(16).padStart(4,'0')} seq:${replySeq} result:${replyResult} (${resultText})`);
                    }
                    if (msgId === 0x0100) {
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    } else if (msgId === 0x0102) {
                        socket.write(buildAck(phone, seq, msgId));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
                        tcpSockets[phone] = socket;
                        socket._phone = phone;  // ADD THIS
                        console.log(`[signalling] Registered socket for ${phone}`);
                        // Step 1: param query handshake (required before 0x9205 on SDK V6.07)
                        setTimeout(() => {
                            if (!socket.destroyed) {
                                socket.write(buildParamQuery(phone));
                                console.log(`[signalling] Sent 0x8103 to ${phone}`);
                            }
                        }, 2000);
                        // Step 2: query recording list after handshake
                        setTimeout(() => {
                            if (!socket.destroyed) {
                                socket.write(buildQueryRecordings(phone));
                                console.log(`[signalling] Sent 0x9205 to ${phone}`);
                            }
                        }, 5000);

                    } else if (msgId === 0x0200) {
                        socket.write(buildAck(phone, seq, msgId));

                        const alarmFlags = body.readUInt32BE(0);
                        const statusBits = body.readUInt32BE(4);
                        const latRaw     = body.readUInt32BE(8);
                        const lonRaw     = body.readUInt32BE(12);
                        const elevation  = body.readUInt16BE(16);
                        const speed      = body.readUInt16BE(18) / 10;
                        const direction  = body.readUInt16BE(20);

                        const south   = !!(statusBits & (1 << 2));
                        const west    = !!(statusBits & (1 << 3));
                        const lat     = latRaw / 1e6 * (south ? -1 : 1);
                        const lon     = lonRaw / 1e6 * (west  ? -1 : 1);
                        const accOn   = !!(statusBits & (1 << 0));
                        const located = !!(statusBits & (1 << 1));

                        const bcd        = b => ((b >> 4) * 10 + (b & 0x0F));
                        const timeOffset = 22;
                        const yy = bcd(body[timeOffset]);
                        const mo = bcd(body[timeOffset+1]);
                        const dd = bcd(body[timeOffset+2]);
                        const hh = bcd(body[timeOffset+3]);
                        const mm = bcd(body[timeOffset+4]);
                        const ss = bcd(body[timeOffset+5]);
                        const dt = `20${String(yy).padStart(2,'0')}-${String(mo).padStart(2,'0')}-${String(dd).padStart(2,'0')} ${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;

                        const alarms = [];
                        if (alarmFlags & (1<<0)) alarms.push('Emergency');
                        if (alarmFlags & (1<<1)) alarms.push('Overspeed');
                        if (alarmFlags & (1<<4)) alarms.push('GNSS Fault');
                        if (alarmFlags & (1<<5)) alarms.push('GNSS Antenna Cut');
                        if (alarmFlags & (1<<7)) alarms.push('Low Voltage');
                        if (alarmFlags & (1<<8)) alarms.push('Power Off');

                        const locationData = {
                            type: 'location', phone, lat, lon, speed, direction,
                            elevation, datetime: dt, accOn, located, alarms,
                            ...parseAdditionalInfo(body.slice(27))
                        };

                        console.log(`[GPS] ${phone} lat=${lat} lon=${lon} speed=${speed}km/h dir=${direction}° dt=${dt}`);

                        wss.clients.forEach(client => {
                            if (client.readyState === 1) client.send(JSON.stringify(locationData));
                        });

                        // Append one CSV line per GPS report to today's log file
                        const fileName  = `gps_log_${new Date().toISOString().slice(0,10)}.txt`;
                        const gpsRecord = {
                            phone,
                            datetime:        dt,
                            latitude:        lat,
                            longitude:       lon,
                            speed_kmh:       speed,
                            direction_deg:   direction,
                            elevation_m:     elevation,
                            acc:             accOn    ? 'ON'    : 'OFF',
                            located:         located  ? 'YES'   : 'NO',
                            mileage:         locationData.mileage        || '0',
                            voltage:         locationData.voltage        || '0',
                            satellites:      locationData.satellites     || '0',
                            signal:          locationData.signalStrength || '0',
                            sensor_speed:    locationData.sensorSpeed    || '0',
                            oil_circuit:     !!(statusBits & (1<<10)) ? 'CUT'  : 'NORMAL',
                            vehicle_circuit: !!(statusBits & (1<<11)) ? 'CUT'  : 'NORMAL',
                            door:            !!(statusBits & (1<<13)) ? 'OPEN' : 'CLOSED',
                            alarms:          alarmFlags !== 0 ? [
                                (alarmFlags & (1<<0)) ? 'EMERGENCY'   : null,
                                (alarmFlags & (1<<1)) ? 'OVERSPEED'   : null,
                                (alarmFlags & (1<<4)) ? 'GNSS_FAULT'  : null,
                                (alarmFlags & (1<<5)) ? 'ANTENNA_CUT' : null,
                                (alarmFlags & (1<<7)) ? 'LOW_VOLTAGE' : null,
                                (alarmFlags & (1<<8)) ? 'POWER_OFF'   : null,
                            ].filter(Boolean).join('|') : 'NONE',
                        }

                        // Forward GPS record to remote TCP server
                        tcpForwarder.sendGpsRecord(gpsRecord);
                        fs.appendFile(`./${fileName}`, Object.values(gpsRecord).join(',') + '\n', err => {
                            if (err) console.error('[GPS LOG] write error:', err.message);
                        });

                    } else if (msgId === 0x0104) {
                        // Response to our 0x8103 param query — device is ready
                        socket.write(buildAck(phone, seq, msgId));
                        console.log(`[signalling] 0x0104 param response from ${phone} — device ready`);

                    } else if (msgId === 0x1205) {
                        // Device SD card recording list
                        socket.write(buildAck(phone, seq, msgId));
                        // If this 0x1205 came after a 0x9201 playback request,
                        // it means the device finished sending the recording
                        if (activeDownloads[phone]) {
                            console.log(`[Rec] 0x1205 received — recording stream complete for ${phone}`);
                            activeDownloads[phone].active = false;
                            delete activeDownloads[phone];
                        }
                        console.log(`[Rec] 0x1205 from ${phone} bodyLen:${body.length}`);
                        try {
                            if (body.length < 6) { console.warn('[Rec] Body too short'); offset = end+1; continue; }
                            const totalItems = body.readUInt32BE(2);
                            console.log(`[Rec] totalItems:${totalItems}`);
                            const bcd = b => ((b>>4)*10+(b&0x0F));
                            const recs = []; let p = 6;
                            while (p + 28 <= body.length) {
                                const ch = body[p];
                                const sY=bcd(body[p+1]),sM=bcd(body[p+2]),sD=bcd(body[p+3]);
                                const sH=bcd(body[p+4]),sm=bcd(body[p+5]),sS=bcd(body[p+6]);
                                const eY=bcd(body[p+7]),eM=bcd(body[p+8]),eD=bcd(body[p+9]);
                                const eH=bcd(body[p+10]),em=bcd(body[p+11]),eS=bcd(body[p+12]);
                                const startTime = `20${String(sY).padStart(2,'0')}-${String(sM).padStart(2,'0')}-${String(sD).padStart(2,'0')} ${String(sH).padStart(2,'0')}:${String(sm).padStart(2,'0')}:${String(sS).padStart(2,'0')}`;
                                const endTime   = `20${String(eY).padStart(2,'0')}-${String(eM).padStart(2,'0')}-${String(eD).padStart(2,'0')} ${String(eH).padStart(2,'0')}:${String(em).padStart(2,'0')}:${String(eS).padStart(2,'0')}`;
                                recs.push({ ch, startTime, endTime, size: body.readUInt32BE(p+24), phone });
                                console.log(`[Rec] ch${ch} ${startTime}→${endTime}`);
                                p += 28;
                            }
                            deviceRecordings[phone] = recs;
                            console.log(`[Rec] Stored ${recs.length} recordings for ${phone}`);
                            // Push to all browsers immediately
                            wss.clients.forEach(c => {
                                if (c.readyState === 1) c.send(JSON.stringify({ type: 'recordings', data: recs }));
                            });
                        } catch(e) { console.error('[Rec] Parse error:', e.message); }

                    } else if (msgId === 0x1206) {
                        // Device finished uploading file to our FTP server
                        socket.write(buildAck(phone, seq, msgId));

                        // Log the FULL raw body in hex so we can see exactly what device sent
                        // console.log(`[Rec] 0x1206 raw body (${body.length} bytes):`, body.toString('hex'));
                        // console.log(`[Rec] 0x1206 raw body ascii:`, body.toString('ascii').replace(/[^\x20-\x7E]/g, '.'));

                        // T/98 §5.6.6 Table 27:
                        // byte 0-1: reply serial number (WORD)
                        // byte 2:   result (0=success, 1=failed)
                        const replySerial = body.readUInt16BE(0);
                        const result      = body[2];

                        const resultMsg = {
                            0: 'Success',
                            1: 'Failed',
                        }[result] || `Unknown(${result})`;

                        console.log(`[Rec] 0x1206 replySerial:${replySerial} result:${result} (${resultMsg})`);

                        if (result === 0) {
                                console.log(`[Rec] ✅ Device finished sending recording for ${phone}`);
                    
                                const dl = activeDownloads[phone];
                                if (dl && dl.writeStream) {
                                    dl.writeStream.end(() => {
                                        console.log(`[Rec] ✅ File saved: ${dl.filename}`);
                                        wss.clients.forEach(c => {
                                            if (c.readyState === 1) c.send(JSON.stringify({
                                                type:     'recording_ready',
                                                url:      `/recordings/${dl.filename}`,
                                                filename: dl.filename,
                                            }));
                                        });
                                        delete activeDownloads[phone];
                                    });
                                }
                            } else {
                            console.error(`[Rec] ❌ FTP upload FAILED for ${phone} result:${result}`);

                            // Common reasons:
                            // 1 = FTP connection refused (wrong IP/port, firewall blocking port 2121)
                            // 1 = FTP login failed (wrong user/pass)
                            // 1 = FTP path not found
                            // 1 = No file found for that time range on SD card

                            const errorDetail = [
                                `Result code: ${result}`,
                                `Check: Is port 2121 open on your server firewall?`,
                                `Check: Can device reach ${CONFIG.serverIp}:2121 ?`,
                                `Check: Is the time range correct for that recording?`,
                                `Check: Does the SD card have that file?`,
                            ].join(' | ');

                            console.error(`[Rec] Debug hints: ${errorDetail}`);

                            wss.clients.forEach(c => {
                                if (c.readyState === 1) c.send(JSON.stringify({
                                    type: 'error',
                                    message: `FTP upload failed (code ${result}). Check server logs for details.`,
                                }));
                            });
                        }

                    } else {
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                offset++;
            }

            buffer = buffer.slice(offset);

        } catch (err) {
            console.error('Error processing data:', err.message);
        }
    });

    socket.on('close', () => {
        console.log(`Device disconnected: ${remote} phone:${phone}`);
        if (phone) delete tcpSockets[phone];
    });
    socket.on('error', err => console.error(`Socket error: ${err.message}`));
});

tcpServer.listen(CONFIG.tcpPort, () => {
    console.log(`✓ TCP server on :${CONFIG.tcpPort}`);
});