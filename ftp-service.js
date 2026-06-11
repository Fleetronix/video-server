'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// ftp-service.js
//
// Fully standalone FTP download service.
// Does NOT import index-pictor.js — communicates only via device-bus.js.
//
// What it owns:
//   • Built-in FTP server  (receives files from camera)
//   • HTTP API             (trigger/cancel downloads, list sessions)
//   • WebSocket server     (push status updates to browser)
//   • All 0x9205 / 0x9206 / 0x9207 frame builders
//
// What index-pictor.js must do (see bottom of this file for instructions):
//   • Emit 'device:connected'    when a device authenticates
//   • Emit 'device:disconnected' when a device disconnects
//   • Emit 'device:message'      for every decoded signalling packet
//   • Listen to 'device:send'    and write the frame to the device socket
//
// HTTP API endpoints:
//   POST /api/ftp-download   { phone, ch, startTime, endTime, folder }
//   POST /api/ftp-cancel     { phone }
//   GET  /api/sessions        → list active sessions
//   GET  /recordings/:file    → download a saved recording
//
// WebSocket status messages (port WS_PORT):
//   { type: 'status',    message: '...' }
//   { type: 'ftp_ready', url: '/recordings/file.mp4', filename: 'file.mp4' }
//   { type: 'error',     message: '...' }
// ─────────────────────────────────────────────────────────────────────────────

require('dotenv').config();
const net  = require('net');
const fs   = require('fs');
const path = require('path');
const http = require('http');
const { WebSocketServer } = require('ws');
const bus  = require('./device-bus');

// ── Config ────────────────────────────────────────────────────────────────────
const SERVER_IP      = process.env.SERVER_IP        || '127.0.0.1';
const FTP_PORT       = parseInt(process.env.FTP_PORT       || '14992');
const PASV_PORT      = parseInt(process.env.PASV_PORT      || '14993');
const HTTP_PORT      = parseInt(process.env.FTP_HTTP_PORT  || '8082');
const WS_PORT        = parseInt(process.env.FTP_WS_PORT    || '8802');
const RECORDINGS_DIR = process.env.RECORDINGS_DIR          || './recordings';

// Phone → SN mapping (same as original ftp-download.js)
const PHONE_TO_SN = {
    '1576064472': '15760064472',
    '1576064474': '15760064474',
};
function framePhone(phone) {
    return PHONE_TO_SN[phone] || phone;
}

// ── Ensure recordings folder exists ──────────────────────────────────────────
if (!fs.existsSync(RECORDINGS_DIR)) fs.mkdirSync(RECORDINGS_DIR, { recursive: true });

// ── Internal state ────────────────────────────────────────────────────────────
const _sessions = {};    // { [phone]: { ch, startTime, endTime, folder, sentAt } }
const _seqMap   = {};    // { [phone]: currentSeq }  — per-device sequence counter

// FTP server state
let _pasvDataSocket = null;
let _pendingStor    = null;

// ── Logging ───────────────────────────────────────────────────────────────────
const log  = (...a) => console.log ('[FTP-SVC]', ...a);
const warn = (...a) => console.warn('[FTP-SVC]', ...a);
const err  = (...a) => console.error('[FTP-SVC]', ...a);

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket server — status updates to browser
// ─────────────────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: WS_PORT });
log(`WebSocket on :${WS_PORT}`);

function broadcast(obj) {
    const raw = JSON.stringify(obj);
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(raw); });
}

wss.on('connection', (ws, req) => {
    log(`Browser connected from ${req.socket.remoteAddress}`);

    // Also accept WS-triggered downloads (same shape as HTTP API)
    ws.on('message', raw => {
        let msg;
        try { msg = JSON.parse(raw); } catch (e) { return; }

        if (msg.type === 'ftp_download') {
            triggerDownload(msg);
        } else if (msg.type === 'ftp_cancel') {
            cancelDownload(msg.phone);
        }
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// HTTP API
// ─────────────────────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    res.setHeader('Content-Type', 'application/json');

    if (req.method === 'OPTIONS') {
        res.writeHead(204); res.end(); return;
    }

    const urlPath = req.url.split('?')[0];

    // ── POST /api/ftp-download ────────────────────────────────────────────────
    if (req.method === 'POST' && urlPath === '/api/ftp-download') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                const { phone, ch, startTime, endTime, folder } = JSON.parse(body);
                if (!phone || !ch || !startTime || !endTime) {
                    res.writeHead(400);
                    res.end(JSON.stringify({ error: 'phone, ch, startTime, endTime are required' }));
                    return;
                }
                triggerDownload({ phone, ch, startTime, endTime, folder });
                res.writeHead(200);
                res.end(JSON.stringify({ status: 'queued', phone, ch, startTime, endTime, folder: folder || '/' }));
            } catch (e) {
                res.writeHead(400);
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // ── POST /api/ftp-cancel ──────────────────────────────────────────────────
    if (req.method === 'POST' && urlPath === '/api/ftp-cancel') {
        let body = '';
        req.on('data', chunk => body += chunk);
        req.on('end', () => {
            try {
                const { phone } = JSON.parse(body);
                cancelDownload(phone);
                res.writeHead(200);
                res.end(JSON.stringify({ status: 'cancelled', phone }));
            } catch (e) {
                res.writeHead(400);
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // ── GET /api/sessions ─────────────────────────────────────────────────────
    if (req.method === 'GET' && urlPath === '/api/sessions') {
        res.writeHead(200);
        res.end(JSON.stringify(_sessions));
        return;
    }

    // ── GET /recordings/:filename ─────────────────────────────────────────────
    if (req.method === 'GET' && urlPath.startsWith('/recordings/')) {
        const filename = path.basename(urlPath);
        const filePath = path.join(RECORDINGS_DIR, filename);
        fs.stat(filePath, (statErr, stat) => {
            if (statErr) { res.writeHead(404); res.end(JSON.stringify({ error: 'Not found' })); return; }
            res.writeHead(200, {
                'Content-Type':   'video/mp4',
                'Content-Length': stat.size,
                'Cache-Control':  'no-cache',
            });
            fs.createReadStream(filePath).pipe(res);
        });
        return;
    }

    res.writeHead(404);
    res.end(JSON.stringify({ error: 'Not found' }));

}).listen(HTTP_PORT, () => log(`HTTP API on :${HTTP_PORT}`));

// ─────────────────────────────────────────────────────────────────────────────
// Device bus listeners
// ─────────────────────────────────────────────────────────────────────────────

bus.on('device:connected', ({ phone }) => {
    log(`Device connected: ${phone}`);
    _seqMap[phone] = 0;
});

bus.on('device:disconnected', ({ phone }) => {
    log(`Device disconnected: ${phone}`);
    delete _sessions[phone];
    delete _seqMap[phone];
});

bus.on('device:message', ({ msgId, body, seq, phone }) => {

    // ── 0x0001: Device ACK for our 0x9206 ────────────────────────────────────
    if (msgId === 0x0001) {
        const replyMsgId  = body.readUInt16BE(2);
        const replyResult = body[4];
        if (replyMsgId === 0x9206) {
            const resultText = ['Success','Failed','Wrong Msg','Not Supported'][replyResult] || `Unknown(${replyResult})`;
            log(`0x0001 ack for 0x9206 — result:${replyResult} (${resultText})`);
            if (replyResult === 0) {
                broadcast({ type: 'status', message: '✅ Camera accepted request, uploading via FTP...' });
            } else {
                err(`Camera rejected 0x9206 code:${replyResult}`);
                broadcast({ type: 'error', message: `Camera rejected download request (code ${replyResult})` });
                delete _sessions[phone];
            }
        }
        return;
    }

    // ── 0x1206: File upload completion notification ───────────────────────────
    if (msgId === 0x1206) {
        const replySerial = body.readUInt16BE(0);
        const result      = body[2];
        log(`0x1206 — phone:${phone} replySerial:${replySerial} result:${result}`);

        // ACK the device
        const fp = framePhone(phone);
        bus.emit('device:send', { phone, frame: buildAck(fp, seq, 0x1206) });

        if (result === 0) {
            log(`✅ Camera finished uploading for ${phone}`);
            broadcast({ type: 'status', message: '📦 Camera upload complete, saving file...' });
        } else {
            const hints = [
                `Result code: ${result}`,
                `Is port ${FTP_PORT} open on the server firewall?`,
                `Can camera reach ${SERVER_IP}:${FTP_PORT}?`,
                `Does the SD card have that recording?`,
            ].join(' | ');
            err(`❌ 0x1206 FAILED for ${phone} — ${hints}`);
            broadcast({ type: 'error', message: `Camera upload failed (code ${result}). ${hints}` });
            delete _sessions[phone];
        }
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// Core download logic
// ─────────────────────────────────────────────────────────────────────────────

function triggerDownload({ phone, ch, startTime, endTime, folder = '/' }) {
    phone = String(phone);
    if (!phone) {
        broadcast({ type: 'error', message: 'No phone specified' });
        return;
    }

    log(`▶ triggerDownload phone:${phone} ch:${ch} ${startTime} → ${endTime} folder:${folder}`);

    // Cancel any stuck previous session
    if (_sessions[phone]) {
        bus.emit('device:send', { phone, frame: build9207(phone, 0, 2) });
        log(`Cancelled previous session for ${phone}`);
        delete _sessions[phone];
    }

    // Step 1 — query file list (0x9205) so camera verifies recording exists
    bus.emit('device:send', { phone, frame: build9205(phone, ch, startTime, endTime) });
    log(`Sent 0x9205 to ${phone}`);

    // Step 2 — after 3s send FTP upload command (0x9206)
    setTimeout(() => {
        const frame = build9206(phone, ch, startTime, endTime, folder);
        bus.emit('device:send', { phone, frame });
        _sessions[phone] = { ch, startTime, endTime, folder, sentAt: Date.now() };
        log(`Sent 0x9206 to ${phone} folder:${folder}`);
        broadcast({ type: 'status', message: `⏳ Requested ch${ch} ${startTime} → ${endTime}. Waiting for camera...` });
    }, 3000);
}

function cancelDownload(phone) {
    phone = String(phone);
    if (!_sessions[phone]) {
        broadcast({ type: 'status', message: 'No active download to cancel' });
        return;
    }
    bus.emit('device:send', { phone, frame: build9207(phone, 0, 2) });
    delete _sessions[phone];
    broadcast({ type: 'status', message: '🛑 Download cancelled' });
    log(`Cancelled download for ${phone}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// JT/T 808 frame builders  (fully self-contained — no dependency on index-pictor)
// ─────────────────────────────────────────────────────────────────────────────

function nextSeq(phone) {
    _seqMap[phone] = ((_seqMap[phone] || 0) + 1) & 0xFFFF;
    return _seqMap[phone];
}

function escapeBuffer(buf) {
    const out = [];
    for (const b of buf) {
        if      (b === 0x7E) { out.push(0x7D, 0x02); }
        else if (b === 0x7D) { out.push(0x7D, 0x01); }
        else                 { out.push(b); }
    }
    return Buffer.from(out);
}

/**
 * buildFrame — builds a complete JT/T 808 framed packet.
 *
 * phone must already be the "frame phone" (SN) string when calling
 * for commands that need the SN (0x9205, 0x9206, 0x9207).
 * For ACKs (0x8001) pass the framePhone too.
 */
function buildFrame(msgId, body, phone) {
    const phoneStr = String(phone).padStart(12, '0');
    const header   = Buffer.alloc(12);
    header.writeUInt16BE(msgId,       0);
    header.writeUInt16BE(body.length, 2);

    // BCD-encode phone digits into 6 bytes
    Buffer.from(
        phoneStr.match(/.{2}/g).map(v => {
            const n = parseInt(v, 10);
            return ((Math.floor(n / 10) << 4) | (n % 10));
        })
    ).copy(header, 4);

    header.writeUInt16BE(nextSeq(phone), 10);

    const payload = Buffer.concat([header, body]);
    let cs = 0;
    payload.forEach(b => cs ^= b);

    return Buffer.concat([
        Buffer.from([0x7E]),
        escapeBuffer(Buffer.concat([payload, Buffer.from([cs])])),
        Buffer.from([0x7E]),
    ]);
}

function buildAck(phone, replySeq, replyMsgId, result = 0) {
    const body = Buffer.alloc(5);
    body.writeUInt16BE(replySeq,    0);
    body.writeUInt16BE(replyMsgId,  2);
    body[4] = result;
    return buildFrame(0x8001, body, phone);
}

function bcdBytes(yy, mo, dd, hh, mm, ss) {
    const enc = n => ((Math.floor(n / 10) << 4) | (n % 10));
    return Buffer.from([enc(yy), enc(mo), enc(dd), enc(hh), enc(mm), enc(ss)]);
}

function parseDateTime(dtStr, fallback) {
    const [date, time = fallback] = dtStr.split(' ');
    const [y, mo, d] = date.split('-').map(Number);
    const [h, mi, s] = time.split(':').map(Number);
    return { y, mo, d, h, mi, s };
}

// T/98 §5.6.1 Table 21 — query recording list
function build9205(phone, channel, startTime, endTime) {
    const fp = framePhone(phone);
    const s  = parseDateTime(startTime, '00:00:00');
    const e  = parseDateTime(endTime,   '23:59:59');
    const body = Buffer.alloc(23);
    body[0] = channel;
    bcdBytes(s.y%100, s.mo, s.d, s.h, s.mi, s.s).copy(body, 1);
    bcdBytes(e.y%100, e.mo, e.d, e.h, e.mi, e.s).copy(body, 7);
    body.fill(0x00, 13, 21);   // alarm logo — no filter
    body[21] = 0;               // avType: audio+video
    body[22] = 0;               // streamType: all
    log(`0x9205 query — ch:${channel} ${startTime} → ${endTime}`);
    return buildFrame(0x9205, body, fp);
}

// T/98 §5.6.5 Table 26 — file upload instruction
function build9206(phone, channel, startTime, endTime, folder = '/') {
    const fp      = framePhone(phone);
    const s       = parseDateTime(startTime, '00:00:00');
    const e       = parseDateTime(endTime,   '23:59:59');
    const ipBuf   = Buffer.from(SERVER_IP, 'ascii');
    const userBuf = Buffer.from('anonymous', 'ascii');
    const passBuf = Buffer.from('anonymous', 'ascii');
    const pathBuf = Buffer.from(folder,      'ascii');
    const k = ipBuf.length, l = userBuf.length, m = passBuf.length, n = pathBuf.length;

    const body = Buffer.alloc(1+k + 2 + 1+l + 1+m + 1+n + 1 + 6 + 6 + 8 + 4);
    let p = 0;

    body[p++] = k;                        ipBuf.copy(body, p);   p += k;
    body.writeUInt16BE(FTP_PORT, p);      p += 2;
    body[p++] = l;                        userBuf.copy(body, p); p += l;
    body[p++] = m;                        passBuf.copy(body, p); p += m;
    body[p++] = n;                        pathBuf.copy(body, p); p += n;
    body[p++] = channel;
    bcdBytes(s.y%100, s.mo, s.d, s.h, s.mi, s.s).copy(body, p); p += 6;
    bcdBytes(e.y%100, e.mo, e.d, e.h, e.mi, e.s).copy(body, p); p += 6;
    body.fill(0x00, p, p + 8); p += 8;   // alarm logo — no filter
    body[p++] = 0;    // avType:      audio+video
    body[p++] = 1;    // streamType:  main stream
    body[p++] = 0;    // storageType: all
    body[p++] = 0x07; // taskCondition: WiFi + LAN + 3G/4G

    log(`0x9206 → phone:${phone} ch:${channel} folder:${folder} ${startTime} → ${endTime}`);
    return buildFrame(0x9206, body, fp);
}

// T/98 §5.6.7 Table 28 — file upload control (pause/resume/cancel)
function build9207(phone, sessionId, control) {
    const fp   = framePhone(phone);
    const body = Buffer.alloc(3);
    body.writeUInt16BE(sessionId, 0);
    body[2] = control;   // 0=pause  1=resume  2=cancel
    return buildFrame(0x9207, body, fp);
}

// ─────────────────────────────────────────────────────────────────────────────
// FTP server  (ported from original ftp-download.js — no changes to logic)
// ─────────────────────────────────────────────────────────────────────────────

function makeFtpHandler() {
    return ftpSock => {
        log(`FTP control connection from ${ftpSock.remoteAddress}:${ftpSock.remotePort}`);

        let dataSocket   = null;
        let uploadStream = null;
        let uploadPath   = null;
        let currentDir   = '/';

        const reply = (code, msg) => {
            log(`→ FTP ${code} ${msg}`);
            ftpSock.write(`${code} ${msg}\r\n`);
        };

        reply(220, 'FTP Server Ready');

        ftpSock.on('data', data => {
            const lines = data.toString().split('\r\n').filter(Boolean);
            lines.forEach(line => {
                log(`← FTP cmd: ${line.trim()}`);
                const [cmd, ...args] = line.trim().split(' ');
                const arg = args.join(' ');

                switch (cmd.toUpperCase()) {

                    case 'USER':
                        reply(331, 'Please specify the password');
                        break;

                    case 'PASS':
                        reply(230, 'User logged in, proceed');
                        break;

                    case 'SIZE':
                        reply(213, '0');
                        break;

                    case 'MDTM':
                        reply(213, '20260101000000');
                        break;

                    case 'DELE':
                        reply(250, 'Delete operation successful');
                        break;

                    case 'RNFR':
                        reply(350, 'Ready for RNTO');
                        break;

                    case 'RNTO':
                        reply(250, 'Rename successful');
                        break;

                    case 'SYST':
                        reply(215, 'UNIX Type: L8');
                        break;

                    case 'TYPE':
                        reply(200, 'Type set to I');
                        break;

                    case 'PWD':
                    case 'XPWD':
                        reply(257, `"${currentDir}" is current directory`);
                        break;

                    case 'CWD':
                        currentDir = arg.startsWith('/') ? arg : path.join(currentDir, arg);
                        reply(250, `Directory changed to ${currentDir}`);
                        break;

                    case 'MKD': {
                        const dirPath = path.join(RECORDINGS_DIR, arg.replace(/^\//, ''));
                        try {
                            fs.mkdirSync(dirPath, { recursive: true });
                            log(`MKD created: ${dirPath}`);
                            reply(257, `"/${arg}" created`);
                        } catch (e) {
                            warn(`MKD failed: ${e.message}`);
                            reply(550, 'Failed to create directory');
                        }
                        break;
                    }

                    case 'EPSV':
                        // Refuse EPSV so camera falls back to PASV
                        reply(502, 'EPSV not supported, use PASV');
                        break;

                    case 'PASV': {
                        _pasvDataSocket = null;
                        dataSocket      = null;
                        const ip = SERVER_IP.split('.');
                        const p1 = Math.floor(PASV_PORT / 256);
                        const p2 = PASV_PORT % 256;
                        log(`PASV → ${SERVER_IP}:${PASV_PORT}`);
                        reply(227, `Entering Passive Mode (${ip.join(',')},${p1},${p2})`);
                        break;
                    }

                    case 'LIST':
                    case 'NLST': {
                        reply(150, 'Here comes the directory listing');
                        const sendListing = () => {
                            if (_pasvDataSocket) {
                                try {
                                    const absDir  = path.join(RECORDINGS_DIR, currentDir);
                                    const entries = fs.readdirSync(absDir);
                                    const months  = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                                    const listing = entries.map(name => {
                                        const full   = path.join(absDir, name);
                                        const stat   = fs.statSync(full);
                                        const isDir  = stat.isDirectory();
                                        const d      = stat.mtime;
                                        const dateStr = `${months[d.getMonth()]} ${String(d.getDate()).padStart(2,' ')} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
                                        return `${isDir ? 'drwxr-xr-x' : '-rw-r--r--'} 1 ftp ftp ${stat.size} ${dateStr} ${name}`;
                                    }).join('\r\n') + '\r\n';
                                    _pasvDataSocket.end(listing);
                                } catch (e) {
                                    warn(`LIST read error: ${e.message}`);
                                    _pasvDataSocket.end('');
                                }
                                _pasvDataSocket = null;
                                reply(226, 'Directory send OK');
                            } else {
                                setTimeout(sendListing, 100);
                            }
                        };
                        sendListing();
                        break;
                    }

                    case 'STOR': {
                        // Determine save folder from current FTP working dir or from arg path
                        const argDir  = path.dirname(arg);
                        const saveDir = (argDir && argDir !== '.') 
                            ? path.join(RECORDINGS_DIR, argDir)
                            : path.join(RECORDINGS_DIR, currentDir);

                        // Create folder if it doesn't exist
                        if (!fs.existsSync(saveDir)) {
                            fs.mkdirSync(saveDir, { recursive: true });
                            log(`STOR created folder: ${saveDir}`);
                        }

                        const filename = path.basename(arg || `rec_${Date.now()}.mp4`);
                        uploadPath     = path.join(saveDir, filename);
                        uploadStream   = fs.createWriteStream(uploadPath);
                        log(`STOR → writing to ${uploadPath}`);
                        reply(150, 'Ok to send data');

                        const onComplete = () => {
                            const fname    = path.basename(uploadPath);
                            const relPath  = uploadPath.replace(RECORDINGS_DIR, '').replace(/^\//, '');
                            log(`✅ File transfer complete: ${fname}`);
                            reply(226, 'Transfer complete');
                            broadcast({ type: 'ftp_ready', url: `/recordings/${relPath}`, filename: fname });
                            _pasvDataSocket = null;
                        };

                        const onError = (e) => {
                            err('Data socket error:', e?.message || e);
                            reply(426, 'Connection closed, transfer aborted');
                            try { uploadStream.destroy(); } catch (_) {}
                        };

                        const handleDataSocket = (s) => {
                            log(`Piping data → ${uploadPath}`);
                            dataSocket = s;
                            s.pipe(uploadStream);
                            uploadStream.on('finish', onComplete);
                            s.on('end',   () => { uploadStream.end(); });
                            s.on('close', () => { uploadStream.end(); });
                            s.on('error', onError);
                        };

                        if (_pasvDataSocket) {
                            handleDataSocket(_pasvDataSocket);
                            _pasvDataSocket = null;
                        } else {
                            _pendingStor = handleDataSocket;
                            setTimeout(() => {
                                if (_pendingStor === handleDataSocket) {
                                    _pendingStor = null;
                                    err('No data connection after 30s');
                                    reply(425, 'No data connection established');
                                }
                            }, 30000);
                        }
                        break;
                    }

                    case 'QUIT':
                        reply(221, 'Goodbye');
                        ftpSock.end();
                        break;

                    case 'NOOP':
                        reply(200, 'OK');
                        break;

                    case 'AUTH':
                    case 'AUTH TLS':
                    case 'AUTH SSL':
                        reply(431, 'No TLS support');
                        break;

                    case 'FEAT':
                        ftpSock.write('211-Features:\r\n211 End\r\n');
                        break;

                    default:
                        warn(`Unhandled FTP cmd: ${cmd}`);
                        reply(202, 'Command not implemented');
                }
            });
        });

        ftpSock.on('close', () => {
            log('FTP control connection closed');
            if (uploadStream) { try { uploadStream.end(); } catch (_) {} }
        });
        ftpSock.on('error', e => err('FTP control socket error:', e.message));
    };
}

function startFtpServer() {
    // ── Permanent PASV data server ────────────────────────────────────────────
    const pasvServer = net.createServer(s => {
        log(`PASV data connection from ${s.remoteAddress}:${s.remotePort}`);
        if (_pendingStor) {
            _pendingStor(s);
            _pendingStor = null;
        } else {
            _pasvDataSocket = s;
            // Auto-close if nothing uses it within 30s
            setTimeout(() => {
                if (_pasvDataSocket === s) { s.end(); _pasvDataSocket = null; }
            }, 30000);
        }
    });
    pasvServer.listen(PASV_PORT, '0.0.0.0', () => log(`✓ PASV data server on :${PASV_PORT}`));
    pasvServer.on('error', e => err('PASV server error:', e.message));

    // ── FTP control server on configured port ─────────────────────────────────
    const ftpServer = net.createServer(makeFtpHandler());
    ftpServer.listen(FTP_PORT, '0.0.0.0', () => log(`✓ FTP control server on :${FTP_PORT}`));
    ftpServer.on('error', e => err(`FTP server :${FTP_PORT} error:`, e.message));

    // ── Port 21 fallback — many cameras ignore the port in 0x9206 ────────────
    // To enable: sudo setcap cap_net_bind_service=+ep $(which node)
    // Or:        sudo iptables -t nat -A PREROUTING -p tcp --dport 21 -j REDIRECT --to-port 14992
    const ftp21 = net.createServer(makeFtpHandler());
    ftp21.listen(21, '0.0.0.0', () => log(`✓ FTP control server on :21 (fallback)`));
    ftp21.on('error', e => {
        warn(`Port 21 unavailable (${e.message})`);
        warn(`Fix: sudo iptables -t nat -A PREROUTING -p tcp --dport 21 -j REDIRECT --to-port ${FTP_PORT}`);
    });
}

// ── Boot ──────────────────────────────────────────────────────────────────────
startFtpServer();
log(`Started — FTP:${FTP_PORT} PASV:${PASV_PORT} HTTP:${HTTP_PORT} WS:${WS_PORT}`);
log(`Recordings dir: ${RECORDINGS_DIR}`);