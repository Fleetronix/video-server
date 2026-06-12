'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// ftp-service.js  —  FTP DOWNLOAD SERVICE  (multi-camera)
//
// Each camera gets:
//   • Its own PASV port from a pool (14993–15002)
//   • Its own recordings subfolder  ./recordings/<phone>/
//   • Its own download session tracked in _sessions[phone]
//
// HTTP API  :8082
//   POST /api/ftp-download  { phone, ch, startTime, endTime, folder }
//   POST /api/ftp-cancel    { phone }
//   GET  /api/sessions
//   GET  /recordings/<path>
//
// WebSocket :8802
//   { type:'status',    phone, message }
//   { type:'ftp_ready', phone, url, filename }
//   { type:'error',     phone, message }
// ─────────────────────────────────────────────────────────────────────────────

require('dotenv').config();
const net  = require('net');
const fs   = require('fs');
const path = require('path');
const http = require('http');
const { WebSocketServer } = require('ws');
const bus  = require('./device-bus');

// ── Config ────────────────────────────────────────────────────────────────────
const SERVER_IP        = process.env.SERVER_IP             || '127.0.0.1';
const FTP_PORT         = parseInt(process.env.FTP_PORT     || '14992');
const PASV_PORT_START  = parseInt(process.env.PASV_PORT    || '14993');
const PASV_POOL_SIZE   = parseInt(process.env.PASV_POOL    || '10');    // max concurrent uploads
const HTTP_PORT        = parseInt(process.env.FTP_HTTP_PORT|| '8082');
const WS_PORT          = parseInt(process.env.FTP_WS_PORT  || '8802');
const RECORDINGS_DIR   = process.env.RECORDINGS_DIR        || './recordings';

// Phone → SN mapping — add new cameras here
const PHONE_TO_SN = {
    '1576064472': '15760064472',
    '1576064474': '15760064474',
};
function framePhone(phone) {
    return PHONE_TO_SN[String(phone)] || String(phone);
}

// ── Ensure recordings folder exists ──────────────────────────────────────────
if (!fs.existsSync(RECORDINGS_DIR)) fs.mkdirSync(RECORDINGS_DIR, { recursive: true });

// ── Internal state ────────────────────────────────────────────────────────────
const _sessions = {};   // { [phone]: { ch, startTime, endTime, folder, sentAt } }
const _seqMap   = {};   // { [phone]: seq }

// ── PASV port pool ────────────────────────────────────────────────────────────
// Each slot: { server: net.Server, inUse: bool, phone: string|null }
const _pasvPool = {};

function initPasvPool() {
    for (let i = 0; i < PASV_POOL_SIZE; i++) {
        const port = PASV_PORT_START + i;
        _pasvPool[port] = {
            server:        null,
            inUse:         false,
            phone:         null,
            dataSocket:    null,
            pendingStor:   null,
        };
    }
}

function allocatePasvPort(phone) {
    for (const [portStr, slot] of Object.entries(_pasvPool)) {
        if (!slot.inUse) {
            slot.inUse = true;
            slot.phone = phone;
            return parseInt(portStr);
        }
    }
    return null;
}

function freePasvPort(port) {
    const slot = _pasvPool[port];
    if (!slot) return;
    slot.inUse       = false;
    slot.phone       = null;
    slot.dataSocket  = null;
    slot.pendingStor = null;
}

// ── Logging ───────────────────────────────────────────────────────────────────
const log  = (...a) => console.log ('[FTP-SVC]', ...a);
const warn = (...a) => console.warn('[FTP-SVC]', ...a);
const err  = (...a) => console.error('[FTP-SVC]', ...a);

// ── WebSocket ─────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: WS_PORT });
log(`WebSocket on :${WS_PORT}`);

function broadcast(obj) {
    const raw = JSON.stringify(obj);
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(raw); });
}

wss.on('connection', (ws, req) => {
    log(`Browser connected from ${req.socket.remoteAddress}`);
    // Send current sessions on connect
    ws.send(JSON.stringify({ type: 'sessions', sessions: _sessions }));

    ws.on('message', raw => {
        let msg;
        try { msg = JSON.parse(raw); } catch (e) { return; }
        if      (msg.type === 'ftp_download') triggerDownload(msg);
        else if (msg.type === 'ftp_cancel')   cancelDownload(msg.phone);
    });
});

// ── HTTP API ──────────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    const urlPath = req.url.split('?')[0];

    // POST /api/ftp-download
    if (req.method === 'POST' && urlPath === '/api/ftp-download') {
        let body = '';
        req.on('data', c => body += c);
        req.on('end', () => {
            try {
                const { phone, ch, startTime, endTime, folder } = JSON.parse(body);
                if (!phone || !ch || !startTime || !endTime) {
                    res.writeHead(400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'phone, ch, startTime, endTime are required' }));
                    return;
                }
                triggerDownload({ phone: String(phone), ch, startTime, endTime, folder });
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ status: 'queued', phone, ch, startTime, endTime }));
            } catch (e) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // POST /api/ftp-cancel
    if (req.method === 'POST' && urlPath === '/api/ftp-cancel') {
        let body = '';
        req.on('data', c => body += c);
        req.on('end', () => {
            try {
                const { phone } = JSON.parse(body);
                cancelDownload(String(phone));
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ status: 'cancelled', phone }));
            } catch (e) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // GET /api/sessions
    if (req.method === 'GET' && urlPath === '/api/sessions') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(_sessions));
        return;
    }

    // GET /recordings/**
    if (req.method === 'GET' && urlPath.startsWith('/recordings/')) {
        const rel      = urlPath.replace('/recordings/', '');
        const filePath = path.join(RECORDINGS_DIR, rel);
        fs.stat(filePath, (e, stat) => {
            if (e) { res.writeHead(404); res.end('Not found'); return; }
            res.writeHead(200, {
                'Content-Type':   'video/mp4',
                'Content-Length': stat.size,
                'Cache-Control':  'no-cache',
            });
            fs.createReadStream(filePath).pipe(res);
        });
        return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not found' }));

}).listen(HTTP_PORT, '0.0.0.0', () => log(`HTTP API on :${HTTP_PORT}`));

// ── Bus listeners ─────────────────────────────────────────────────────────────
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

    // 0x0001 — ACK for our 0x9206
    if (msgId === 0x0001) {
        const replyMsgId  = body.readUInt16BE(2);
        const replyResult = body[4];
        if (replyMsgId === 0x9206) {
            const resultText = ['Success','Failed','Wrong Msg','Not Supported'][replyResult] || `Unknown(${replyResult})`;
            log(`[${phone}] 0x9206 ack — result:${replyResult} (${resultText})`);
            if (replyResult === 0) {
                broadcast({ type: 'status', phone, message: '✅ Camera accepted, uploading via FTP...' });
            } else {
                err(`[${phone}] Camera rejected 0x9206 code:${replyResult}`);
                broadcast({ type: 'error', phone, message: `Camera rejected request (code ${replyResult})` });
                delete _sessions[phone];
            }
        }
        return;
    }

    // 0x1205 — file list response (camera confirmed files exist)
    if (msgId === 0x1205) {
        const totalFiles = body.readUInt32BE(2);
        log(`[${phone}] 0x1205 file list — total:${totalFiles}`);
        if (totalFiles === 0) {
            warn(`[${phone}] ⚠️ Camera reports 0 files — wrong time range or no SD card`);
            broadcast({ type: 'error', phone, message: '⚠️ Camera found 0 files for this time range' });
        } else {
            broadcast({ type: 'status', phone, message: `📁 Camera found ${totalFiles} file(s), sending FTP command...` });
        }
        return;
    }

    // 0x1206 — file upload complete
    if (msgId === 0x1206) {
        const result = body[2];
        log(`[${phone}] 0x1206 upload result:${result}`);

        // ACK back to camera
        bus.emit('device:send', { phone, frame: buildAck(framePhone(phone), seq, 0x1206) });

        if (result === 0) {
            log(`[${phone}] ✅ Upload complete`);
            broadcast({ type: 'status', phone, message: '📦 Camera upload complete!' });
        } else {
            err(`[${phone}] ❌ Upload failed code:${result}`);
            broadcast({ type: 'error', phone, message: `Upload failed (code ${result})` });
            delete _sessions[phone];
        }
    }
});

// ── Core logic ────────────────────────────────────────────────────────────────
function triggerDownload({ phone, ch, startTime, endTime, folder }) {
    phone = String(phone);

    // Default folder: /recordings/<phone>/
    if (!folder) folder = `/${phone}/`;

    log(`▶ triggerDownload phone:${phone} ch:${ch} ${startTime} → ${endTime} folder:${folder}`);

    // Cancel any stuck previous session
    if (_sessions[phone]) {
        bus.emit('device:send', { phone, frame: build9207(phone, 0, 2) });
        log(`[${phone}] Cancelled previous session`);
        delete _sessions[phone];
    }

    // Step 1 — query file list (0x9205)
    bus.emit('device:send', { phone, frame: build9205(phone, ch, startTime, endTime) });
    log(`[${phone}] Sent 0x9205`);
    broadcast({ type: 'status', phone, message: `🔍 Querying camera for ch${ch} recordings...` });

    // Step 2 — send FTP command (0x9206) after 3s
    setTimeout(() => {
        const frame = build9206(phone, ch, startTime, endTime, folder);
        bus.emit('device:send', { phone, frame });
        _sessions[phone] = { ch, startTime, endTime, folder, sentAt: Date.now() };
        log(`[${phone}] Sent 0x9206 folder:${folder}`);
        broadcast({ type: 'status', phone, message: `⏳ FTP command sent, waiting for camera...` });
    }, 3000);
}

function cancelDownload(phone) {
    phone = String(phone);
    if (!_sessions[phone]) {
        broadcast({ type: 'status', phone, message: 'No active download' });
        return;
    }
    bus.emit('device:send', { phone, frame: build9207(phone, 0, 2) });
    delete _sessions[phone];
    broadcast({ type: 'status', phone, message: '🛑 Download cancelled' });
    log(`[${phone}] Cancelled`);
}

// ── Frame builders ────────────────────────────────────────────────────────────
function nextSeq(phone) {
    _seqMap[phone] = ((_seqMap[phone] || 0) + 1) & 0xFFFF;
    return _seqMap[phone];
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
    const phoneStr = String(phone).padStart(12, '0');
    const header   = Buffer.alloc(12);
    header.writeUInt16BE(msgId,       0);
    header.writeUInt16BE(body.length, 2);
    Buffer.from(
        phoneStr.match(/.{2}/g).map(v => {
            const n = parseInt(v, 10);
            return ((Math.floor(n / 10) << 4) | (n % 10));
        })
    ).copy(header, 4);
    header.writeUInt16BE(nextSeq(phone), 10);

    const payload = Buffer.concat([header, body]);
    let cs = 0; payload.forEach(b => cs ^= b);

    return Buffer.concat([
        Buffer.from([0x7E]),
        escapeBuffer(Buffer.concat([payload, Buffer.from([cs])])),
        Buffer.from([0x7E]),
    ]);
}

function buildAck(phone, replySeq, replyMsgId, result = 0) {
    const body = Buffer.alloc(5);
    body.writeUInt16BE(replySeq,   0);
    body.writeUInt16BE(replyMsgId, 2);
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

// 0x9205 — query recording list
function build9205(phone, channel, startTime, endTime) {
    const fp   = framePhone(phone);
    const s    = parseDateTime(startTime, '00:00:00');
    const e    = parseDateTime(endTime,   '23:59:59');
    const body = Buffer.alloc(23);
    body[0] = channel;
    bcdBytes(s.y%100, s.mo, s.d, s.h, s.mi, s.s).copy(body, 1);
    bcdBytes(e.y%100, e.mo, e.d, e.h, e.mi, e.s).copy(body, 7);
    body.fill(0x00, 13, 21);
    body[21] = 0;
    body[22] = 0;
    return buildFrame(0x9205, body, fp);
}

// 0x9206 — file upload instruction
function build9206(phone, channel, startTime, endTime, folder = '/') {
    const fp      = framePhone(phone);
    const s       = parseDateTime(startTime, '00:00:00');
    const e       = parseDateTime(endTime,   '23:59:59');
    const ipBuf   = Buffer.from(SERVER_IP,   'ascii');
    const userBuf = Buffer.from('anonymous', 'ascii');
    const passBuf = Buffer.from('anonymous', 'ascii');
    const pathBuf = Buffer.from(folder,      'ascii');
    const k = ipBuf.length, l = userBuf.length, m = passBuf.length, n = pathBuf.length;

    const body = Buffer.alloc(1+k + 2 + 1+l + 1+m + 1+n + 1 + 6 + 6 + 8 + 4);
    let p = 0;
    body[p++] = k;                       ipBuf.copy(body, p);   p += k;
    body.writeUInt16BE(FTP_PORT, p);     p += 2;
    body[p++] = l;                       userBuf.copy(body, p); p += l;
    body[p++] = m;                       passBuf.copy(body, p); p += m;
    body[p++] = n;                       pathBuf.copy(body, p); p += n;
    body[p++] = channel;
    bcdBytes(s.y%100, s.mo, s.d, s.h, s.mi, s.s).copy(body, p); p += 6;
    bcdBytes(e.y%100, e.mo, e.d, e.h, e.mi, e.s).copy(body, p); p += 6;
    body.fill(0x00, p, p + 8); p += 8;
    body[p++] = 0;    // avType:      audio+video
    body[p++] = 1;    // streamType:  main stream
    body[p++] = 0;    // storageType: all
    body[p++] = 0x07; // taskCondition: WiFi+LAN+3G/4G

    return buildFrame(0x9206, body, fp);
}

// 0x9207 — upload control (pause/resume/cancel)
function build9207(phone, sessionId, control) {
    const fp   = framePhone(phone);
    const body = Buffer.alloc(3);
    body.writeUInt16BE(sessionId, 0);
    body[2] = control;
    return buildFrame(0x9207, body, fp);
}

// ── FTP server ────────────────────────────────────────────────────────────────
function makeFtpHandler(sessionPasvPort) {
    return ftpSock => {
        log(`FTP control from ${ftpSock.remoteAddress}:${ftpSock.remotePort}`);

        let uploadStream = null;
        let currentDir   = '/';
        let assignedPort = sessionPasvPort; // each session gets its own PASV port

        const reply = (code, msg) => ftpSock.write(`${code} ${msg}\r\n`);
        reply(220, 'FTP Server Ready');

        ftpSock.on('data', data => {
            const lines = data.toString().split('\r\n').filter(Boolean);
            lines.forEach(line => {
                const [cmd, ...args] = line.trim().split(' ');
                const arg = args.join(' ');

                switch (cmd.toUpperCase()) {

                    case 'USER': reply(331, 'Please specify the password'); break;
                    case 'PASS': reply(230, 'Logged in'); break;
                    case 'SIZE': reply(213, '0'); break;
                    case 'MDTM': reply(213, '20260101000000'); break;
                    case 'DELE': reply(250, 'Deleted'); break;
                    case 'RNFR': reply(350, 'Ready for RNTO'); break;
                    case 'RNTO': reply(250, 'Renamed'); break;
                    case 'SYST': reply(215, 'UNIX Type: L8'); break;
                    case 'TYPE': reply(200, 'Type set to I'); break;
                    case 'NOOP': reply(200, 'OK'); break;
                    case 'FEAT': ftpSock.write('211-Features:\r\n211 End\r\n'); break;
                    case 'AUTH': reply(431, 'No TLS'); break;
                    case 'EPSV': reply(502, 'Use PASV'); break;

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
                            reply(257, `"${arg}" created`);
                        } catch (e) {
                            reply(550, 'Failed to create directory');
                        }
                        break;
                    }

                    case 'PASV': {
                        // Allocate a fresh port for this FTP session
                        if (!assignedPort) {
                            const phone = _pasvPool[assignedPort]?.phone || 'unknown';
                            assignedPort = allocatePasvPort(phone);
                        }
                        if (!assignedPort) {
                            reply(421, 'No data ports available, try again later');
                            break;
                        }
                        const ip = SERVER_IP.split('.');
                        const p1 = Math.floor(assignedPort / 256);
                        const p2 = assignedPort % 256;
                        log(`PASV → ${SERVER_IP}:${assignedPort}`);
                        reply(227, `Entering Passive Mode (${ip.join(',')},${p1},${p2})`);
                        break;
                    }

                    case 'LIST':
                    case 'NLST': {
                        reply(150, 'Directory listing');
                        const slot = assignedPort ? _pasvPool[assignedPort] : null;
                        const sendList = () => {
                            const ds = slot?.dataSocket;
                            if (ds) {
                                try {
                                    const absDir  = path.join(RECORDINGS_DIR, currentDir);
                                    const months  = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                                    const entries = fs.existsSync(absDir) ? fs.readdirSync(absDir) : [];
                                    const listing = entries.map(name => {
                                        const full  = path.join(absDir, name);
                                        const stat  = fs.statSync(full);
                                        const isDir = stat.isDirectory();
                                        const d     = stat.mtime;
                                        return `${isDir?'drwxr-xr-x':'-rw-r--r--'} 1 ftp ftp ${stat.size} ${months[d.getMonth()]} ${String(d.getDate()).padStart(2,' ')} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')} ${name}`;
                                    }).join('\r\n') + '\r\n';
                                    ds.end(listing);
                                } catch (e) {
                                    ds.end('');
                                }
                                slot.dataSocket = null;
                                reply(226, 'Directory send OK');
                            } else {
                                setTimeout(sendList, 100);
                            }
                        };
                        sendList();
                        break;
                    }

                    case 'STOR': {
                        const argDir  = path.dirname(arg);
                        const saveDir = (argDir && argDir !== '.')
                            ? path.join(RECORDINGS_DIR, argDir)
                            : path.join(RECORDINGS_DIR, currentDir);

                        if (!fs.existsSync(saveDir)) {
                            fs.mkdirSync(saveDir, { recursive: true });
                        }

                        const filename   = path.basename(arg || `rec_${Date.now()}.mp4`);
                        const uploadPath = path.join(saveDir, filename);

                        // ── Ignore duplicate STOR for same file ──────────────────────────
                        if (fs.existsSync(uploadPath) && fs.statSync(uploadPath).size > 1024) {
                            log(`STOR duplicate ignored — file already exists: ${filename}`);
                            reply(125, 'Already received, skipping');
                            break;
                        }
                        uploadStream     = fs.createWriteStream(uploadPath);
                        log(`STOR → ${uploadPath}`);
                        reply(150, 'Ready to receive');

                        const slot = assignedPort ? _pasvPool[assignedPort] : null;

                        const onComplete = () => {
                            const relPath = uploadPath.replace(RECORDINGS_DIR, '').replace(/^[/\\]/, '');
                            log(`✅ Transfer complete: ${filename}`);
                            reply(226, 'Transfer complete');
                            broadcast({
                                type:     'ftp_ready',
                                phone:    slot?.phone || 'unknown',
                                url:      `/recordings/${relPath}`,
                                filename: filename,
                            });
                            if (assignedPort) freePasvPort(assignedPort);
                            assignedPort = null;
                        };

                        const handleData = (ds) => {
                            log(`Piping data → ${uploadPath}`);
                            ds.pipe(uploadStream);
                            uploadStream.on('finish', onComplete);
                            ds.on('end',   () => uploadStream.end());
                            ds.on('close', () => uploadStream.end());
                            ds.on('error', e => {
                                err('Data socket error:', e.message);
                                reply(426, 'Transfer aborted');
                                uploadStream.destroy();
                            });
                        };

                        if (slot?.dataSocket) {
                            handleData(slot.dataSocket);
                            slot.dataSocket = null;
                        } else if (slot) {
                            slot.pendingStor = handleData;
                            setTimeout(() => {
                                if (slot.pendingStor === handleData) {
                                    slot.pendingStor = null;
                                    err('No data connection after 30s');
                                    reply(425, 'No data connection');
                                    if (assignedPort) freePasvPort(assignedPort);
                                }
                            }, 30000);
                        } else {
                            reply(425, 'No PASV port allocated');
                        }
                        break;
                    }

                    case 'QUIT':
                        reply(221, 'Goodbye');
                        ftpSock.end();
                        if (assignedPort) freePasvPort(assignedPort);
                        break;

                    default:
                        reply(202, 'Command not implemented');
                }
            });
        });

        ftpSock.on('close', () => {
            log('FTP control connection closed');
            if (uploadStream) { try { uploadStream.end(); } catch (_) {} }
            if (assignedPort) freePasvPort(assignedPort);
        });
        ftpSock.on('error', e => err('FTP control socket error:', e.message));
    };
}

// ── Start FTP + PASV servers ──────────────────────────────────────────────────
function startFtpServer() {
    initPasvPool();

    // One PASV data server per port in the pool
    for (let i = 0; i < PASV_POOL_SIZE; i++) {
        const port = PASV_PORT_START + i;
        const slot = _pasvPool[port];

        const pasvServer = net.createServer(ds => {
            log(`PASV data connection on :${port} from ${ds.remoteAddress}`);
            if (slot.pendingStor) {
                slot.pendingStor(ds);
                slot.pendingStor = null;
            } else {
                slot.dataSocket = ds;
                setTimeout(() => {
                    if (slot.dataSocket === ds) {
                        ds.end();
                        slot.dataSocket = null;
                    }
                }, 30000);
            }
        });

        pasvServer.listen(port, '0.0.0.0', () => log(`✓ PASV :${port}`));
        pasvServer.on('error', e => err(`PASV :${port} error:`, e.message));
        slot.server = pasvServer;
    }

    // FTP control server
    const ftpServer = net.createServer(makeFtpHandler(null));
    ftpServer.listen(FTP_PORT, '0.0.0.0', () => log(`✓ FTP control on :${FTP_PORT}`));
    ftpServer.on('error', e => err(`FTP :${FTP_PORT} error:`, e.message));

    // Port 21 fallback
    const ftp21 = net.createServer(makeFtpHandler(null));
    ftp21.listen(21, '0.0.0.0', () => log(`✓ FTP control on :21 (fallback)`));
    ftp21.on('error', e => {
        warn(`Port 21 unavailable (${e.message})`);
        warn(`Fix: sudo iptables -t nat -A PREROUTING -p tcp --dport 21 -j REDIRECT --to-port ${FTP_PORT}`);
    });
}

// ── Boot ──────────────────────────────────────────────────────────────────────
startFtpServer();
log(`Started — FTP:${FTP_PORT} PASV:${PASV_PORT_START}-${PASV_PORT_START+PASV_POOL_SIZE-1} HTTP:${HTTP_PORT} WS:${WS_PORT}`);
log(`Recordings: ${RECORDINGS_DIR}`);
log(`PASV pool size: ${PASV_POOL_SIZE} (max ${PASV_POOL_SIZE} concurrent uploads)`);