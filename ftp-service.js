'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// ftp-service.js  —  FTP DOWNLOAD SERVICE  (multi-camera + Redis tracking)
//
// Every download request gets a unique requestId.
// Status is tracked in Azure Redis under key:  ftp:<phone>:<requestId>
//
// Redis record shape:
// {
//   requestId:  'abc123',
//   phone:      '15760064474',
//   ch:         1,
//   startTime:  '2026-06-15 14:38:00',
//   endTime:    '2026-06-15 14:38:20',
//   folder:     '/15760064474/',
//   status:     'queued' | 'in_progress' | 'complete' | 'failed',
//   filePath:   '/full/path/to/file.mp4',   // set when complete
//   filename:   'CH0-....MP4',              // set when complete
//   url:        '/recordings/...',          // set when complete
//   fileSize:   1234567,                    // set when complete
//   createdAt:  '2026-06-15T14:38:00.000Z',
//   updatedAt:  '2026-06-15T14:38:30.000Z',
//   error:      'reason',                  // set when failed
// }
//
// HTTP API  :8082
//   POST /api/ftp-download   { phone, ch, startTime, endTime, folder }
//        → { requestId, status:'queued', phone, ch, startTime, endTime, folder }
//
//   GET  /api/ftp-status/:requestId          → Redis record for that request
//   GET  /api/ftp-history/:phone             → all records for a phone (latest 50)
//   POST /api/ftp-cancel     { phone }
//   GET  /api/sessions                       → active in-memory sessions
//   GET  /recordings/**                      → download saved file
// ─────────────────────────────────────────────────────────────────────────────

require('dotenv').config();
const net    = require('net');
const fs     = require('fs');
const path   = require('path');
const http   = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const Redis  = require('ioredis');
const bus    = require('./device-bus');

// ── Config ────────────────────────────────────────────────────────────────────
const SERVER_IP       = process.env.SERVER_IP             || '127.0.0.1';
const FTP_PORT        = parseInt(process.env.FTP_PORT     || '14992');
const PASV_PORT_START = parseInt(process.env.PASV_PORT    || '14993');
const PASV_POOL_SIZE  = parseInt(process.env.PASV_POOL    || '10');
const HTTP_PORT       = parseInt(process.env.FTP_HTTP_PORT|| '8082');
const WS_PORT         = parseInt(process.env.FTP_WS_PORT  || '8802');
const RECORDINGS_DIR  = process.env.RECORDINGS_DIR        || './recordings';
const REDIS_TTL       = parseInt(process.env.REDIS_TTL    || String(7 * 24 * 3600)); // 7 days

// Phone → SN mapping
const PHONE_TO_SN = {
    '1576064472': '15760064472',
    '1576064474': '15760064474',
};
function framePhone(phone) {
    return PHONE_TO_SN[String(phone)] || String(phone);
}

if (!fs.existsSync(RECORDINGS_DIR)) fs.mkdirSync(RECORDINGS_DIR, { recursive: true });

// ── Logging ───────────────────────────────────────────────────────────────────
const log  = (...a) => console.log ('[FTP-SVC]', ...a);
const warn = (...a) => console.warn('[FTP-SVC]', ...a);
const err  = (...a) => console.error('[FTP-SVC]', ...a);

// ── Redis ─────────────────────────────────────────────────────────────────────
let redis = null;

function connectRedis() {
    const opts = {
        host:            process.env.REDIS_HOST,
        port:            parseInt(process.env.REDIS_PORT || '6380'),
        password:        process.env.REDIS_PASSWORD,
        tls:             process.env.REDIS_TLS === 'false' ? undefined : {},  // Azure Redis uses TLS
        retryStrategy:   (times) => Math.min(times * 500, 5000),
        lazyConnect:     true,
        enableReadyCheck: true,
    };

    if (!opts.host) {
        warn('REDIS_HOST not set — Redis tracking disabled. Set in .env to enable.');
        return null;
    }

    const client = new Redis(opts);

    client.on('connect',  () => log('✅ Redis connected'));
    client.on('ready',    () => log('✅ Redis ready'));
    client.on('error',    e  => err('Redis error:', e.message));
    client.on('close',    () => warn('Redis connection closed'));
    client.on('reconnecting', () => warn('Redis reconnecting...'));

    client.connect().catch(e => err('Redis initial connect error:', e.message));
    return client;
}

redis = connectRedis();

// ── Redis helpers ─────────────────────────────────────────────────────────────
//
// Structure:
//
// HASH  videoRecInfo            ← current operation per phone
//   field: <phone>               value: JSON string
//
// LIST  ftp:history:<phone>     ← full history per phone (newest first, max 50)
//   each item: JSON string

const CURRENT_HASH = 'videoRecInfo';

function historyKey(phone) {
    return `ftp:history:${phone}`;
}

// Save new record → HSET videoRecInfo <phone> <json>  +  LPUSH history
async function saveToRedis(record) {
    if (!redis) return;
    try {
        record.updatedAt = new Date().toISOString();
        const json = JSON.stringify(record);

        // 1. Current — HSET videoRecInfo <phone> <json>
        await redis.hset(CURRENT_HASH, record.phone, json);

        // 2. History — LPUSH ftp:history:<phone> <json>  (cap at 50)
        const hKey = historyKey(record.phone);
        await redis.lpush(hKey, json);
        await redis.ltrim(hKey, 0, 49);
        await redis.expire(hKey, REDIS_TTL);

        log(`Redis HSET ${CURRENT_HASH}[${record.phone}] requestId:${record.requestId} status:${record.status}`);
    } catch (e) {
        err('Redis save error:', e.message);
    }
}

// Update current record — HGET → merge → HSET  +  update matching history entry
async function updateRedis(phone, requestId, patch) {
    if (!redis) return;
    try {
        const now = new Date().toISOString();

        // 1. Update current hash
        const existing = await redis.hget(CURRENT_HASH, phone);
        if (!existing) { warn(`Redis HGET ${CURRENT_HASH}[${phone}] — not found`); return; }
        const current = { ...JSON.parse(existing), ...patch, updatedAt: now };
        await redis.hset(CURRENT_HASH, phone, JSON.stringify(current));

        // 2. Update matching history entry
        const hKey = historyKey(phone);
        const items = await redis.lrange(hKey, 0, 49);
        for (let i = 0; i < items.length; i++) {
            const item = JSON.parse(items[i]);
            if (item.requestId === requestId) {
                await redis.lset(hKey, i, JSON.stringify({ ...item, ...patch, updatedAt: now }));
                break;
            }
        }

        log(`Redis HSET ${CURRENT_HASH}[${phone}] requestId:${requestId} status:${patch.status || current.status}`);
    } catch (e) {
        err('Redis update error:', e.message);
    }
}

// HGET videoRecInfo <phone>
async function getCurrentFromRedis(phone) {
    if (!redis) return null;
    try {
        const raw = await redis.hget(CURRENT_HASH, phone);
        return raw ? JSON.parse(raw) : null;
    } catch (e) {
        err('Redis hget error:', e.message);
        return null;
    }
}

// HGETALL videoRecInfo  → all phones
async function getAllCurrentFromRedis() {
    if (!redis) return {};
    try {
        const all = await redis.hgetall(CURRENT_HASH);
        if (!all) return {};
        const result = {};
        for (const [phone, raw] of Object.entries(all)) {
            try { result[phone] = JSON.parse(raw); } catch (_) {}
        }
        return result;
    } catch (e) {
        err('Redis hgetall error:', e.message);
        return {};
    }
}

// Find by requestId — check current hash first, then scan history
async function getByRequestId(requestId) {
    if (!redis) return null;
    try {
        // Check current hash first (fast)
        const all = await redis.hgetall(CURRENT_HASH);
        if (all) {
            for (const raw of Object.values(all)) {
                const rec = JSON.parse(raw);
                if (rec.requestId === requestId) return rec;
            }
        }
        // Scan history lists
        const historyKeys = await redis.keys('ftp:history:*');
        for (const hKey of historyKeys) {
            const items = await redis.lrange(hKey, 0, 49);
            for (const raw of items) {
                const rec = JSON.parse(raw);
                if (rec.requestId === requestId) return rec;
            }
        }
        return null;
    } catch (e) {
        err('Redis getByRequestId error:', e.message);
        return null;
    }
}

// LRANGE ftp:history:<phone> 0 49
async function getHistoryFromRedis(phone) {
    if (!redis) return [];
    try {
        const items = await redis.lrange(historyKey(phone), 0, 49);
        return items.map(raw => { try { return JSON.parse(raw); } catch (_) { return null; } }).filter(Boolean);
    } catch (e) {
        err('Redis history error:', e.message);
        return [];
    }
}

// ── Internal state ────────────────────────────────────────────────────────────
// _sessions[phone] = { requestId, ch, startTime, endTime, folder, sentAt }
const _sessions = {};
const _seqMap   = {};

// PASV pool
const _pasvPool = {};

function initPasvPool() {
    for (let i = 0; i < PASV_POOL_SIZE; i++) {
        const port = PASV_PORT_START + i;
        _pasvPool[port] = { inUse: false, phone: null, dataSocket: null, pendingStor: null, server: null };
    }
}

function allocatePasvPort() {
    for (const [portStr, slot] of Object.entries(_pasvPool)) {
        if (!slot.inUse) {
            slot.inUse = true;
            return parseInt(portStr);
        }
    }
    return null;
}

function freePasvPort(port) {
    const slot = _pasvPool[port];
    if (!slot) return;
    if (!slot.dataSocket && !slot.pendingStor) {
        slot.inUse = false; slot.phone = null;
        slot.dataSocket = null; slot.pendingStor = null;
        log(`PASV port ${port} freed`);
    }
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: WS_PORT });
log(`WebSocket on :${WS_PORT}`);

function broadcast(obj) {
    const raw = JSON.stringify(obj);
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(raw); });
}

wss.on('connection', (ws, req) => {
    log(`Browser connected from ${req.socket.remoteAddress}`);
    ws.send(JSON.stringify({ type: 'sessions', sessions: _sessions }));
    ws.on('message', raw => {
        let msg; try { msg = JSON.parse(raw); } catch (e) { return; }
        if      (msg.type === 'ftp_download') triggerDownload(msg).catch(e => err(e.message));
        else if (msg.type === 'ftp_cancel')   cancelDownload(msg.phone);
    });
});

// ── HTTP API ──────────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

    const urlPath = req.url.split('?')[0];

    // ── POST /api/ftp-download ────────────────────────────────────────────────
    if (req.method === 'POST' && urlPath === '/api/ftp-download') {
        let body = '';
        req.on('data', c => body += c);
        req.on('end', async () => {
            try {
                console.log("[FTP-SVC] /api/ftp-download body:", body);
                const { phone, ch, startTime, endTime, folder } = JSON.parse(body);
                if (!phone || !ch || !startTime || !endTime) {
                    res.writeHead(400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'phone, ch, startTime, endTime are required' }));
                    return;
                }
                const result = await triggerDownload({
                    phone: String(phone), ch, startTime, endTime, folder,
                });
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(result));
            } catch (e) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // ── GET /api/ftp-status/:requestId ────────────────────────────────────────
    if (req.method === 'GET' && urlPath.startsWith('/api/ftp-status/')) {
        const requestId = urlPath.replace('/api/ftp-status/', '').trim();
        (async () => {
            if (!redis) {
                res.writeHead(503, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Redis not configured' }));
                return;
            }
            const record = await getByRequestId(requestId);
            if (!record) {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: `Request ${requestId} not found` }));
                return;
            }
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(record));
        })();
        return;
    }

    // ── GET /api/ftp-current              → all phones current status ─────────
    // ── GET /api/ftp-current/:phone       → one phone current status ──────────
    if (req.method === 'GET' && urlPath.startsWith('/api/ftp-current')) {
        const phone = urlPath.replace('/api/ftp-current', '').replace(/^\//, '').trim();
        (async () => {
            if (!redis) {
                res.writeHead(503, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'Redis not configured' }));
                return;
            }
            if (phone) {
                // Single phone
                const record = await getCurrentFromRedis(phone);
                res.writeHead(record ? 200 : 404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(record || { error: `No current record for phone ${phone}` }));
            } else {
                // All phones
                const all = await getAllCurrentFromRedis();
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(all));
            }
        })();
        return;
    }

    // ── GET /api/ftp-history/:phone ───────────────────────────────────────────
    if (req.method === 'GET' && urlPath.startsWith('/api/ftp-history/')) {
        const phone = urlPath.replace('/api/ftp-history/', '').trim();
        (async () => {
            const history = await getHistoryFromRedis(phone);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(history));
        })();
        return;
    }

    // ── POST /api/ftp-cancel ──────────────────────────────────────────────────
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

    // ── GET /api/sessions ─────────────────────────────────────────────────────
    if (req.method === 'GET' && urlPath === '/api/sessions') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(_sessions));
        return;
    }

    // ── GET /recordings/** ────────────────────────────────────────────────────
    if (req.method === 'GET' && urlPath.startsWith('/recordings/')) {
        const rel      = urlPath.replace('/recordings/', '');
        const filePath = path.join(RECORDINGS_DIR, rel);
        fs.stat(filePath, (e, stat) => {
            if (e) { res.writeHead(404); res.end('Not found'); return; }
            res.writeHead(200, {
                'Content-Type':        'video/mp4',
                'Content-Length':      stat.size,
                'Content-Disposition': `attachment; filename="${path.basename(filePath)}"`,
                'Cache-Control':       'no-cache',
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
    // Mark any in-progress session as failed
    if (_sessions[phone]) {
        updateRedis(phone, _sessions[phone].requestId, { status: 'failed', error: 'Device disconnected' });
        delete _sessions[phone];
    }
    delete _seqMap[phone];
});

bus.on('device:message', ({ msgId, body, seq, phone }) => {

    // 0x0001 — ACK for our 0x9206
    if (msgId === 0x0001) {
        const replyMsgId  = body.readUInt16BE(2);
        const replyResult = body[4];
        if (replyMsgId === 0x9206) {
            const session = _sessions[phone];
            log(`[${phone}] 0x9206 ack — result:${replyResult}`);
            if (replyResult === 0) {
                broadcast({ type: 'status', phone, message: '✅ Camera accepted, uploading via FTP...' });
                if (session) {
                    updateRedis(phone, session.requestId, { status: 'in_progress' });
                }
            } else {
                err(`[${phone}] Camera rejected 0x9206 code:${replyResult}`);
                broadcast({ type: 'error', phone, message: `Camera rejected request (code ${replyResult})` });
                if (session) {
                    updateRedis(phone, session.requestId, {
                        status: 'failed',
                        error: `Camera rejected 0x9206 (code ${replyResult})`,
                    });
                    delete _sessions[phone];
                }
            }
        }
        return;
    }

    // 0x1205 — file list response
    if (msgId === 0x1205) {
        const totalFiles = body.readUInt32BE(2);
        log(`[${phone}] 0x1205 file list — total:${totalFiles}`);
        if (totalFiles === 0) {
            warn(`[${phone}] ⚠️ Camera reports 0 files`);
            broadcast({ type: 'error', phone, message: '⚠️ Camera found 0 files for this time range' });
            const session = _sessions[phone];
            if (session) {
                updateRedis(phone, session.requestId, { status: 'failed', error: '0 files found on camera' });
                delete _sessions[phone];
            }
        } else {
            broadcast({ type: 'status', phone, message: `📁 Camera found ${totalFiles} file(s)` });
        }
        return;
    }

    // 0x1206 — file upload complete
    if (msgId === 0x1206) {
        const result  = body[2];
        const session = _sessions[phone];
        log(`[${phone}] 0x1206 upload result:${result}`);

        // ACK back to camera
        bus.emit('device:send', { phone, frame: buildAck(framePhone(phone), seq, 0x1206) });

        if (result !== 0) {
            err(`[${phone}] ❌ Upload failed code:${result}`);
            broadcast({ type: 'error', phone, message: `Upload failed (code ${result})` });
            if (session) {
                updateRedis(phone, session.requestId, {
                    status: 'failed',
                    error:  `Camera reported upload failure (code ${result})`,
                });
                delete _sessions[phone];
            }
        }
        // result=0: FTP STOR handler will mark complete when file lands
    }
});

// ── Core logic ────────────────────────────────────────────────────────────────
async function triggerDownload({ phone, ch, startTime, endTime, folder }) {
    phone = String(phone);
    if (!folder) folder = `/${phone}/`;

    // Generate unique request ID
    const requestId = crypto.randomBytes(8).toString('hex');
    const createdAt = new Date().toISOString();

    log(`▶ triggerDownload requestId:${requestId} phone:${phone} ch:${ch} ${startTime} → ${endTime} folder:${folder}`);

    // Cancel any stuck previous session
    if (_sessions[phone]) {
        bus.emit('device:send', { phone, frame: build9207(phone, 0, 2) });
        await updateRedis(phone, _sessions[phone].requestId, {
            status: 'failed',
            error:  'Superseded by new request',
        });
        delete _sessions[phone];
    }

    // Build the expected file path (camera uses its own naming convention)
    // We can't know exact filename until STOR — but we know the folder
    const expectedFolder = path.join(RECORDINGS_DIR, folder);

    // Save initial Redis record
    const record = {
        requestId,
        phone,
        ch,
        startTime,
        endTime,
        folder,
        status:         'queued',
        filePath:       null,
        filename:       null,
        url:            null,
        fileSize:       null,
        expectedFolder: expectedFolder,
        createdAt,
        updatedAt:      createdAt,
        error:          null,
    };
    await saveToRedis(record);

    // Store session in memory
    _sessions[phone] = { requestId, ch, startTime, endTime, folder, sentAt: Date.now() };

    // Step 1 — query file list
    bus.emit('device:send', { phone, frame: build9205(phone, ch, startTime, endTime) });
    log(`[${phone}] Sent 0x9205`);
    broadcast({ type: 'status', phone, requestId, message: `🔍 Querying camera for ch${ch} recordings...` });

    // Step 2 — send FTP command after 3s
    setTimeout(async () => {
        const frame = build9206(phone, ch, startTime, endTime, folder);
        bus.emit('device:send', { phone, frame });
        log(`[${phone}] Sent 0x9206 folder:${folder}`);
        broadcast({ type: 'status', phone, requestId, message: `⏳ FTP command sent to camera...` });
        await updateRedis(phone, requestId, { status: 'in_progress' });
    }, 3000);

    // Return immediately with requestId so caller can track
    return {
        requestId,
        status:    'queued',
        phone,
        ch,
        startTime,
        endTime,
        folder,
        trackUrl:  `/api/ftp-status/${requestId}`,
        historyUrl: `/api/ftp-history/${phone}`,
        message:   'Download queued. Use trackUrl to poll status.',
    };
}

function cancelDownload(phone) {
    phone = String(phone);
    const session = _sessions[phone];
    if (!session) {
        broadcast({ type: 'status', phone, message: 'No active download' });
        return;
    }
    bus.emit('device:send', { phone, frame: build9207(phone, 0, 2) });
    updateRedis(phone, session.requestId, { status: 'failed', error: 'Cancelled by user' });
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

function build9205(phone, channel, startTime, endTime) {
    const fp   = framePhone(phone);
    const s    = parseDateTime(startTime, '00:00:00');
    const e    = parseDateTime(endTime,   '23:59:59');
    const body = Buffer.alloc(23);
    body[0] = channel;
    bcdBytes(s.y%100, s.mo, s.d, s.h, s.mi, s.s).copy(body, 1);
    bcdBytes(e.y%100, e.mo, e.d, e.h, e.mi, e.s).copy(body, 7);
    body.fill(0x00, 13, 21);
    body[21] = 0; body[22] = 0;
    return buildFrame(0x9205, body, fp);
}

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
    body[p++] = 0;    // avType
    body[p++] = 1;    // streamType: main
    body[p++] = 0;    // storageType: all
    body[p++] = 0x07; // taskCondition: WiFi+LAN+3G/4G
    return buildFrame(0x9206, body, fp);
}

function build9207(phone, sessionId, control) {
    const fp   = framePhone(phone);
    const body = Buffer.alloc(3);
    body.writeUInt16BE(sessionId, 0);
    body[2] = control;
    return buildFrame(0x9207, body, fp);
}

// ── FTP server ────────────────────────────────────────────────────────────────
function makeFtpHandler() {
    return ftpSock => {
        log(`FTP control from ${ftpSock.remoteAddress}:${ftpSock.remotePort}`);

        let uploadStream = null;
        let currentDir   = '/';
        let assignedPort = null;

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
                        try { fs.mkdirSync(dirPath, { recursive: true }); reply(257, `"${arg}" created`); }
                        catch (e) { reply(550, 'Failed to create directory'); }
                        break;
                    }

                    case 'PASV': {
                        if (assignedPort) { freePasvPort(assignedPort); assignedPort = null; }
                        assignedPort = allocatePasvPort();
                        if (!assignedPort) { reply(421, 'No data ports available'); break; }
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
                                } catch (e) { ds.end(''); }
                                slot.dataSocket = null;
                                reply(226, 'Directory send OK');
                            } else { setTimeout(sendList, 100); }
                        };
                        sendList();
                        break;
                    }

                    case 'STOR': {
                        const argDir  = path.dirname(arg);
                        const saveDir = (argDir && argDir !== '.')
                            ? path.join(RECORDINGS_DIR, argDir)
                            : path.join(RECORDINGS_DIR, currentDir);

                        if (!fs.existsSync(saveDir)) fs.mkdirSync(saveDir, { recursive: true });

                        const filename = path.basename(arg || `rec_${Date.now()}.mp4`);
                        let uploadPath = path.join(saveDir, filename);

                        // Rename if duplicate from same camera retry
                        if (fs.existsSync(uploadPath) && fs.statSync(uploadPath).size > 1024) {
                            const ext  = path.extname(filename);
                            const base = path.basename(filename, ext);
                            uploadPath = path.join(saveDir, `${base}_${Date.now()}${ext}`);
                            log(`STOR duplicate — renaming to: ${path.basename(uploadPath)}`);
                        }

                        uploadStream = fs.createWriteStream(uploadPath);
                        log(`STOR → ${uploadPath}`);
                        reply(150, 'Ready to receive');

                        // ── Identify phone and requestId NOW (before async completes) ──
                        // Extract phone from folder using path.relative to handle ./recordings vs recordings
                        const relDir   = path.relative(path.resolve(RECORDINGS_DIR), path.resolve(saveDir));
                        // relDir is now just "15760064474" (the phone folder)
                        const ftpPhone = relDir.split(path.sep)[0] || null;

                        // Capture requestId from session AT THIS MOMENT — not inside async callback
                        // because _sessions[phone] may be deleted by then
                        const capturedRequestId = ftpPhone ? (_sessions[ftpPhone]?.requestId || null) : null;

                        log(`STOR phone:${ftpPhone} requestId:${capturedRequestId}`);

                        const slot = assignedPort ? _pasvPool[assignedPort] : null;

                        // Guard against onComplete firing twice (end + close both call uploadStream.end())
                        let completed = false;

                        const onComplete = async () => {
                            if (completed) return;
                            completed = true;

                            const finalFilename = path.basename(uploadPath);
                            const relPath       = path.relative(path.resolve(RECORDINGS_DIR), path.resolve(uploadPath));
                            const fileSize      = fs.existsSync(uploadPath) ? fs.statSync(uploadPath).size : 0;
                            const fullPath      = path.resolve(uploadPath);

                            log(`✅ Transfer complete: ${finalFilename} (${fileSize} bytes) phone:${ftpPhone} requestId:${capturedRequestId}`);
                            reply(226, 'Transfer complete');

                            broadcast({
                                type:      'ftp_ready',
                                phone:     ftpPhone,
                                requestId: capturedRequestId,
                                url:       `/recordings/${relPath}`,
                                filename:  finalFilename,
                                filePath:  fullPath,
                                fileSize,
                            });

                            // Update Redis to complete
                            if (ftpPhone && capturedRequestId) {
                                await updateRedis(ftpPhone, capturedRequestId, {
                                    status:   'complete',
                                    filePath: fullPath,
                                    filename: finalFilename,
                                    url:      `/recordings/${relPath}`,
                                    fileSize,
                                });
                                // Clean up session only after Redis is updated
                                delete _sessions[ftpPhone];
                                log(`Redis marked complete for ${ftpPhone} requestId:${capturedRequestId}`);
                            } else {
                                log(`⚠️ No requestId captured — Redis not updated. phone:${ftpPhone}`);
                            }

                            if (assignedPort) { freePasvPort(assignedPort); assignedPort = null; }
                        };

                        const handleData = (ds) => {
                            log(`Piping data → ${uploadPath}`);
                            ds.pipe(uploadStream);
                            uploadStream.on('finish', onComplete);
                            // Only call uploadStream.end() once — prefer 'end' over 'close'
                            ds.on('end',   () => { uploadStream.end(); });
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
                                    if (assignedPort) { freePasvPort(assignedPort); assignedPort = null; }
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
                        if (assignedPort && !_pasvPool[assignedPort]?.dataSocket) {
                            freePasvPort(assignedPort); assignedPort = null;
                        }
                        break;

                    default: reply(202, 'Command not implemented');
                }
            });
        });

        ftpSock.on('close', () => {
            log('FTP control connection closed');
            if (uploadStream) { try { uploadStream.end(); } catch (_) {} }
            if (assignedPort && !_pasvPool[assignedPort]?.dataSocket) {
                freePasvPort(assignedPort);
            }
        });
        ftpSock.on('error', e => err('FTP control socket error:', e.message));
    };
}

function startFtpServer() {
    initPasvPool();

    for (let i = 0; i < PASV_POOL_SIZE; i++) {
        const port = PASV_PORT_START + i;
        const slot = _pasvPool[port];
        const srv  = net.createServer(ds => {
            log(`PASV data on :${port} from ${ds.remoteAddress}`);
            if (slot.pendingStor) { slot.pendingStor(ds); slot.pendingStor = null; }
            else {
                slot.dataSocket = ds;
                setTimeout(() => { if (slot.dataSocket === ds) { ds.end(); slot.dataSocket = null; } }, 30000);
            }
        });
        srv.listen(port, '0.0.0.0', () => log(`✓ PASV :${port}`));
        srv.on('error', e => err(`PASV :${port} error:`, e.message));
        slot.server = srv;
    }

    const ftpServer = net.createServer(makeFtpHandler());
    ftpServer.listen(FTP_PORT, '0.0.0.0', () => log(`✓ FTP control on :${FTP_PORT}`));
    ftpServer.on('error', e => err(`FTP :${FTP_PORT} error:`, e.message));

    const ftp21 = net.createServer(makeFtpHandler());
    ftp21.listen(21, '0.0.0.0', () => log(`✓ FTP control on :21 (fallback)`));
    ftp21.on('error', e => {
        warn(`Port 21 unavailable (${e.message})`);
        warn(`Fix: sudo iptables -t nat -A PREROUTING -p tcp --dport 21 -j REDIRECT --to-port ${FTP_PORT}`);
    });
}

startFtpServer();
log(`Started — FTP:${FTP_PORT} PASV:${PASV_PORT_START}-${PASV_PORT_START+PASV_POOL_SIZE-1} HTTP:${HTTP_PORT} WS:${WS_PORT}`);