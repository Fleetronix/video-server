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
const { BlobServiceClient } = require('@azure/storage-blob');

// ── Config ────────────────────────────────────────────────────────────────────
const SERVER_IP       = process.env.SERVER_IP             || '127.0.0.1';
const FTP_PORT        = parseInt(process.env.FTP_PORT     || '14992');
const PASV_PORT_START = parseInt(process.env.PASV_PORT    || '14993');
const PASV_POOL_SIZE  = parseInt(process.env.PASV_POOL    || '10');
const HTTP_PORT       = parseInt(process.env.FTP_HTTP_PORT|| '8082');
const WS_PORT         = parseInt(process.env.FTP_WS_PORT  || '8802');
const RECORDINGS_DIR  = process.env.RECORDINGS_DIR        || './recordings';
const REDIS_TTL       = parseInt(process.env.REDIS_TTL    || String(7 * 24 * 3600)); // 7 days

console.log(process.env);


// Azure Blob Storage
const AZURE_CONN_STRING  = process.env.AZURE_STORAGE_CONNECTION_STRING || null;
const AZURE_CONTAINER    = process.env.AZURE_STORAGE_CONTAINER         || 'recordings';
const DELETE_LOCAL_AFTER_UPLOAD = process.env.DELETE_LOCAL_AFTER_UPLOAD !== 'false'; // default true
console.log(`[FTP-SVC] Azure Blob Storage: ${AZURE_CONN_STRING}, ${AZURE_CONN_STRING ? 'enabled' : 'disabled'}, container: ${AZURE_CONTAINER}, deleteLocalAfterUpload: ${DELETE_LOCAL_AFTER_UPLOAD}`);
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

// ── Azure Blob Storage ───────────────────────────────────────────────────────
let blobServiceClient = null;
let containerClient   = null;

async function initAzureBlob() {
    if (!AZURE_CONN_STRING) {
        warn('AZURE_STORAGE_CONNECTION_STRING not set — Blob upload disabled, files stay local only.');
        return;
    }
    try {
        blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_CONN_STRING);
        containerClient    = blobServiceClient.getContainerClient(AZURE_CONTAINER);
        await containerClient.createIfNotExists();
        log(`✅ Azure Blob ready — container: ${AZURE_CONTAINER}`);
    } catch (e) {
        err('Azure Blob init error:', e.message);
        blobServiceClient = null;
        containerClient   = null;
    }
}

initAzureBlob();

// Note: file→blob upload now happens inline via blockBlobClient.uploadStream()
// directly from the FTP data socket in the STOR handler — no local file helper needed.

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
// ── Redis ─────────────────────────────────────────────────────────────────────
// Only ONE Redis hash used:
//
// HASH  stoppageVideoRecordingTriggered
//   field: <folder>   (e.g. 15760064474_tripId_20260702T083914)
//   value: JSON { folder, phone, requestId, status, blobUrl, blobPath,
//                 startTime, endTime, createdAt, updatedAt, error }
//
// folder is used as key so duplicate requests for the same folder are blocked.

const STOPPAGE_HASH = 'stoppageVideoRecordingTriggered';

// ── Check if folder already processed ────────────────────────────────────────
async function getStoppageRecord(folder) {
    if (!redis) return null;
    try {
        const raw = await redis.hget(STOPPAGE_HASH, folder);
        return raw ? JSON.parse(raw) : null;
    } catch (e) {
        err('Redis hget error:', e.message);
        return null;
    }
}

// ── Save new record keyed by folder ──────────────────────────────────────────
async function saveStoppageRecord(folder, data) {
    if (!redis) return;
    try {
        const record = { ...data, updatedAt: new Date().toISOString() };
        await redis.hset(STOPPAGE_HASH, folder, JSON.stringify(record));
        log(`Redis HSET ${STOPPAGE_HASH}[${folder}] status:${data.status}`);
    } catch (e) {
        err('Redis save error:', e.message);
    }
}

// ── Update existing record ────────────────────────────────────────────────────
async function updateStoppageRecord(folder, patch) {
    if (!redis) return;
    try {
        const existing = await redis.hget(STOPPAGE_HASH, folder);
        if (!existing) return;
        const record = { ...JSON.parse(existing), ...patch, updatedAt: new Date().toISOString() };
        await redis.hset(STOPPAGE_HASH, folder, JSON.stringify(record));
        log(`Redis updated ${STOPPAGE_HASH}[${folder}] status:${record.status}`);
    } catch (e) {
        err('Redis update error:', e.message);
    }
}

// ── Delete a record (used when file not found in Azure after failure) ─────────
async function deleteStoppageRecord(folder) {
    if (!redis) return;
    try {
        await redis.hdel(STOPPAGE_HASH, folder);
        log(`Redis deleted ${STOPPAGE_HASH}[${folder}]`);
    } catch (e) {
        err('Redis delete error:', e.message);
    }
}

// ── Kept for API compatibility (ftp-status, ftp-current, ftp-history) ─────────
// These now read from stoppageVideoRecordingTriggered only
async function getByRequestId(requestId) {
    if (!redis) return null;
    try {
        const all = await redis.hgetall(STOPPAGE_HASH);
        if (!all) return null;
        for (const raw of Object.values(all)) {
            const rec = JSON.parse(raw);
            if (rec.requestId === requestId) return rec;
        }
        return null;
    } catch (e) { return null; }
}

async function getCurrentFromRedis(phone) {
    if (!redis) return null;
    try {
        const all = await redis.hgetall(STOPPAGE_HASH);
        if (!all) return null;
        const entries = Object.values(all)
            .map(r => { try { return JSON.parse(r); } catch (_) { return null; } })
            .filter(r => r && r.phone === phone)
            .sort((a, b) => new Date(b.updatedAt) - new Date(a.updatedAt));
        return entries[0] || null;
    } catch (e) { return null; }
}

async function getAllCurrentFromRedis() {
    if (!redis) return {};
    try {
        const all = await redis.hgetall(STOPPAGE_HASH);
        if (!all) return {};
        const result = {};
        for (const [folder, raw] of Object.entries(all)) {
            try { result[folder] = JSON.parse(raw); } catch (_) {}
        }
        return result;
    } catch (e) { return {}; }
}

async function getHistoryFromRedis(phone) {
    if (!redis) return [];
    try {
        const all = await redis.hgetall(STOPPAGE_HASH);
        if (!all) return [];
        return Object.values(all)
            .map(r => { try { return JSON.parse(r); } catch (_) { return null; } })
            .filter(r => r && r.phone === phone)
            .sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    } catch (e) { return []; }
}

// Stubs to avoid breaking existing call sites
async function saveToRedis() {}
async function updateRedis(phone, requestId, patch) {
    // Find folder for this requestId and update it
    if (!redis) return;
    try {
        const all = await redis.hgetall(STOPPAGE_HASH);
        if (!all) return;
        for (const [folder, raw] of Object.entries(all)) {
            const rec = JSON.parse(raw);
            if (rec.requestId === requestId) {
                await updateStoppageRecord(folder, patch);
                break;
            }
        }
    } catch (e) { err('updateRedis error:', e.message); }
}

// ── Internal state ────────────────────────────────────────────────────────────
// _queue[phone]   = array of pending jobs waiting to be sent to camera
// _sessions[phone] = the SINGLE job currently active (sent to camera)
const _queue    = {};   // { [phone]: [ job, job, ... ] }
const _sessions = {};   // { [phone]: job }  — currently active job
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
                const { phone, ch, startTime, endTime, folder, requestKey, alarmFlag, quality, events } = JSON.parse(body);
                if (!phone || !ch || !startTime || !endTime) {
                    res.writeHead(400, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify({ error: 'phone, ch, startTime, endTime are required' }));
                    return;
                }
                const result = await triggerDownload({
                    phone: String(phone), ch, startTime, endTime, folder, requestKey, alarmFlag, quality, events
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

    // ── GET /api/ftp-queue/:phone  → pending queue for a phone ───────────────
    if (req.method === 'GET' && urlPath.startsWith('/api/ftp-queue/')) {
        const phone = urlPath.replace('/api/ftp-queue/', '').trim();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            phone,
            active:  _sessions[phone] || null,
            pending: (_queue[phone] || []).map((j, i) => ({ ...j, queuePosition: i + 1 })),
            total:   (_queue[phone]?.length || 0) + (_sessions[phone] ? 1 : 0),
        }));
        return;
    }

    // ── GET /api/sessions ─────────────────────────────────────────────────────
    if (req.method === 'GET' && urlPath === '/api/sessions') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(_sessions));
        return;
    }

    // ── GET /api/ftp-events  → list valid event names + their masks ───────────
    if (req.method === 'GET' && urlPath === '/api/ftp-events') {
        const events = Object.entries(EVENT_BITS).map(([name, bit]) => ({
            name,
            bit,
            mask: '0x' + BigInt.asUintN(64, 1n << BigInt(bit)).toString(16).padStart(16, '0'),
            group: bit >= 32 ? 'video (this standard)' : 'vehicle (JT/T 808-2011)',
        }));
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ events, aliases: EVENT_ALIASES }));
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
    if (_sessions[phone]) {
        updateRedis(phone, _sessions[phone].requestId, { status: 'failed', error: 'Device disconnected' });
        delete _sessions[phone];
    }
    // Fail all queued jobs too
    if (_queue[phone]) {
        _queue[phone].forEach(job => {
            updateRedis(phone, job.requestId, { status: 'failed', error: 'Device disconnected' });
        });
        _queue[phone] = [];
    }
    delete _seqMap[phone];
});

bus.on('device:message', async ({ msgId, body, seq, phone }) => {

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
                    jobFinished(phone);  // ← move to next
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
                // Mark as no_files — blocks retries for this folder
                await updateStoppageRecord(session.folderKey, {
                    status: 'no_files',
                    error:  '0 files found on camera SD card for this time range',
                });
                jobFinished(phone, '0-files');
            }
        } else {
            broadcast({ type: 'status', phone, message: `📁 Camera found ${totalFiles} file(s)` });
            const session = _sessions[phone];
            if (session) {
                session.expectedFiles = totalFiles;
                session.startedFiles  = session.startedFiles  || 0;
                session.resolvedFiles = session.resolvedFiles || 0;
                session.savedFiles    = session.savedFiles    || 0;
                session.cameraDone    = false;
                if (session._jobTimeout) clearTimeout(session._jobTimeout);
                session._jobTimeout = setTimeout(() => {
                    warn(`[${phone}] Job timeout — saved ${session.savedFiles}/${totalFiles} file(s)`);
                    updateStoppageRecord(session.folderKey, {
                        status: session.savedFiles > 0 ? 'partial' : 'failed',
                        error:  `Timed out: ${session.savedFiles}/${totalFiles} saved`,
                    });
                    jobFinished(phone, 'job-timeout');
                }, 15 * 60 * 1000);
            }
        }
        return;
    }

    // 0x1206 — file upload complete notification from camera
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
                jobFinished(phone, '0x1206-fail');  // ← move to next
            }
        } else if (session) {
            // Camera finished sending EVERY file for this instruction.
            // Whatever transfers have started are the full set.
            session.cameraDone = true;
            log(`[${phone}] 0x1206 done — started:${session.startedFiles || 0} saved:${session.savedFiles || 0}`);
            if ((session.startedFiles || 0) === 0) {
                // Camera said done but never opened a transfer — nothing to save
                updateRedis(phone, session.requestId, {
                    status: 'failed',
                    error:  'Camera reported done but uploaded nothing',
                });
                jobFinished(phone, '0x1206-empty');
            } else {
                maybeFinish(phone);
            }
        }
    }
});

// ── Core logic ────────────────────────────────────────────────────────────────

// Process next job in queue for a phone — called after each job completes/fails
async function processNextInQueue(phone) {
    // If something still active, wait for it to finish
    if (_sessions[phone]) return;

    const queue = _queue[phone];
    if (!queue || queue.length === 0) {
        log(`[${phone}] Queue empty`);
        return;
    }

    const job = queue.shift();
    log(`[${phone}] Queue: starting job requestId:${job.requestId} (${queue.length} remaining)`);

    _sessions[phone] = job;

    // Update Redis status to in_progress
    await updateRedis(phone, job.requestId, { status: 'in_progress', queuePosition: 0 });
    broadcast({ type: 'status', phone, requestId: job.requestId, message: `▶ Starting download ch${job.ch} ${job.startTime} → ${job.endTime}` });

    // Step 1 — query file list
    bus.emit('device:send', { phone, frame: build9205(phone, job.ch, job.startTime, job.endTime, BigInt(job.alarmMask || '0'), job.streamType || 1) });
    log(`[${phone}] Sent 0x9205`);

    // Step 2 — send FTP command after 3s
    setTimeout(async () => {
        // Check session still matches — may have been cancelled
        if (_sessions[phone]?.requestId !== job.requestId) return;
        const frame = build9206(phone, job.ch, job.startTime, job.endTime, job.folder, BigInt(job.alarmMask || '0'), job.streamType || 1);
        bus.emit('device:send', { phone, frame });
        log(`[${phone}] Sent 0x9206 folder:${job.folder}`);
        broadcast({ type: 'status', phone, requestId: job.requestId, message: `⏳ FTP command sent to camera...` });
    }, 3000);
}

// Advance the queue only when the camera says it's done AND every transfer that
// started has resolved (uploaded to Azure or failed). Handles the case where the
// camera lists 3 files but sends fewer, and where the last blob is still flushing.
function maybeFinish(phone) {
    const s = _sessions[phone];
    if (!s) return;
    if (s.cameraDone &&
        (s.startedFiles  || 0) > 0 &&
        (s.resolvedFiles || 0) >= (s.startedFiles || 0)) {
        updateRedis(phone, s.requestId, {
            status:     (s.savedFiles || 0) > 0 ? 'complete' : 'failed',
            filesSaved: s.savedFiles || 0,
        });
        jobFinished(phone, 'all-files-resolved');
    }
}

// Called when a job finishes (complete or failed) — clears session and starts next.
// Idempotent: duplicate calls for the same job are ignored.
async function jobFinished(phone, reason = 'done') {
    const job = _sessions[phone];
    if (!job) return;                       // already finished — ignore duplicate
    if (job._jobTimeout) { clearTimeout(job._jobTimeout); job._jobTimeout = null; }
    log(`[${phone}] jobFinished (${reason}) requestId:${job.requestId} saved:${job.savedFiles || 0}/${job.expectedFiles || '?'}`);
    delete _sessions[phone];
    // Small delay so camera can reset before next job
    setTimeout(() => processNextInQueue(phone), 2000);
}

async function triggerDownload({ phone, ch, startTime, endTime, folder, requestKey, alarmFlag, quality, events }) {
    phone = String(phone);
    if (!folder) folder = `/${phone}/`;

    const streamType = normalizeStreamType(quality);
    const { mask: alarmMask, resolved: events_resolved, unknown: events_unknown } = buildAlarmMask(events, alarmFlag);
    const requestId = requestKey || crypto.randomBytes(8).toString('hex');
    const createdAt = new Date().toISOString();

    // ── Folder key (strip leading/trailing slashes for consistent Redis key) ──
    const folderKey = folder.replace(/^\/+|\/+$/g, '');

    // ── DUPLICATE CHECK — if folder already processed, skip ──────────────────
    const existing = await getStoppageRecord(folderKey);
    if (existing) {
        if (existing.status === 'complete' && existing.blobUrl) {
            log(`[${phone}] Duplicate blocked — folder:${folderKey} already complete blobUrl:${existing.blobUrl}`);
            return {
                requestId:    existing.requestId,
                status:       'complete',
                duplicate:    true,
                blobUrl:      existing.blobUrl,
                blobPath:     existing.blobPath,
                message:      'Already processed. File available at blobUrl.',
            };
        }
        if (existing.status === 'no_files') {
            log(`[${phone}] Duplicate blocked — folder:${folderKey} previously had 0 files`);
            return {
                requestId:    existing.requestId,
                status:       'no_files',
                duplicate:    true,
                message:      'Camera had 0 files for this time range. Not retrying.',
            };
        }
        if (existing.status === 'queued' || existing.status === 'in_progress') {
            log(`[${phone}] Duplicate blocked — folder:${folderKey} already ${existing.status}`);
            return {
                requestId:    existing.requestId,
                status:       existing.status,
                duplicate:    true,
                message:      `Already ${existing.status}.`,
            };
        }
        // status === 'failed' → check Azure before retrying
        if (existing.status === 'failed' && containerClient) {
            const expectedBlob = `${folderKey}/vehicle-monitoring-trip.MP4`;
            try {
                const blobClient = containerClient.getBlockBlobClient(expectedBlob);
                const exists = await blobClient.exists();
                if (exists) {
                    // File is in Azure despite failed status — update and return
                    const blobUrl = blobClient.url;
                    await updateStoppageRecord(folderKey, { status: 'complete', blobUrl, blobPath: expectedBlob });
                    log(`[${phone}] Failed record recovered — file found in Azure: ${expectedBlob}`);
                    return { requestId: existing.requestId, status: 'complete', duplicate: true, blobUrl, message: 'File found in Azure.' };
                } else {
                    // Not in Azure — delete Redis key and retry
                    await deleteStoppageRecord(folderKey);
                    log(`[${phone}] Failed record removed — file not in Azure, retrying: ${folderKey}`);
                }
            } catch (e) {
                err('Azure exists check error:', e.message);
            }
        }
    }

    // Init queue for this phone
    if (!_queue[phone]) _queue[phone] = [];
    const queuePosition = _queue[phone].length + (_sessions[phone] ? 1 : 0);

    log(`▶ triggerDownload requestId:${requestId} phone:${phone} ch:${ch} ${startTime} → ${endTime} folder:${folderKey} queuePos:${queuePosition}`);

    const job = { requestId, phone, ch, startTime, endTime, folder, folderKey, streamType, alarmMask: alarmMask.toString(), sentAt: null };

    // Save initial record to stoppageVideoRecordingTriggered keyed by folder
    await saveStoppageRecord(folderKey, {
        requestId,
        phone,
        ch,
        startTime,
        endTime,
        folder:       folderKey,
        streamType,
        quality:      streamType === 2 ? 'low' : 'high',
        alarmFlag:    alarmMask.toString(),
        status:       'queued',
        queuePosition,
        blobUrl:      null,
        blobPath:     null,
        filename:     null,
        fileSize:     null,
        createdAt,
        error:        null,
    });

    _queue[phone].push(job);

    broadcast({ type: 'status', phone, requestId, message: queuePosition === 0 ? `▶ Starting immediately` : `⏳ Queued at position ${queuePosition}` });

    _queue[phone].forEach((j, i) => {
        updateStoppageRecord(j.folderKey, { queuePosition: i + 1 });
    });

    if (!_sessions[phone]) processNextInQueue(phone);

    return {
        requestId,
        status:        'queued',
        queuePosition,
        phone,
        ch,
        startTime,
        endTime,
        folder:        folderKey,
        trackUrl:      `/api/ftp-status/${requestId}`,
        message:       queuePosition === 0 ? 'Starting immediately.' : `Queued at position ${queuePosition}.`,
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
    broadcast({ type: 'status', phone, message: '🛑 Download cancelled' });
    log(`[${phone}] Cancelled requestId:${session.requestId}`);
    jobFinished(phone);  // move to next in queue
}

// ── Frame builders ────────────────────────────────────────────────────────────

// Map a quality hint from the request body to a protocol stream type (Table 26).
//   high / main / 1        → 1 (main stream  = high quality)
//   low  / sub  / 2        → 2 (sub stream   = low quality)
//   anything else / unset  → 1 (default to high)
function normalizeStreamType(quality) {
    if (quality === undefined || quality === null) return 1;
    const q = String(quality).trim().toLowerCase();
    if (q === 'low'  || q === 'sub'  || q === '2') return 2;
    if (q === 'high' || q === 'main' || q === '1') return 1;
    return 1;
}

// Normalize an alarm/event filter into a 64-bit BigInt mask (Table 26 alarm logo).
// Accepts: number, decimal string, '0x..' hex string, or BigInt. undefined/null → 0n.
//   bit0–bit31  : JT/T 808-2011 Table 18 alarm flags
//   bit32–bit63 : video alarm flags (Table 13 of this standard)
function normalizeAlarmFlag(alarmFlag) {
    if (alarmFlag === undefined || alarmFlag === null || alarmFlag === '') return 0n;
    try {
        if (typeof alarmFlag === 'bigint') return BigInt.asUintN(64, alarmFlag);
        if (typeof alarmFlag === 'number') return BigInt.asUintN(64, BigInt(Math.trunc(alarmFlag)));
        const s = String(alarmFlag).trim();
        const v = s.toLowerCase().startsWith('0x') ? BigInt(s) : BigInt(s);
        return BigInt.asUintN(64, v);
    } catch (e) {
        warn(`Invalid alarmFlag '${alarmFlag}' — ignoring, using 0 (no filter)`);
        return 0n;
    }
}

// Named events → bit position in the 64-bit alarm logo.
//   bit0–bit31  : JT/T 808-2011 Table 18 vehicle alarms (subset of common ones)
//   bit32–bit63 : video alarms defined in Table 14 of this standard
// Keys are the canonical names; ALIASES below map alternate spellings to them.
const EVENT_BITS = {
    // ── Common JT/T 808-2011 vehicle alarms (bit0–bit31) ──
    emergency:                 0,
    overspeed:                 1,
    fatigue_driving:           2,
    // ── Video alarms, this standard, Table 14 (bit32–bit63) ──
    video_signal_loss:         32,
    video_signal_blocking:     33,
    storage_unit_failure:      34,
    other_video_failure:       35,
    bus_overload:              36,
    abnormal_driving_behavior: 37,
    special_alarm_recording:   38,
};

// Friendly aliases → canonical event name.
const EVENT_ALIASES = {
    sos:                  'emergency',
    speeding:             'overspeed',
    fatigue:              'fatigue_driving',
    signal_loss:          'video_signal_loss',
    loss:                 'video_signal_loss',
    blocking:             'video_signal_blocking',
    occlusion:            'video_signal_blocking',
    storage_failure:      'storage_unit_failure',
    storage_fault:        'storage_unit_failure',
    equipment_failure:    'other_video_failure',
    overload:             'bus_overload',
    abnormal_driving:     'abnormal_driving_behavior',
    special_recording:    'special_alarm_recording',
    special_alarm:        'special_alarm_recording',
};

function resolveEventName(name) {
    const key = String(name).trim().toLowerCase().replace(/[\s-]+/g, '_');
    if (key in EVENT_BITS)    return key;
    if (key in EVENT_ALIASES) return EVENT_ALIASES[key];
    return null;
}

// Build a combined 64-bit mask from an optional `events` array AND an optional
// raw `alarmFlag`. The two are OR'd, so callers can mix named events with raw
// bits. Returns { mask: BigInt, resolved: [names], unknown: [names] }.
function buildAlarmMask(events, alarmFlag) {
    let mask = normalizeAlarmFlag(alarmFlag);   // raw hex/decimal contribution (or 0n)
    const resolved = [];
    const unknown  = [];

    if (events !== undefined && events !== null) {
        const list = Array.isArray(events) ? events : [events];
        for (const e of list) {
            const canon = resolveEventName(e);
            if (canon === null) { unknown.push(String(e)); continue; }
            mask |= (1n << BigInt(EVENT_BITS[canon]));
            resolved.push(canon);
        }
        if (unknown.length) {
            warn(`Unknown event name(s) ignored: ${unknown.join(', ')}. Valid: ${Object.keys(EVENT_BITS).join(', ')}`);
        }
    }

    return { mask: BigInt.asUintN(64, mask), resolved, unknown };
}

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

function build9205(phone, channel, startTime, endTime, alarmMask = 0n, streamType = 1) {
    const fp   = framePhone(phone);
    const s    = parseDateTime(startTime, '00:00:00');
    const e    = parseDateTime(endTime,   '23:59:59');
    const body = Buffer.alloc(23);
    body[0] = channel;
    bcdBytes(s.y%100, s.mo, s.d, s.h, s.mi, s.s).copy(body, 1);
    bcdBytes(e.y%100, e.mo, e.d, e.h, e.mi, e.s).copy(body, 7);
    // Alarm logo (64 bits, big-endian) — event filter; 0 = no alarm condition
    body.writeBigUInt64BE(BigInt.asUintN(64, BigInt(alarmMask)), 13);
    body[21] = 0;                              // avType: audio+video
    body[22] = (streamType === 2 ? 2 : 1);     // stream: 1=main(high), 2=sub(low)
    return buildFrame(0x9205, body, fp);
}

function build9206(phone, channel, startTime, endTime, folder = '/', alarmMask = 0n, streamType = 1) {
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
    // Alarm logo (64 bits, big-endian) — event filter; 0 = no alarm condition
    //   bit0–bit31  : JT/T 808-2011 Table 18 alarm flags
    //   bit32–bit63 : video alarm flags (Table 13 of this standard)
    body.writeBigUInt64BE(BigInt.asUintN(64, BigInt(alarmMask)), p); p += 8;
    body[p++] = 0;                       // avType
    body[p++] = (streamType === 2 ? 2 : 1); // streamType: 1=main(high), 2=sub(low)
    body[p++] = 0;                       // storageType: all
    body[p++] = 0x07;                    // taskCondition: WiFi+LAN+3G/4G
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

                        const filename = path.basename(arg || `rec_${Date.now()}.mp4`);

                        // ── Identify folder, phone and requestId NOW ─────────────────
                        // Camera uploads into a folder named like:
                        //   <phone>_<id>_<timestamp>/CH0-....MP4
                        // The session is keyed by the BARE phone, so we must split the
                        // folder on '_' and take the prefix — otherwise the lookup misses
                        // and requestId comes back null (queue never advances).
                        const relDir    = path.relative(path.resolve(RECORDINGS_DIR), path.resolve(saveDir));
                        const ftpFolder = relDir.split(path.sep)[0] || '';          // full folder, for blob path
                        const ftpPhone  = (ftpFolder.split('_')[0] || '').trim() || null;  // bare phone, for session
                        const capturedRequestId = ftpPhone ? (_sessions[ftpPhone]?.requestId || null) : null;

                        log(`STOR folder:${ftpFolder} phone:${ftpPhone} requestId:${capturedRequestId} filename:${filename}`);

                        if (!containerClient) {
                            err('Azure Blob not configured — cannot accept STOR. Set AZURE_STORAGE_CONNECTION_STRING.');
                            reply(550, 'Storage backend not configured');
                            break;
                        }

                        // Fixed filename — easy to find in Azure
                        let finalFilename = 'vehicle-monitoring-trip.MP4';

                        const blobPath = `${ftpFolder || ftpPhone || 'unknown'}/${finalFilename}`;
                        const blockBlobClient = containerClient.getBlockBlobClient(blobPath);

                        log(`STOR → streaming directly to Azure Blob: ${blobPath}`);
                        reply(150, 'Ready to receive');

                        const slot = assignedPort ? _pasvPool[assignedPort] : null;
                        let completed = false;

                        const onComplete = async (fileSize) => {
                            if (completed) return;
                            completed = true;
                            clearTimeout(uploadTimeout);

                            const blobUrl = blockBlobClient.url;
                            log(`✅ ☁️  Blob upload complete: ${blobPath} (${fileSize} bytes)`);
                            reply(226, 'Transfer complete');

                            broadcast({
                                type:      'ftp_ready',
                                phone:     ftpPhone,
                                requestId: capturedRequestId,
                                url:       blobUrl,
                                filename:  finalFilename,
                                blobUrl,
                                blobPath,
                                fileSize,
                            });

                            if (ftpPhone) {
                                const session = _sessions[ftpPhone];
                                if (session && session.requestId === capturedRequestId) {
                                    session.savedFiles    = (session.savedFiles    || 0) + 1;
                                    session.resolvedFiles = (session.resolvedFiles || 0) + 1;
                                }
                                // Update stoppageVideoRecordingTriggered keyed by folder
                                await updateStoppageRecord(ftpFolder, {
                                    status:   'complete',
                                    blobUrl,
                                    blobPath,
                                    filename: finalFilename,
                                    fileSize,
                                    storedIn: 'azure-blob',
                                });
                                log(`[${ftpPhone}] saved → ${blobPath}`);
                                maybeFinish(ftpPhone);
                            } else {
                                log(`⚠️ No phone identified — Redis not updated`);
                            }

                            if (assignedPort) { freePasvPort(assignedPort); assignedPort = null; }
                        };

                        const onError = (e) => {
                            if (completed) return;
                            completed = true;
                            err(`Blob upload error for ${blobPath}:`, e.message);
                            reply(426, 'Transfer aborted — storage upload failed');
                            if (ftpPhone) {
                                const session = _sessions[ftpPhone];
                                if (session && session.requestId === capturedRequestId) {
                                    session.resolvedFiles = (session.resolvedFiles || 0) + 1;
                                }
                                // Mark failed — next request will check Azure and retry if needed
                                updateStoppageRecord(ftpFolder, {
                                    status: 'failed',
                                    error:  `Azure Blob upload failed: ${e.message}`,
                                });
                                maybeFinish(ftpPhone);
                            }
                            if (assignedPort) { freePasvPort(assignedPort); assignedPort = null; }
                        };

                        // Safety timeout — if upload hangs for 10 min, fail it
                        const uploadTimeout = setTimeout(() => {
                            if (!completed) {
                                onError(new Error('Upload timeout after 10 minutes'));
                            }
                        }, 10 * 60 * 1000);

                        // ── Pipe FTP data socket directly into Azure Blob — no disk write ──
                        const handleData = async (ds) => {
                            log(`Streaming data socket → Azure Blob (no local disk)`);

                            // Wrap in PassThrough so we can handle 'close' without 'end'
                            // Azure SDK's uploadStream needs a proper stream end signal
                            const { PassThrough } = require('stream');
                            const pass = new PassThrough();
                            let totalBytes = 0;

                            // Idle timeout: no bytes for 60s → treat the socket as dead and
                            // abort cleanly, instead of waiting the full 10-minute backstop.
                            ds.setTimeout(60000, () => {
                                err(`Data socket idle 60s — aborting (${totalBytes} bytes so far)`);
                                pass.destroy(new Error(`Idle timeout after ${totalBytes} bytes`));
                                try { ds.destroy(); } catch (_) {}
                            });

                            ds.on('data',  chunk => { totalBytes += chunk.length; });
                            ds.on('error', e => { pass.destroy(e); });
                            ds.on('end',   () => { pass.end(); });
                            ds.on('close', () => {
                                // Some cameras close socket without emitting 'end'
                                if (!pass.writableEnded) pass.end();
                            });

                            // Guard: an early-arriving socket may already be finished
                            if (ds.destroyed || ds.readableEnded) {
                                pass.end();
                            } else {
                                ds.resume();   // undo pause() applied to early sockets
                                ds.pipe(pass);
                            }

                            try {
                                await blockBlobClient.uploadStream(
                                    pass,
                                    4 * 1024 * 1024,   // 4MB block size
                                    5,                  // 5 parallel blocks
                                    {
                                        blobHTTPHeaders:    { blobContentType: 'video/mp4' },
                                        onProgress: (p) => log(`☁️  Blob upload progress: ${p.loadedBytes} bytes`),
                                    }
                                );
                                await onComplete(totalBytes);
                            } catch (e) {
                                onError(e);
                            }
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
                                    // Count this file as resolved (failed) so the queue can advance
                                    if (ftpPhone) {
                                        const s = _sessions[ftpPhone];
                                        if (s && s.requestId === capturedRequestId) {
                                            s.resolvedFiles = (s.resolvedFiles || 0) + 1;
                                        }
                                        maybeFinish(ftpPhone);
                                    }
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
                ds.pause();                 // hold data until STOR's handleData attaches listeners
                slot.dataSocket = ds;
                setTimeout(() => { if (slot.dataSocket === ds) { ds.end(); slot.dataSocket = null; } }, 60000);
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