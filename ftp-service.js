'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// ftp-service.js  —  FTP DOWNLOAD SERVICE  (multi-camera + Redis tracking)
//
// Every download request gets a unique requestId and is tracked per FOLDER in
// Redis. State lives across several purpose-built keys — see ftp-store.js for
// the key list. The legacy `stoppageVideoRecordingTriggered` hash is still
// mirrored on every transition, so the scheduler server needs no change.
//
// Job lifecycle:
//   queued → active → complete
//                   ↘ partial | failed | no_files | interrupted → (retry) → queued
//                                                               → dead (budget spent)
//
// Nothing except a verified `complete` or a `dead` record blocks a retry.
// A job that stalls is caught three ways: an in-process watchdog, a Redis
// lease that expires if the process dies, and a periodic reconciler that
// re-queues due retries and promotes jobs whose blob is already in Azure.
//
// HTTP API  :8082
//   POST   /api/ftp-download        { phone, ch, startTime, endTime, folder }
//          → { requestId, status, terminal, queuePosition, ... }
//   GET    /api/ftp-status/:requestId    → record for that request
//   GET    /api/ftp-history/:phone       → records for a phone (latest 50)
//   GET    /api/ftp-queue/:phone         → active job + durable pending queue
//   GET    /api/ftp-stuck                → everything not complete, with attempts
//   GET    /api/ftp-log/:folder          → state-transition history for one job
//   GET    /api/ftp-migrate-preview      → what the legacy import would do (read-only)
//   POST   /api/ftp-retry           { folder }   → force another attempt
//   POST   /api/ftp-reconcile            → run the recovery sweep now
//   DELETE /api/ftp-record/:folder       → wipe a job and all its indexes
//   POST   /api/ftp-cancel          { phone }
//   GET    /api/sessions                 → active in-memory sessions
//   GET    /recordings/**                → download saved file
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
const { handleEventDownload, handleEventHistory } = require('./event-download');

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

// ── Redis state ───────────────────────────────────────────────────────────────
//
// State lives in ftp-store.js now, spread across purpose-built keys instead of
// one opaque hash — see that file's header for the full key list. The legacy
// `stoppageVideoRecordingTriggered` hash is still mirrored on every transition,
// so the scheduler server needs no change.
//
// The wrappers below keep the existing call sites working while the rest of the
// file moves to explicit state transitions.

const store = require('./ftp-store');
store.init(redis);

// ── Retry / watchdog tuning ───────────────────────────────────────────────────
const MAX_ATTEMPTS      = parseInt(process.env.FTP_MAX_ATTEMPTS      || '3');
const RETRY_BASE_MS     = parseInt(process.env.FTP_RETRY_BASE_MS     || String(2 * 60 * 1000));
const RETRY_MAX_MS      = parseInt(process.env.FTP_RETRY_MAX_MS      || String(30 * 60 * 1000));
const LEASE_MS          = parseInt(process.env.FTP_LEASE_MS          || String(3 * 60 * 1000));
const HANDSHAKE_TIMEOUT = parseInt(process.env.FTP_HANDSHAKE_TIMEOUT || String(90 * 1000));
const TRANSFER_TIMEOUT  = parseInt(process.env.FTP_TRANSFER_TIMEOUT  || String(15 * 60 * 1000));
const RECONCILE_MS      = parseInt(process.env.FTP_RECONCILE_MS      || String(60 * 1000));

// Attempt 1 fails → retry in 2 min, then 4, then 8 … capped at RETRY_MAX_MS.
function backoffMs(attempts) {
    return Math.min(RETRY_BASE_MS * Math.pow(2, Math.max(0, attempts - 1)), RETRY_MAX_MS);
}

// ── Record helpers ────────────────────────────────────────────────────────────
// Patch update: `status` in the patch is the state to move to.
// Unlike the previous version this never silently no-ops — a missing record
// is logged, because that used to be how a finished upload went unrecorded.
async function updateStoppageRecord(folder, patch = {}, reason = '') {
    if (!folder) { warn('updateStoppageRecord called without a folder — update dropped'); return null; }
    const { status, ...rest } = patch;
    const rec = await store.getJob(folder);
    if (!rec) {
        warn(`No job record for folder:${folder} — update dropped (status:${status || 'unchanged'})`);
        return null;
    }
    return store.transition(folder, status || rec.status, {
        patch:  rest,
        reason: reason || status || 'update',
        phone:  rec.phone,
    });
}

async function deleteStoppageRecord(folder) {
    return store.deleteJob(folder);
}

// Mark a job failed and schedule the next attempt, or bury it once the
// attempt budget is spent. This is the single place failure policy lives.
async function failJob(folder, error, reason = 'failed', opts = {}) {
    if (!folder) { warn(`failJob without folder (${reason})`); return null; }
    const rec = await store.getJob(folder);
    if (!rec) { warn(`failJob: no record for folder:${folder} (${reason})`); return null; }

    const attempts = Number(rec.attempts || 0);
    const state    = opts.state || 'failed';
    const budget   = opts.maxAttempts || MAX_ATTEMPTS;

    if (attempts >= budget) {
        log(`[${rec.phone}] ${folder} exhausted ${attempts}/${budget} attempts — marking dead`);
        return store.transition(folder, 'dead', {
            patch:  { error: `${error} (gave up after ${attempts} attempts)`, lastError: error },
            reason,
            phone:  rec.phone,
        });
    }

    const retryAt = Date.now() + backoffMs(attempts);
    log(`[${rec.phone}] ${folder} → ${state}, retry ${attempts + 1}/${budget} in ${Math.round(backoffMs(attempts) / 1000)}s`);
    return store.transition(folder, state, {
        patch:   { error, lastError: error },
        reason,
        retryAt,
        phone:   rec.phone,
    });
}

// ── Read paths (indexed — no more full-hash scans) ────────────────────────────
async function getByRequestId(requestId)   { return store.findByRequestId(requestId); }
async function getCurrentFromRedis(phone)  { return store.latestByPhone(phone); }
async function getAllCurrentFromRedis()    { return store.allJobs(); }
async function getHistoryFromRedis(phone)  { return store.historyByPhone(phone); }


// ── Internal state ────────────────────────────────────────────────────────────
// The pending queue lives in Redis (ftp:queue:<phone>) so it survives a
// restart. Only the job currently being driven is held in memory.
// _sessions[phone] = the SINGLE job currently active (sent to camera)
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

    // ── POST /api/event-download  (everything: see event-download.js) ─────────
    if (req.method === 'POST' && urlPath === '/api/event-download') {
        return handleEventDownload(req, res);
    }

    // ── GET /api/event-history/:deviceId  (see event-download.js) ─────────────
    if (req.method === 'GET' && urlPath.startsWith('/api/event-history/')) {
        return handleEventHistory(req, res);
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
        (async () => {
            const pending = await store.queueList(phone);
            const records = await Promise.all(pending.map(f => store.getJob(f)));
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                phone,
                active:  _sessions[phone] || null,
                lease:   await store.leaseOwner(phone),
                pending: records.filter(Boolean).map((r, i) => ({ ...r, queuePosition: i + 1 })),
                total:   pending.length + (_sessions[phone] ? 1 : 0),
            }));
        })();
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

    // ── GET /api/ftp-stuck  → everything that is not complete ────────────────
    // This is the "why did only 4 of my 5 videos process" endpoint.
    if (req.method === 'GET' && urlPath === '/api/ftp-stuck') {
        (async () => {
            const states = ['queued', 'active', 'partial', 'failed', 'no_files', 'interrupted', 'dead'];
            const out = {};
            for (const s of states) {
                const jobs = await store.jobsInState(s);
                if (jobs.length) {
                    out[s] = jobs.map(j => ({
                        folder:     j.folder,
                        phone:      j.phone,
                        requestId:  j.requestId,
                        startTime:  j.startTime,
                        endTime:    j.endTime,
                        attempts:   Number(j.attempts || 0),
                        maxAttempts: Number(j.maxAttempts || MAX_ATTEMPTS),
                        error:      j.error || null,
                        updatedAt:  j.updatedAt,
                        ageMinutes: j.updatedAt ? Math.round((Date.now() - new Date(j.updatedAt)) / 60000) : null,
                    }));
                }
            }
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ counts: await store.countsByState(), stuck: out, stats: await store.stats() }, null, 2));
        })();
        return;
    }

    // ── GET /api/ftp-log/:folder  → the transition history for one job ───────
    if (req.method === 'GET' && urlPath.startsWith('/api/ftp-log/')) {
        const folder = decodeURIComponent(urlPath.replace('/api/ftp-log/', '')).trim();
        (async () => {
            const record = await store.getJob(folder);
            if (!record) {
                res.writeHead(404, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: `No record for folder ${folder}` }));
                return;
            }
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ record, history: await store.getLog(folder) }, null, 2));
        })();
        return;
    }

    // ── POST /api/ftp-retry  { folder }  → force another attempt ─────────────
    // The supported replacement for deleting the Redis key by hand.
    if (req.method === 'POST' && urlPath === '/api/ftp-retry') {
        let body = '';
        req.on('data', c => body += c);
        req.on('end', async () => {
            try {
                const { folder, resetAttempts = true } = JSON.parse(body || '{}');
                if (!folder) throw new Error('folder is required');
                const record = await store.getJob(folder);
                if (!record) throw new Error(`No record for folder ${folder}`);

                const recovered = await recoverFromStorage(folder, record);
                if (recovered) {
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    res.end(JSON.stringify(recovered));
                    return;
                }

                await store.transition(folder, 'queued', {
                    patch:  resetAttempts ? { attempts: 0, error: null } : { error: null },
                    reason: 'manual-retry',
                    phone:  record.phone,
                });
                await store.queuePush(record.phone, folder);
                if (!_sessions[record.phone]) processNextInQueue(record.phone);

                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ folder, phone: record.phone, status: 'queued', requeued: true }));
            } catch (e) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: e.message }));
            }
        });
        return;
    }

    // ── DELETE /api/ftp-record/:folder  → wipe a job and all its indexes ──────
    if (req.method === 'DELETE' && urlPath.startsWith('/api/ftp-record/')) {
        const folder = decodeURIComponent(urlPath.replace('/api/ftp-record/', '')).trim();
        (async () => {
            const record = await store.getJob(folder);
            if (record?.phone) await store.queueRemove(record.phone, folder);
            await deleteStoppageRecord(folder);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ folder, deleted: true, existed: !!record }));
        })();
        return;
    }

    // ── GET /api/ftp-migrate-preview  → what the legacy import would do ──────
    // Read-only. Add ?full=1 for the per-folder list.
    if (req.method === 'GET' && urlPath === '/api/ftp-migrate-preview') {
        (async () => {
            try {
                const plan = await planLegacyMigration();
                const full = /[?&]full=1/.test(req.url);
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({
                    hash:                plan.hash,
                    dryRunEnabled:       MIGRATE_DRYRUN,
                    autoRetryEnabled:    MIGRATE_AUTORETRY,
                    total:               plan.total,
                    alreadyMigrated:     plan.alreadyMigrated,
                    willImport:          plan.willImport,
                    willAutoRetry:       plan.willAutoRetry,
                    byLegacyStatus:      plan.byLegacyStatus,
                    byTarget:            plan.byTarget,
                    unparsable:          plan.unparsable,
                    unrunnable:          plan.unrunnable,
                    completeWithoutBlob: plan.completeWithoutBlob,
                    items: full
                        ? plan.items.map(({ legacy, ...rest }) => rest)
                        : `${plan.items.length} item(s) — add ?full=1 to list them`,
                }, null, 2));
            } catch (e) {
                res.writeHead(500, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: e.message }));
            }
        })();
        return;
    }

    // ── POST /api/ftp-reconcile  → run the sweep now instead of waiting ──────
    if (req.method === 'POST' && urlPath === '/api/ftp-reconcile') {
        (async () => {
            await reconcile();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ ran: true, counts: await store.countsByState() }));
        })();
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
bus.on('device:connected', async ({ phone }) => {
    log(`Device connected: ${phone}`);
    _seqMap[phone] = 0;
    // Anything left interrupted by an earlier drop becomes due immediately,
    // so a camera coming back online picks its own backlog up.
    for (const rec of await store.jobsInState('interrupted')) {
        if (rec.phone !== phone) continue;
        await store.transition(rec.folder, 'interrupted', {
            reason: 'device-reconnected', retryAt: Date.now(), phone,
        });
    }
    processNextInQueue(phone);
});

bus.on('device:disconnected', async ({ phone }) => {
    log(`Device disconnected: ${phone}`);
    // Interrupted, not failed: the pending work is still wanted. It stays in
    // the durable queue and is retried when the camera returns, instead of
    // being discarded the way the old in-memory queue was.
    const active = _sessions[phone];
    if (active) {
        if (active._jobTimeout) { clearTimeout(active._jobTimeout); active._jobTimeout = null; }
        delete _sessions[phone];
        await store.leaseRelease(phone, active.folderKey);
        await failJob(active.folderKey, 'Device disconnected mid-download', 'device-disconnected', { state: 'interrupted' });
    }
    for (const folder of await store.queueList(phone)) {
        await store.transition(folder, 'interrupted', {
            patch:   { error: 'Device disconnected before this job started' },
            reason:  'device-disconnected',
            retryAt: Date.now() + backoffMs(1),
            phone,
        });
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
                    // Camera is talking — swap the short handshake watchdog for
                    // the long transfer one.
                    touchJob(phone, 'camera-accepted', TRANSFER_TIMEOUT);
                }
            } else {
                err(`[${phone}] Camera rejected 0x9206 code:${replyResult}`);
                broadcast({ type: 'error', phone, message: `Camera rejected request (code ${replyResult})` });
                if (session) {
                    await failJob(session.folderKey,
                        `Camera rejected 0x9206 (code ${replyResult})`, '0x9206-rejected');
                    jobFinished(phone, '0x9206-rejected');  // ← move to next
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
                // 0x1205 returning 0 is not proof the SD card is empty — the
                // camera can be busy or still flushing the current segment, and
                // the 0x9206 that follows often succeeds anyway. So this is a
                // retryable no_files, not the permanent tombstone it used to be.
                await failJob(session.folderKey,
                    '0 files reported by camera for this time range', '0-files', { state: 'no_files' });
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
                await updateStoppageRecord(session.folderKey,
                    { expectedFiles: totalFiles }, 'file-list-received');
                touchJob(phone, 'file-list-received', TRANSFER_TIMEOUT);
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
                await failJob(session.folderKey,
                    `Camera reported upload failure (code ${result})`, '0x1206-fail');
                jobFinished(phone, '0x1206-fail');  // ← move to next
            }
        } else if (session) {
            // Camera finished sending EVERY file for this instruction.
            // Whatever transfers have started are the full set.
            session.cameraDone = true;
            log(`[${phone}] 0x1206 done — started:${session.startedFiles || 0} saved:${session.savedFiles || 0}`);
            if ((session.startedFiles || 0) === 0) {
                // Camera said done but never opened a transfer — nothing to save
                await failJob(session.folderKey,
                    'Camera reported done but uploaded nothing', '0x1206-empty');
                jobFinished(phone, '0x1206-empty');
            } else {
                touchJob(phone, '0x1206-done', TRANSFER_TIMEOUT);
                maybeFinish(phone);
            }
        }
    }
});

// ── Core logic ────────────────────────────────────────────────────────────────

// Keep a running job alive. Every sign of life — a camera reply, an upload
// progress tick — pushes both the in-process watchdog and the Redis lease out.
// If the process dies or the job wedges, the lease simply expires and the
// reconciler can tell the job is orphaned. That is what makes a permanently
// stuck `in_progress` record impossible rather than merely unlikely.
function touchJob(phone, reason = 'activity', timeoutMs = TRANSFER_TIMEOUT) {
    const session = _sessions[phone];
    if (!session) return;

    if (session._jobTimeout) clearTimeout(session._jobTimeout);
    session._jobTimeout = setTimeout(() => {
        const s = _sessions[phone];
        if (!s || s.requestId !== session.requestId) return;
        warn(`[${phone}] Watchdog fired after ${Math.round(timeoutMs / 1000)}s idle (${reason}) — saved ${s.savedFiles || 0}/${s.expectedFiles || '?'}`);
        const partial = (s.savedFiles || 0) > 0;
        failJob(s.folderKey,
            `Stalled after ${reason}: ${s.savedFiles || 0}/${s.expectedFiles || '?'} files saved`,
            'watchdog-timeout',
            { state: partial ? 'partial' : 'interrupted' },
        ).finally(() => jobFinished(phone, 'watchdog-timeout'));
    }, timeoutMs);

    store.leaseRenew(phone, session.folderKey, timeoutMs + LEASE_MS).catch(() => {});
}

// A record is only dispatchable if it carries everything build9205/build9206
// need. Legacy records migrated from the old hash can be missing the time
// window entirely, and an undefined startTime used to throw inside the
// dispatch path and leave the job pinned as `active`.
function unrunnableReason(record) {
    if (!record.startTime || !record.endTime) return 'missing startTime/endTime';
    if (!/^\d{4}-\d{2}-\d{2}/.test(String(record.startTime))) return `unparsable startTime "${record.startTime}"`;
    if (!/^\d{4}-\d{2}-\d{2}/.test(String(record.endTime)))   return `unparsable endTime "${record.endTime}"`;
    if (!record.phone)                                        return 'missing phone';
    if (record.ch == null || Number.isNaN(Number(record.ch))) return 'missing channel';
    return null;
}

// Every call site fires this without awaiting, so it must never reject —
// an unhandled rejection takes the whole process down on Node 15+.
async function processNextInQueue(phone) {
    try {
        await dispatchNext(phone);
    } catch (e) {
        err(`[${phone}] processNextInQueue crashed:`, e.stack || e.message);
        const s = _sessions[phone];
        if (s) {
            await failJob(s.folderKey, `Internal error: ${e.message}`, 'dispatch-crash').catch(() => {});
            await jobFinished(phone, 'dispatch-crash').catch(() => {});
        }
    }
}

// Pull the next folder off the durable queue and start it.
// The queue lives in Redis now, so a restart mid-batch no longer loses the
// jobs that had not been dispatched yet.
async function dispatchNext(phone) {
    if (_sessions[phone]) return;   // something still active — wait for it

    let folder;
    let record;

    // Skip over queue entries whose record has since been completed or removed.
    while ((folder = await store.queuePop(phone))) {
        record = await store.getJob(folder);
        if (!record) { warn(`[${phone}] Queue entry ${folder} has no record — dropped`); continue; }
        if (record.status === 'complete' || record.status === 'dead') {
            log(`[${phone}] Queue entry ${folder} already ${record.status} — skipped`);
            continue;
        }
        // Retrying this would throw in build9205 every time — bury it now so it
        // shows up in /api/ftp-stuck instead of wedging the queue in a loop.
        const bad = unrunnableReason(record);
        if (bad) {
            err(`[${phone}] ${folder} cannot be dispatched (${bad}) — marking dead`);
            await store.transition(folder, 'dead', {
                patch:  { error: `Not dispatchable: ${bad}` },
                reason: 'unrunnable-record',
                phone,
            });
            continue;
        }
        break;
    }

    if (!folder || !record) { log(`[${phone}] Queue empty`); return; }

    // The lease is what stops two workers (or a restarted process racing its
    // own leftovers) from driving the same camera at once.
    if (!await store.leaseAcquire(phone, folder, HANDSHAKE_TIMEOUT + LEASE_MS)) {
        const owner = await store.leaseOwner(phone);
        warn(`[${phone}] Lease held by ${owner} — requeueing ${folder}`);
        await store.queueUnshift(phone, folder);   // keep the batch in order
        return;
    }

    const attempts = Number(record.attempts || 0) + 1;
    const job = {
        requestId:  record.requestId,
        phone,
        ch:         Number(record.ch),
        startTime:  record.startTime,
        endTime:    record.endTime,
        folder:     record.folderPath || `/${folder}/`,
        folderKey:  folder,
        streamType: Number(record.streamType || 1),
        alarmMask:  record.alarmFlag || '0',
        attempt:    attempts,
        sentAt:     Date.now(),
    };

    _sessions[phone] = job;
    log(`[${phone}] Starting ${folder} requestId:${job.requestId} attempt:${attempts}/${MAX_ATTEMPTS}`);

    await store.transition(folder, 'active', {
        patch:  { attempts, lastAttemptAt: new Date().toISOString(), queuePosition: 0, maxAttempts: MAX_ATTEMPTS },
        reason: `dispatch-attempt-${attempts}`,
        phone,
    });

    broadcast({ type: 'status', phone, requestId: job.requestId, message: `▶ Starting download ch${job.ch} ${job.startTime} → ${job.endTime} (attempt ${attempts})` });

    // Anything that throws while building or sending a frame must fail this job
    // and release the camera — otherwise the session stays pinned and every
    // later video for this phone waits behind it.
    try {
        // Arm the watchdog NOW, not once the camera answers. Previously a camera
        // that never replied left the session pinned forever and stalled every
        // later video in the batch.
        touchJob(phone, 'awaiting-camera', HANDSHAKE_TIMEOUT);

        // Step 1 — query file list
        bus.emit('device:send', { phone, frame: build9205(phone, job.ch, job.startTime, job.endTime, BigInt(job.alarmMask || '0'), job.streamType || 1) });
        log(`[${phone}] Sent 0x9205`);
    } catch (e) {
        err(`[${phone}] Dispatch failed for ${folder}:`, e.message);
        await failJob(folder, `Dispatch failed: ${e.message}`, 'dispatch-error');
        await jobFinished(phone, 'dispatch-error');
        return;
    }

    // Step 2 — send FTP command after 3s
    setTimeout(async () => {
        // Check session still matches — may have been cancelled
        if (_sessions[phone]?.requestId !== job.requestId) return;
        try {
            const frame = build9206(phone, job.ch, job.startTime, job.endTime, job.folder, BigInt(job.alarmMask || '0'), job.streamType || 1);
            bus.emit('device:send', { phone, frame });
            log(`[${phone}] Sent 0x9206 folder:${job.folder}`);
            broadcast({ type: 'status', phone, requestId: job.requestId, message: `⏳ FTP command sent to camera...` });
        } catch (e) {
            err(`[${phone}] 0x9206 build failed for ${folder}:`, e.message);
            await failJob(folder, `0x9206 build failed: ${e.message}`, 'dispatch-error');
            await jobFinished(phone, 'dispatch-error');
        }
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
        const saved = s.savedFiles || 0;
        if (saved > 0) {
            store.transition(s.folderKey, 'complete', {
                patch:  { filesSaved: saved, error: null },
                reason: 'all-files-resolved',
                phone,
            }).finally(() => jobFinished(phone, 'all-files-resolved'));
        } else {
            failJob(s.folderKey, 'Camera finished but no file reached storage', 'no-files-saved')
                .finally(() => jobFinished(phone, 'no-files-saved'));
        }
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
    await store.leaseRelease(phone, job.folderKey);
    await store.queueRemove(phone, job.folderKey);
    // Small delay so camera can reset before next job
    setTimeout(() => processNextInQueue(phone), 2000);
}

async function triggerDownload({ phone, ch, startTime, endTime, folder, requestKey, alarmFlag, quality, events }) {
    phone = String(phone);
    if (!folder) folder = `/${phone}/`;

    // The queue, the dedupe claim and the retry schedule all live in Redis.
    // Without it a request would be accepted and then silently dropped, so
    // fail loudly instead of pretending it was queued.
    if (!store.enabled()) {
        throw new Error('Redis is not configured — downloads cannot be queued. Set REDIS_HOST.');
    }

    const streamType = normalizeStreamType(quality);
    const { mask: alarmMask } = buildAlarmMask(events, alarmFlag);
    const requestId = requestKey || crypto.randomBytes(8).toString('hex');
    const createdAt = new Date().toISOString();

    // ── Folder key (strip leading/trailing slashes for consistent Redis key) ──
    const folderKey = folder.replace(/^\/+|\/+$/g, '');

    const newRecord = {
        requestId,
        phone,
        ch,
        startTime,
        endTime,
        folder:     folderKey,
        folderPath: folder,
        streamType,
        quality:    streamType === 2 ? 'low' : 'high',
        alarmFlag:  alarmMask.toString(),
        maxAttempts: MAX_ATTEMPTS,
        createdAt,
    };

    // ── Claim the folder atomically ───────────────────────────────────────────
    // createJob only succeeds for the first caller, so two concurrent requests
    // for the same video can no longer both start.
    const claimed = await store.createJob(folderKey, newRecord);

    if (!claimed) {
        const existing = await store.getJob(folderKey);
        const decision = await decideOnExisting(phone, folderKey, existing);
        if (decision) return decision;

        // Retrying an existing record. Adopt this caller's requestId so the
        // trackUrl we hand back actually resolves, and refresh the request
        // parameters in case the caller widened the time window.
        await store.transition(folderKey, 'queued', {
            patch: {
                requestId,
                startTime,
                endTime,
                ch,
                streamType,
                folderPath: folder,
                alarmFlag:  alarmMask.toString(),
                error:      null,
                // An explicit re-request is fresh intent, so it gets a fresh
                // attempt budget rather than inheriting a spent one.
                attempts:   0,
            },
            reason: 're-requested',
            phone,
        });
    }

    log(`▶ triggerDownload requestId:${requestId} phone:${phone} ch:${ch} ${startTime} → ${endTime} folder:${folderKey}`);

    await store.queuePush(phone, folderKey);
    const pending = await store.queueList(phone);
    const queuePosition = Math.max(0, pending.indexOf(folderKey)) + (_sessions[phone] ? 1 : 0);

    await updateStoppageRecord(folderKey, { queuePosition }, 'enqueued');
    broadcast({ type: 'status', phone, requestId, message: queuePosition === 0 ? `▶ Starting immediately` : `⏳ Queued at position ${queuePosition}` });

    if (!_sessions[phone]) processNextInQueue(phone);

    return {
        requestId,
        status:        'queued',
        terminal:      false,
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

// What to do when a request arrives for a folder we already know about.
// Returns a response to send back, or null meaning "reset it and re-run".
//
// The old version treated `no_files` and any `queued`/`in_progress` as
// permanent, which is how a record ended up needing to be deleted by hand.
// Now the only truly terminal states are a verified `complete` and a `dead`
// record that has burned its whole attempt budget.
async function decideOnExisting(phone, folderKey, existing) {
    if (!existing) return null;   // record vanished under us — treat as new

    const attempts = Number(existing.attempts || 0);

    // ── Terminal: already delivered ──────────────────────────────────────────
    if (existing.status === 'complete' && existing.blobUrl) {
        log(`[${phone}] Duplicate blocked — ${folderKey} already complete`);
        return {
            requestId: existing.requestId, status: 'complete', terminal: true, duplicate: true,
            blobUrl: existing.blobUrl, blobPath: existing.blobPath,
            message: 'Already processed. File available at blobUrl.',
        };
    }

    // ── Genuinely running: leave it alone ────────────────────────────────────
    // "Running" means there is a live lease or an in-process session, not just
    // a status field that says so.
    if (existing.status === 'active') {
        const owner = await store.leaseOwner(phone);
        if (_sessions[phone]?.folderKey === folderKey || owner === folderKey) {
            log(`[${phone}] Duplicate blocked — ${folderKey} is actually running`);
            return {
                requestId: existing.requestId, status: 'in_progress', terminal: false, duplicate: true,
                message: 'Already in progress.',
            };
        }
        warn(`[${phone}] ${folderKey} claims active but holds no lease — orphaned, reclaiming`);
    }

    // ── Still waiting its turn ───────────────────────────────────────────────
    if (existing.status === 'queued') {
        const pending = await store.queueList(phone);
        if (pending.includes(folderKey)) {
            return {
                requestId: existing.requestId, status: 'queued', terminal: false, duplicate: true,
                queuePosition: pending.indexOf(folderKey),
                message: 'Already queued.',
            };
        }
        warn(`[${phone}] ${folderKey} claims queued but is not in the queue — requeueing`);
    }

    // ── Anything else: check storage before spending another attempt ─────────
    const recovered = await recoverFromStorage(folderKey, existing);
    if (recovered) return recovered;

    // A record buried for being unrunnable — typically a legacy import with no
    // time window — must yield to a fresh request that actually supplies one.
    // The caller patches the new parameters in on the retry path.
    if (existing.status === 'dead' && unrunnableReason(existing)) {
        log(`[${phone}] ${folderKey} was dead but unrunnable (${unrunnableReason(existing)}) — adopting the new request`);
        return null;
    }

    if (existing.status === 'dead' || attempts >= MAX_ATTEMPTS) {
        log(`[${phone}] ${folderKey} exhausted ${attempts}/${MAX_ATTEMPTS} attempts — needs manual retry`);
        return {
            requestId: existing.requestId, status: 'dead', terminal: true, duplicate: true,
            attempts, error: existing.error || null,
            message: `Gave up after ${attempts} attempts. POST /api/ftp-retry to force another.`,
            retryUrl: '/api/ftp-retry',
        };
    }

    log(`[${phone}] ${folderKey} was ${existing.status} (attempt ${attempts}) — retrying`);
    return null;   // fall through and re-queue
}

// If the blob is already in Azure, the job really did succeed — the record just
// never got updated (e.g. the camera uploaded into a differently-named folder).
// Promote it instead of re-downloading a video we already have.
async function recoverFromStorage(folderKey, existing) {
    if (!containerClient) return null;
    const expectedBlob = `${folderKey}/vehicle-monitoring-trip.MP4`;
    try {
        const blobClient = containerClient.getBlockBlobClient(expectedBlob);
        if (!await blobClient.exists()) return null;
        const props   = await blobClient.getProperties().catch(() => ({}));
        const blobUrl = blobClient.url;
        await store.transition(folderKey, 'complete', {
            patch: {
                blobUrl,
                blobPath: expectedBlob,
                filename: 'vehicle-monitoring-trip.MP4',
                fileSize: props.contentLength || null,
                storedIn: 'azure-blob',
                error:    null,
            },
            reason: 'recovered-from-storage',
            phone:  existing?.phone,
        });
        log(`Recovered ${folderKey} — blob already in Azure: ${expectedBlob}`);
        return {
            requestId: existing?.requestId, status: 'complete', terminal: true, duplicate: true,
            blobUrl, blobPath: expectedBlob, recovered: true,
            message: 'File already present in Azure.',
        };
    } catch (e) {
        err('Azure exists check error:', e.message);
        return null;
    }
}

// ── Reconciler ────────────────────────────────────────────────────────────────
// The piece that removes the manual step. Every RECONCILE_MS it:
//   1. re-homes jobs whose lease expired (process died, camera wedged),
//   2. verifies unfinished jobs against Azure and promotes real successes,
//   3. re-queues anything whose retry time has come.
let _reconciling = false;

async function reconcile() {
    if (!store.enabled() || _reconciling) return;
    _reconciling = true;
    try {
        // 1 — orphaned `active` jobs: status says running, nothing holds the lease.
        for (const rec of await store.jobsInState('active')) {
            const { folder, phone } = rec;
            if (_sessions[phone]?.folderKey === folder) continue;      // running here
            if (await store.leaseOwner(phone) === folder) continue;    // lease still alive
            warn(`Reconciler: ${folder} is active with no lease — recovering`);
            if (await recoverFromStorage(folder, rec)) continue;
            await failJob(folder, 'Worker lost the job (lease expired)', 'orphaned', { state: 'interrupted' });
        }

        // 2 — jobs stuck in `queued` that nothing is going to pick up.
        for (const rec of await store.jobsInState('queued')) {
            const { folder, phone } = rec;
            const pending = await store.queueList(phone);
            if (pending.includes(folder)) continue;
            if (_sessions[phone]?.folderKey === folder) continue;
            warn(`Reconciler: ${folder} is queued but absent from the queue — requeueing`);
            await store.queuePush(phone, folder);
            if (!_sessions[phone]) processNextInQueue(phone);
        }

        // 3 — anything whose backoff has elapsed.
        for (const folder of await store.dueRetries()) {
            const rec = await store.getJob(folder);
            if (!rec) { await store.clearRetry(folder); continue; }
            if (rec.status === 'complete' || rec.status === 'dead') { await store.clearRetry(folder); continue; }

            if (await recoverFromStorage(folder, rec)) { await store.clearRetry(folder); continue; }

            if (Number(rec.attempts || 0) >= MAX_ATTEMPTS) {
                await store.transition(folder, 'dead', {
                    patch:  { error: rec.error || `Gave up after ${rec.attempts} attempts` },
                    reason: 'attempts-exhausted',
                    phone:  rec.phone,
                });
                continue;
            }

            log(`Reconciler: retrying ${folder} (attempt ${Number(rec.attempts || 0) + 1}/${MAX_ATTEMPTS})`);
            await store.clearRetry(folder);
            await store.transition(folder, 'queued', { reason: 'retry-due', phone: rec.phone });
            await store.queuePush(rec.phone, folder);
            if (!_sessions[rec.phone]) processNextInQueue(rec.phone);
        }
    } catch (e) {
        err('Reconciler error:', e.message);
    } finally {
        _reconciling = false;
    }
}

// ── One-time backfill from the legacy hash ────────────────────────────────────
// Records written before this change exist only in
// `stoppageVideoRecordingTriggered`. Without importing them, an already-
// completed video would look brand new and be downloaded all over again.
//
// Legacy `queued`/`in_progress` entries are the stuck ones — they are imported
// as `interrupted` so they show up in /api/ftp-stuck. They are NOT auto-retried
// unless FTP_MIGRATE_AUTORETRY=true, so a first deploy doesn't kick off a burst
// of downloads; use POST /api/ftp-retry or just re-request them.
const MIGRATE_AUTORETRY = process.env.FTP_MIGRATE_AUTORETRY === 'true';

const LEGACY_STATUS_MAP = {
    complete:    'complete',
    failed:      'failed',
    partial:     'partial',
    no_files:    'no_files',
    queued:      'interrupted',
    in_progress: 'interrupted',
};

const MIGRATE_DRYRUN = process.env.FTP_MIGRATE_DRYRUN === 'true';

const isRetryTarget = t => t !== 'complete' && t !== 'dead';

function legacyHashName() {
    return process.env.REDIS_MIRROR_HASH || 'stoppageVideoRecordingTriggered';
}

// Work out what the migration would do, without writing anything.
// Exposed as GET /api/ftp-migrate-preview so it can be inspected on a running
// service, and used by migrateLegacyHash() so the plan and the action agree.
async function planLegacyMigration() {
    const hashName = legacyHashName();
    const all      = await redis.hgetall(hashName);
    const folders  = Object.keys(all || {});

    const plan = {
        hash:               hashName,
        total:              folders.length,
        alreadyMigrated:    0,
        unparsable:         [],
        unrunnable:         [],
        byLegacyStatus:     {},
        byTarget:           {},
        completeWithoutBlob: [],
        items:              [],
    };

    // One pipelined existence check for the whole hash, rather than a round
    // trip per folder — this can run over thousands of records.
    const already = await store.existingJobs(folders);
    plan.alreadyMigrated = already.size;

    for (const folder of folders) {
        if (already.has(folder)) continue;

        let legacy;
        try { legacy = JSON.parse(all[folder]); } catch (_) { plan.unparsable.push(folder); continue; }

        const legacyStatus = legacy.status || '(none)';
        const phone        = legacy.phone || folder.split('_')[0];
        let   target       = LEGACY_STATUS_MAP[legacy.status] || 'interrupted';

        // Old records predating the current request shape can be missing the
        // time window. Those can never be re-run — importing them as retryable
        // just throws in build9205 on every attempt. Bury them instead, so they
        // are visible in /api/ftp-stuck rather than looping.
        const bad = (target !== 'complete') ? unrunnableReason({ ...legacy, phone }) : null;
        if (bad) {
            target = 'dead';
            plan.unrunnable.push({ folder, legacyStatus, reason: bad });
        }

        plan.byLegacyStatus[legacyStatus] = (plan.byLegacyStatus[legacyStatus] || 0) + 1;
        plan.byTarget[target]             = (plan.byTarget[target] || 0) + 1;

        // A legacy record marked complete but carrying no blobUrl is not
        // treated as delivered — it will be re-downloaded. Worth seeing up front.
        if (target === 'complete' && !legacy.blobUrl) plan.completeWithoutBlob.push(folder);

        plan.items.push({
            folder, phone, legacyStatus, target,
            hasBlob:   !!legacy.blobUrl,
            startTime: legacy.startTime || null,
            endTime:   legacy.endTime || null,
            error:     legacy.error || null,
            legacy,
        });
    }

    plan.willImport    = plan.items.length;
    plan.willAutoRetry = MIGRATE_AUTORETRY
        ? plan.items.filter(i => isRetryTarget(i.target)).length
        : 0;

    return plan;
}

function logMigrationPlan(plan, dryRun) {
    const tag = dryRun ? 'DRY RUN — no writes' : 'applying';
    log(`Legacy migration (${tag}) from "${plan.hash}"`);
    log(`  total fields:      ${plan.total}`);
    log(`  already migrated:  ${plan.alreadyMigrated}`);
    log(`  would import:      ${plan.willImport}`);
    if (plan.unparsable.length) warn(`  unparsable JSON:   ${plan.unparsable.length} → ${plan.unparsable.slice(0, 5).join(', ')}`);
    for (const [from, n] of Object.entries(plan.byLegacyStatus)) {
        log(`    ${from.padEnd(12)} → ${(LEGACY_STATUS_MAP[from] || 'interrupted').padEnd(12)} ${n}`);
    }
    if (plan.completeWithoutBlob.length) {
        warn(`  ${plan.completeWithoutBlob.length} record(s) marked complete but with no blobUrl — these will be re-downloaded:`);
        plan.completeWithoutBlob.slice(0, 10).forEach(f => warn(`    ${f}`));
    }
    if (plan.unrunnable.length) {
        warn(`  ${plan.unrunnable.length} record(s) cannot be re-run and will be imported as dead:`);
        plan.unrunnable.slice(0, 10).forEach(u => warn(`    ${u.folder} — ${u.reason}`));
    }
    log(`  auto-retry on boot: ${plan.willAutoRetry}${MIGRATE_AUTORETRY ? '' : ' (FTP_MIGRATE_AUTORETRY is not true)'}`);
}

async function migrateLegacyHash({ dryRun = MIGRATE_DRYRUN } = {}) {
    if (!store.enabled()) return null;
    try {
        const plan = await planLegacyMigration();
        if (!plan.total) return plan;

        logMigrationPlan(plan, dryRun);

        if (dryRun) {
            log('Legacy migration skipped — FTP_MIGRATE_DRYRUN=true. Unset it to apply.');
            return plan;
        }

        let imported = 0, skipped = 0;
        for (const item of plan.items) {
            const { folder, phone, target, legacy } = item;

            const created = await store.createJob(folder, {
                requestId:   legacy.requestId || `legacy-${folder}`,
                phone,
                ch:          legacy.ch != null ? legacy.ch : 1,
                startTime:   legacy.startTime,
                endTime:     legacy.endTime,
                folderPath:  `/${folder}/`,
                streamType:  legacy.streamType != null ? legacy.streamType : 1,
                quality:     legacy.quality || 'high',
                alarmFlag:   legacy.alarmFlag || '0',
                maxAttempts: MAX_ATTEMPTS,
                createdAt:   legacy.createdAt || new Date().toISOString(),
            });
            if (!created) { skipped++; continue; }   // someone beat us to it

            await store.transition(folder, target, {
                patch: {
                    blobUrl:   legacy.blobUrl || null,
                    blobPath:  legacy.blobPath || null,
                    filename:  legacy.filename || null,
                    fileSize:  legacy.fileSize != null ? legacy.fileSize : null,
                    error:     legacy.error || null,
                    migrated:  'true',
                },
                reason:  `migrated-from-legacy-${legacy.status}`,
                phone,
                retryAt: (MIGRATE_AUTORETRY && isRetryTarget(target)) ? Date.now() : undefined,
            });
            imported++;
        }
        log(`Legacy migration done: imported ${imported}, skipped ${skipped}, already present ${plan.alreadyMigrated}, total ${plan.total}`);
        return plan;
    } catch (e) {
        err('Legacy migration error:', e.message);
        return null;
    }
}

// On boot, nothing is running — any record claiming otherwise is a leftover
// from the previous process and must not block a retry.
async function recoverOnStartup() {
    if (!store.enabled()) return;
    try {
        await migrateLegacyHash();
        for (const rec of await store.jobsInState('active')) {
            warn(`Startup: ${rec.folder} was active when the process stopped — marking interrupted`);
            if (await recoverFromStorage(rec.folder, rec)) continue;
            await failJob(rec.folder, 'Service restarted mid-download', 'startup-recovery', { state: 'interrupted' });
        }
        for (const phone of await store.queuedPhones()) {
            const pending = await store.queueList(phone);
            if (pending.length) {
                log(`Startup: ${phone} has ${pending.length} job(s) still queued — resuming`);
                processNextInQueue(phone);
            }
        }
        const counts = await store.countsByState();
        log('Startup state:', JSON.stringify(counts));
    } catch (e) {
        err('Startup recovery error:', e.message);
    }
}
async function cancelDownload(phone) {
    phone = String(phone);
    const session = _sessions[phone];
    if (!session) {
        broadcast({ type: 'status', phone, message: 'No active download' });
        return;
    }
    bus.emit('device:send', { phone, frame: build9207(phone, 0, 2) });
    // Deliberate cancel — fail it but schedule no retry. A fresh request for
    // the same folder will still start it again.
    await store.transition(session.folderKey, 'failed', {
        patch:  { error: 'Cancelled by user' },
        reason: 'user-cancelled',
        phone,
    });
    broadcast({ type: 'status', phone, message: '🛑 Download cancelled' });
    log(`[${phone}] Cancelled requestId:${session.requestId}`);
    jobFinished(phone, 'user-cancelled');  // move to next in queue
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
                        const activeJob = ftpPhone ? _sessions[ftpPhone] : null;
                        const capturedRequestId = activeJob?.requestId || null;

                        // The record is keyed by the folder from the original request.
                        // If the camera uploads into a differently-named folder the
                        // parsed name won't match anything, and the old code silently
                        // dropped the update — blob in Azure, record stuck forever.
                        // Trust the running job's key and fall back to the parsed one.
                        const recordKey = activeJob?.folderKey || ftpFolder;
                        if (activeJob && ftpFolder && activeJob.folderKey !== ftpFolder) {
                            warn(`STOR folder mismatch — camera used "${ftpFolder}", job expects "${activeJob.folderKey}". Updating the job's record.`);
                        }

                        log(`STOR folder:${ftpFolder} record:${recordKey} phone:${ftpPhone} requestId:${capturedRequestId} filename:${filename}`);

                        // Count the transfer the moment it starts. This was never
                        // incremented, so maybeFinish() could never fire and the
                        // 0x1206 handler always took the "uploaded nothing" branch —
                        // racing a successful upload back to 'failed'.
                        if (activeJob) {
                            activeJob.startedFiles = (activeJob.startedFiles || 0) + 1;
                            touchJob(ftpPhone, 'transfer-started', TRANSFER_TIMEOUT);
                        }

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
                                await store.transition(recordKey, 'complete', {
                                    patch: {
                                        blobUrl,
                                        blobPath,
                                        filename:   finalFilename,
                                        fileSize,
                                        storedIn:   'azure-blob',
                                        uploadedAs: ftpFolder,
                                        error:      null,
                                    },
                                    reason: 'blob-upload-complete',
                                    phone:  ftpPhone,
                                });
                                log(`[${ftpPhone}] saved → ${blobPath}`);
                                maybeFinish(ftpPhone);
                            } else {
                                err(`⚠️ No phone identified for ${blobPath} — record not updated`);
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
                                // Schedule a retry rather than leaving it failed forever.
                                failJob(recordKey, `Azure Blob upload failed: ${e.message}`, 'blob-upload-error')
                                    .finally(() => maybeFinish(ftpPhone));
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
                                        onProgress: (p) => {
                                            log(`☁️  Blob upload progress: ${p.loadedBytes} bytes`);
                                            // A big file is still a live job — keep the
                                            // watchdog and the lease from expiring under it.
                                            if (ftpPhone) touchJob(ftpPhone, 'upload-progress', TRANSFER_TIMEOUT);
                                        },
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

// Recover anything the previous process left mid-flight, then keep sweeping.
// Without this, a restart leaves records claiming `in_progress` that nothing
// will ever finish — the case that used to need a manual Redis delete.
if (store.enabled()) {
    setTimeout(() => recoverOnStartup(), 5000);
    const reconcileTimer = setInterval(() => reconcile(), RECONCILE_MS);
    reconcileTimer.unref?.();
    log(`Reconciler every ${Math.round(RECONCILE_MS / 1000)}s — maxAttempts:${MAX_ATTEMPTS} lease:${Math.round(LEASE_MS / 1000)}s handshake:${Math.round(HANDSHAKE_TIMEOUT / 1000)}s transfer:${Math.round(TRANSFER_TIMEOUT / 60000)}m`);
}

log(`Started — FTP:${FTP_PORT} PASV:${PASV_PORT_START}-${PASV_PORT_START+PASV_POOL_SIZE-1} HTTP:${HTTP_PORT} WS:${WS_PORT}`);