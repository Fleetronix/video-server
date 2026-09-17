'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// ftp-store.js — Redis state layer for the FTP download service.
//
// Replaces the single `stoppageVideoRecordingTriggered` hash with a set of
// purpose-built keys, so a job that gets stuck is visible instead of silent.
//
//   ftp:job:<folder>        HASH   the record itself (source of truth)
//   ftp:log:<folder>        LIST   capped transition history — why it stalled
//   ftp:s:<state>           SET    folders currently in that state
//   ftp:queue:<phone>       LIST   durable pending queue (survives restart)
//   ftp:active:<phone>      STRING lease on the running job, auto-expires
//   ftp:retry               ZSET   folder → nextAttemptAt (ms)
//   ftp:idx:request         HASH   requestId → folder
//   ftp:idx:phone:<phone>   ZSET   folder → createdAt (ms)
//   ftp:stats:<YYYYMMDD>    HASH   per-day transition counters
//
// The legacy hash is still written on every transition so the scheduler
// server keeps working unchanged — see mirrorRecord().
//
// NOTE ON CLUSTERING: these keys are not hash-tagged. Azure Redis in
// non-clustered mode (the default for Basic/Standard) is fine. If you ever
// move to a clustered Premium tier, add a {<folder>} tag to the per-folder
// keys so a single transition stays inside one slot.
// ─────────────────────────────────────────────────────────────────────────────

const PREFIX      = process.env.REDIS_PREFIX || 'ftp';
const MIRROR_HASH = process.env.REDIS_MIRROR_HASH || 'stoppageVideoRecordingTriggered';
const TTL_SECONDS = parseInt(process.env.REDIS_TTL || String(7 * 24 * 3600));

// Order matters — the Lua script maps STATES[i] to KEYS[STATE_KEY_OFFSET + i].
const STATES = ['queued', 'active', 'complete', 'partial', 'failed', 'no_files', 'interrupted', 'dead'];

// Statuses the scheduler server already understands. Internal states it has
// never seen are mirrored as the closest legacy equivalent, with the real
// state carried alongside in `internalStatus`.
const MIRROR_STATUS = {
    queued:      'queued',
    active:      'in_progress',
    complete:    'complete',
    partial:     'partial',
    failed:      'failed',
    no_files:    'no_files',
    interrupted: 'failed',
    dead:        'failed',
};

const NUMERIC_FIELDS = ['ch', 'fileSize', 'attempts', 'maxAttempts', 'filesSaved',
                        'queuePosition', 'streamType', 'expectedFiles', 'createdAtMs'];

const K = {
    job:      f => PREFIX + ':job:' + f,
    log:      f => PREFIX + ':log:' + f,
    state:    s => PREFIX + ':s:' + s,
    queue:    p => PREFIX + ':queue:' + p,
    active:   p => PREFIX + ':active:' + p,
    retry:    () => PREFIX + ':retry',
    idxReq:   () => PREFIX + ':idx:request',
    idxPhone: p => PREFIX + ':idx:phone:' + p,
    stats:    d => PREFIX + ':stats:' + d,
    mirror:   () => MIRROR_HASH,
};

const log  = (...a) => console.log ('[FTP-STORE]', ...a);
const warn = (...a) => console.warn('[FTP-STORE]', ...a);
const err  = (...a) => console.error('[FTP-STORE]', ...a);

let redis = null;

// ── Atomic transition ────────────────────────────────────────────────────────
// Applies a field patch, moves the folder between state sets, refreshes the
// indexes, schedules/clears a retry and appends to the audit log — all in one
// round trip, so the record and its indexes can never disagree.
const TRANSITION_LUA = [
    "local folder    = ARGV[1]",
    "local to        = ARGV[2]",
    "local nowIso    = ARGV[3]",
    "local nowMs     = tonumber(ARGV[4])",
    "local ttl       = tonumber(ARGV[5])",
    "local patch     = cjson.decode(ARGV[6])",
    "local delFields = cjson.decode(ARGV[7])",
    "local retryAt   = tonumber(ARGV[8])",
    "local expect    = ARGV[9]",
    "local reason    = ARGV[10]",
    "local phone     = ARGV[11]",
    "",
    "local STATES = {'queued','active','complete','partial','failed','no_files','interrupted','dead'}",
    "local OFFSET = 6",
    "",
    "if redis.call('EXISTS', KEYS[1]) == 0 then",
    "  return {0, 'MISSING', {}}",
    "end",
    "",
    "local from = redis.call('HGET', KEYS[1], 'status') or ''",
    "if expect ~= '' and from ~= expect then",
    "  return {0, from, {}}",
    "end",
    "",
    "local args = {}",
    "for k, v in pairs(patch) do",
    "  table.insert(args, k)",
    "  table.insert(args, tostring(v))",
    "end",
    "table.insert(args, 'status');    table.insert(args, to)",
    "table.insert(args, 'updatedAt'); table.insert(args, nowIso)",
    "if from ~= to then",
    "  table.insert(args, 'stateChangedAt'); table.insert(args, nowIso)",
    "end",
    "redis.call('HSET', KEYS[1], unpack(args))",
    "",
    "if #delFields > 0 then",
    "  redis.call('HDEL', KEYS[1], unpack(delFields))",
    "end",
    "",
    "-- Exactly one state set holds this folder, whatever `from` claimed.",
    "for i, s in ipairs(STATES) do",
    "  if s == to then",
    "    redis.call('SADD', KEYS[OFFSET + i], folder)",
    "  else",
    "    redis.call('SREM', KEYS[OFFSET + i], folder)",
    "  end",
    "end",
    "",
    "local reqId = redis.call('HGET', KEYS[1], 'requestId')",
    "if reqId then redis.call('HSET', KEYS[4], reqId, folder) end",
    "",
    "if phone ~= '' then",
    "  local createdAtMs = tonumber(redis.call('HGET', KEYS[1], 'createdAtMs')) or nowMs",
    "  redis.call('ZADD', KEYS[5], createdAtMs, folder)",
    "end",
    "",
    "if retryAt >= 0 then",
    "  redis.call('ZADD', KEYS[3], retryAt, folder)",
    "else",
    "  redis.call('ZREM', KEYS[3], folder)",
    "end",
    "",
    "redis.call('LPUSH', KEYS[2], cjson.encode({ts = nowIso, from = from, to = to, reason = reason}))",
    "redis.call('LTRIM', KEYS[2], 0, 99)",
    "",
    "redis.call('HINCRBY', KEYS[6], to, 1)",
    "redis.call('EXPIRE', KEYS[6], 604800)",
    "",
    "if ttl > 0 then",
    "  redis.call('EXPIRE', KEYS[1], ttl)",
    "  redis.call('EXPIRE', KEYS[2], ttl)",
    "end",
    "",
    "return {1, from, redis.call('HGETALL', KEYS[1])}",
].join('\n');

// Release a lease only if we still own it — stops a slow job from releasing
// the lease belonging to the job that replaced it.
const LEASE_RELEASE_LUA = [
    "if redis.call('GET', KEYS[1]) == ARGV[1] then",
    "  return redis.call('DEL', KEYS[1])",
    "end",
    "return 0",
].join('\n');

const LEASE_RENEW_LUA = [
    "if redis.call('GET', KEYS[1]) == ARGV[1] then",
    "  return redis.call('PEXPIRE', KEYS[1], ARGV[2])",
    "end",
    "return 0",
].join('\n');

function init(client) {
    redis = client;
    if (!redis) { warn('No Redis client — store disabled, service runs in-memory only.'); return; }
    redis.defineCommand('ftpTransition',   { numberOfKeys: 14, lua: TRANSITION_LUA });
    redis.defineCommand('ftpLeaseRelease', { numberOfKeys: 1,  lua: LEASE_RELEASE_LUA });
    redis.defineCommand('ftpLeaseRenew',   { numberOfKeys: 1,  lua: LEASE_RENEW_LUA });
    log(`ready — prefix:${PREFIX} mirror:${MIRROR_HASH} ttl:${TTL_SECONDS}s`);
}

function enabled() { return !!redis; }

// ── Helpers ──────────────────────────────────────────────────────────────────
function today() {
    return new Date().toISOString().slice(0, 10).replace(/-/g, '');
}

function decode(hash) {
    if (!hash) return null;
    const rec = { ...hash };
    for (const f of NUMERIC_FIELDS) {
        if (rec[f] !== undefined && rec[f] !== '') rec[f] = Number(rec[f]);
    }
    return rec;
}

function flatToObject(flat) {
    if (!Array.isArray(flat)) return null;
    const out = {};
    for (let i = 0; i < flat.length; i += 2) out[flat[i]] = flat[i + 1];
    return Object.keys(out).length ? out : null;
}

// Redis hashes have no null, so a patch is split into fields to write and
// fields to remove.
//
// Values are stringified here rather than in Lua: Lua 5.1's tostring() renders
// numbers with %.14g, which would turn a large fileSize into "1.2345e+15".
function splitPatch(patch) {
    const set = {};
    const del = [];
    for (const [k, v] of Object.entries(patch || {})) {
        if (v === null || v === undefined) del.push(k);
        else set[k] = typeof v === 'object' ? JSON.stringify(v) : String(v);
    }
    return { set, del };
}

function isRetryable(status) {
    return ['failed', 'no_files', 'partial', 'interrupted'].includes(status);
}

// ── Legacy mirror ────────────────────────────────────────────────────────────
// Same JSON shape the scheduler reads today, plus a few extra fields it can
// start using whenever it is ready.
async function mirrorRecord(folder, rec) {
    if (!redis || !rec) return;
    try {
        const payload = {
            requestId:      rec.requestId || null,
            phone:          rec.phone || null,
            ch:             rec.ch != null ? Number(rec.ch) : null,
            startTime:      rec.startTime || null,
            endTime:        rec.endTime || null,
            folder,
            streamType:     rec.streamType != null ? Number(rec.streamType) : null,
            quality:        rec.quality || null,
            alarmFlag:      rec.alarmFlag || null,
            status:         MIRROR_STATUS[rec.status] || rec.status || null,
            queuePosition:  rec.queuePosition != null ? Number(rec.queuePosition) : null,
            blobUrl:        rec.blobUrl || null,
            blobPath:       rec.blobPath || null,
            filename:       rec.filename || null,
            fileSize:       rec.fileSize != null ? Number(rec.fileSize) : null,
            createdAt:      rec.createdAt || null,
            updatedAt:      rec.updatedAt || null,
            error:          rec.error || null,
            // Extra detail — safe to ignore, useful once the scheduler wants it.
            internalStatus: rec.status || null,
            attempts:       rec.attempts != null ? Number(rec.attempts) : 0,
            retryable:      isRetryable(rec.status),
        };
        await redis.hset(K.mirror(), folder, JSON.stringify(payload));
    } catch (e) {
        err('mirror write failed:', e.message);
    }
}

// ── Core API ─────────────────────────────────────────────────────────────────

// Atomically claim a folder. Returns true only for the caller that created it,
// so two concurrent requests for the same video can never both start.
async function createJob(folder, record) {
    if (!redis) return true;
    const nowIso = new Date().toISOString();
    const won    = await redis.hsetnx(K.job(folder), 'createdAt', record.createdAt || nowIso);
    if (!won) return false;

    const { set } = splitPatch({
        ...record,
        folder,
        createdAtMs: Date.now(),
        attempts:    record.attempts != null ? record.attempts : 0,
        status:      'queued',
        updatedAt:   nowIso,
    });
    await redis.hset(K.job(folder), set);
    await transition(folder, 'queued', { reason: 'created', phone: record.phone });
    return true;
}

async function transition(folder, to, opts = {}) {
    if (!redis) return null;
    if (!STATES.includes(to)) throw new Error(`Unknown state: ${to}`);

    const phone = opts.phone || (await redis.hget(K.job(folder), 'phone')) || '';
    const { set, del } = splitPatch(opts.patch);
    const nowIso  = new Date().toISOString();
    const retryAt = opts.retryAt != null ? opts.retryAt : -1;

    try {
        const [ok, from, flat] = await redis.ftpTransition(
            K.job(folder), K.log(folder), K.retry(), K.idxReq(),
            K.idxPhone(phone || '_unknown'), K.stats(today()),
            ...STATES.map(s => K.state(s)),
            folder, to, nowIso, String(Date.now()), String(TTL_SECONDS),
            JSON.stringify(set), JSON.stringify(del), String(retryAt),
            opts.expectFrom || '', opts.reason || '', phone || '',
        );
        if (!ok) {
            warn(`transition ${folder} → ${to} skipped (current:${from})`);
            return null;
        }
        const rec = decode(flatToObject(flat));
        await mirrorRecord(folder, rec);
        log(`${folder}: ${from || 'none'} → ${to}${opts.reason ? ` (${opts.reason})` : ''}`);
        return rec;
    } catch (e) {
        err(`transition ${folder} → ${to} failed:`, e.message);
        return null;
    }
}

// Which of these folders already have a job record. One pipelined round trip
// instead of one per folder — matters when sweeping a large legacy hash.
async function existingJobs(folders) {
    const out = new Set();
    if (!redis || !folders.length) return out;
    try {
        const p = redis.pipeline();
        folders.forEach(f => p.exists(K.job(f)));
        const res = await p.exec();
        folders.forEach((f, i) => { if (res[i] && res[i][1]) out.add(f); });
    } catch (e) { err('existingJobs:', e.message); }
    return out;
}

async function getJob(folder) {
    if (!redis) return null;
    try {
        const h = await redis.hgetall(K.job(folder));
        return h && Object.keys(h).length ? decode(h) : null;
    } catch (e) { err('getJob:', e.message); return null; }
}

async function deleteJob(folder) {
    if (!redis) return;
    try {
        const rec = await getJob(folder);
        const p = redis.pipeline();
        p.del(K.job(folder), K.log(folder));
        STATES.forEach(s => p.srem(K.state(s), folder));
        p.zrem(K.retry(), folder);
        p.hdel(K.mirror(), folder);
        if (rec && rec.requestId) p.hdel(K.idxReq(), rec.requestId);
        if (rec && rec.phone)     p.zrem(K.idxPhone(rec.phone), folder);
        await p.exec();
        log(`deleted ${folder}`);
    } catch (e) { err('deleteJob:', e.message); }
}

async function listState(state) {
    if (!redis) return [];
    try { return await redis.smembers(K.state(state)); }
    catch (e) { return []; }
}

async function jobsInState(state) {
    const folders = await listState(state);
    return (await Promise.all(folders.map(getJob))).filter(Boolean);
}

async function countsByState() {
    if (!redis) return {};
    const p = redis.pipeline();
    STATES.forEach(s => p.scard(K.state(s)));
    const res = await p.exec();
    const out = {};
    STATES.forEach((s, i) => { out[s] = (res[i] && res[i][1]) || 0; });
    return out;
}

// ── Retry schedule ───────────────────────────────────────────────────────────
async function dueRetries(now = Date.now(), limit = 50) {
    if (!redis) return [];
    try { return await redis.zrangebyscore(K.retry(), '-inf', now, 'LIMIT', 0, limit); }
    catch (e) { return []; }
}

async function clearRetry(folder) {
    if (!redis) return;
    try { await redis.zrem(K.retry(), folder); } catch (_) {}
}

// ── Durable per-phone queue ──────────────────────────────────────────────────
async function queuePush(phone, folder) {
    if (!redis) return;
    try {
        await redis.lrem(K.queue(phone), 0, folder);   // never queue the same folder twice
        await redis.rpush(K.queue(phone), folder);
    } catch (e) { err('queuePush:', e.message); }
}

// Put a folder back at the head — used when a job is popped but cannot start,
// so a batch keeps its original order instead of being shuffled to the back.
async function queueUnshift(phone, folder) {
    if (!redis) return;
    try {
        await redis.lrem(K.queue(phone), 0, folder);
        await redis.lpush(K.queue(phone), folder);
    } catch (e) { err('queueUnshift:', e.message); }
}

async function queuePop(phone) {
    if (!redis) return null;
    try { return await redis.lpop(K.queue(phone)); }
    catch (e) { return null; }
}

async function queueList(phone) {
    if (!redis) return [];
    try { return await redis.lrange(K.queue(phone), 0, -1); }
    catch (e) { return []; }
}

async function queueRemove(phone, folder) {
    if (!redis) return;
    try { await redis.lrem(K.queue(phone), 0, folder); } catch (_) {}
}

async function queuedPhones() {
    if (!redis) return [];
    try {
        const keys = await redis.keys(PREFIX + ':queue:*');
        return keys.map(k => k.split(':').pop());
    } catch (e) { return []; }
}

// ── Active-job lease ─────────────────────────────────────────────────────────
// The lease is the watchdog. While a job is alive we keep pushing the expiry
// out; if the process dies or the job wedges, the key simply vanishes and the
// reconciler sees the job is orphaned — no stale `in_progress` forever.
async function leaseAcquire(phone, folder, ttlMs) {
    if (!redis) return true;
    try {
        const ok = await redis.set(K.active(phone), folder, 'PX', ttlMs, 'NX');
        return ok === 'OK';
    } catch (e) { return false; }
}

async function leaseRenew(phone, folder, ttlMs) {
    if (!redis) return true;
    try { return (await redis.ftpLeaseRenew(K.active(phone), folder, String(ttlMs))) === 1; }
    catch (e) { return false; }
}

async function leaseRelease(phone, folder) {
    if (!redis) return;
    try { await redis.ftpLeaseRelease(K.active(phone), folder); } catch (_) {}
}

async function leaseOwner(phone) {
    if (!redis) return null;
    try { return await redis.get(K.active(phone)); }
    catch (e) { return null; }
}

// ── Indexed lookups (replace the old full-hash scans) ────────────────────────
async function findByRequestId(requestId) {
    if (!redis) return null;
    try {
        const folder = await redis.hget(K.idxReq(), requestId);
        return folder ? await getJob(folder) : null;
    } catch (e) { return null; }
}

async function historyByPhone(phone, limit = 50) {
    if (!redis) return [];
    try {
        const folders = await redis.zrevrange(K.idxPhone(phone), 0, limit - 1);
        return (await Promise.all(folders.map(getJob))).filter(Boolean);
    } catch (e) { return []; }
}

async function latestByPhone(phone) {
    const list = await historyByPhone(phone, 1);
    return list[0] || null;
}

async function getLog(folder, limit = 50) {
    if (!redis) return [];
    try {
        const raw = await redis.lrange(K.log(folder), 0, limit - 1);
        return raw.map(r => { try { return JSON.parse(r); } catch (_) { return null; } }).filter(Boolean);
    } catch (e) { return []; }
}

async function allJobs() {
    if (!redis) return {};
    const out = {};
    for (const s of STATES) {
        for (const rec of await jobsInState(s)) out[rec.folder] = rec;
    }
    return out;
}

async function stats(day = today()) {
    if (!redis) return {};
    try { return await redis.hgetall(K.stats(day)); }
    catch (e) { return {}; }
}

module.exports = {
    init, enabled, K, STATES, MIRROR_STATUS, isRetryable,
    createJob, transition, getJob, existingJobs, deleteJob,
    listState, jobsInState, countsByState,
    dueRetries, clearRetry,
    queuePush, queuePop, queueUnshift, queueList, queueRemove, queuedPhones,
    leaseAcquire, leaseRenew, leaseRelease, leaseOwner,
    findByRequestId, historyByPhone, latestByPhone, getLog, allJobs, stats,
};
