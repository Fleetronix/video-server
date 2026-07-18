'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// event-download.js  —  EVENT/ADAS ALARM DOWNLOAD SERVICE
//
// HTTP API
//   POST /api/event-download
//   body: {
//     "ids": [326],
//     "pageNumber": 1,
//     "pageSize": 35,
//     "startTime": "2026-07-09 04:50:00",
//     "endTime":   "2026-07-14 11:59:59",
//     "queryType": "1",
//     "queryParams": ["15760064474", "15760064716"]
//   }
//
// What it does, per request:
//   1. POSTs that body to the upstream "recentlyAdasList" API.
//   2. For every row in the response that has an aviPath:
//        - if we've already uploaded this alarmId's video before (Mongo says
//          uploadStatus:'uploaded' and has a blobUrl) → SKIP the Azure upload
//          entirely. Calling this API again with the same data will not
//          create duplicate blobs.
//        - otherwise downloads <EVENT_API_BASE_URL><aviPath> and uploads it
//          to Azure Blob, retrying up to UPLOAD_MAX_RETRIES times with
//          backoff on failure.
//        - upserts the full alarm row + upload result into MongoDB, keyed on
//          alarmId (see event-alarm-model.js). A row that failed every retry
//          is saved with uploadStatus:'failed' — the *next* time this API is
//          called with the same alarm in range, it will be retried again
//          automatically, since only 'uploaded' rows are skipped.
//   3. Responds with a summary of what was uploaded / skipped / failed / saved.
//
// Mongo connection lives in mongo.js, the schema lives in event-alarm-model.js
// — this file only holds the upstream call, the upload pipeline, and the
// HTTP route.
//
// .env
//   EVENT_API_BASE_URL      = https://y.gpstracktech.com
//   EVENT_API_KEY           = 19794beb-452c-4371-9682-811f69843c9b
//   EVENT_API_PATH          = /api//alarm/recentlyAdasList   (upstream really uses a double slash)
//   AZURE_STORAGE_CONNECTION_STRING = <same string ftp-service.js uses, or a dedicated one>
//   AZURE_EVENTS_CONTAINER   = events
//   UPLOAD_MAX_RETRIES       = 3
//   UPLOAD_RETRY_DELAY_MS    = 1500
//
// Wiring — in ftp-service.js's HTTP dispatcher:
//   const { handleEventDownload } = require('./event-download');
//   if (req.method === 'POST' && urlPath === '/api/event-download') {
//       return handleEventDownload(req, res);
//   }
// ─────────────────────────────────────────────────────────────────────────────

require('dotenv').config();
const https                 = require('https');
const { BlobServiceClient } = require('@azure/storage-blob');
const { connectMongo }      = require('./mongo');
const EventAlarm            = require('./event-alarm-model');

// ── Config ────────────────────────────────────────────────────────────────────
const EVENT_API_BASE_URL = process.env.EVENT_API_BASE_URL || 'https://y.gpstracktech.com';
const EVENT_API_KEY      = process.env.EVENT_API_KEY      || '';
const EVENT_API_PATH     = process.env.EVENT_API_PATH     || '/api/alarm/recentlyAdasList';

const AZURE_CONN_STRING      = process.env.AZURE_STORAGE_CONNECTION_STRING || null;
const AZURE_EVENTS_CONTAINER = process.env.AZURE_EVENTS_CONTAINER          || 'events';

const UPLOAD_MAX_RETRIES    = parseInt(process.env.UPLOAD_MAX_RETRIES    || '3', 10);
const UPLOAD_RETRY_DELAY_MS = parseInt(process.env.UPLOAD_RETRY_DELAY_MS || '1500', 10);

const log  = (...a) => console.log ('[EVENT-DL]', ...a);
const warn = (...a) => console.warn('[EVENT-DL]', ...a);
const err  = (...a) => console.error('[EVENT-DL]', ...a);

const sleep = (ms) => new Promise(res => setTimeout(res, ms));

// ── Azure Blob — own container, own client ──────────────────────────────────
let containerClient = null;

async function ensureAzureContainer() {
    if (containerClient) return containerClient;
    if (!AZURE_CONN_STRING) {
        warn('AZURE_STORAGE_CONNECTION_STRING not set — event videos will NOT be uploaded.');
        return null;
    }
    const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_CONN_STRING);
    containerClient = blobServiceClient.getContainerClient(AZURE_EVENTS_CONTAINER);
    await containerClient.createIfNotExists();
    log(`✅ Azure Blob ready — container: ${AZURE_EVENTS_CONTAINER}`);
    return containerClient;
}

// ── Small HTTPS JSON POST helper (no extra deps beyond core 'https') ───────────
function postJson(fullUrl, headers, bodyObj) {
    return new Promise((resolve, reject) => {
        const url     = new URL(fullUrl);
        const payload = Buffer.from(JSON.stringify(bodyObj), 'utf8');

        const req = https.request({
            hostname: url.hostname,
            path:     url.pathname + url.search,
            method:   'POST',
            headers:  { ...headers, 'Content-Length': payload.length },
        }, (res) => {
            const chunks = [];
            res.on('data', c => chunks.push(c));
            res.on('end', () => {
                const raw = Buffer.concat(chunks).toString('utf8');
                if (res.statusCode < 200 || res.statusCode >= 300) {
                    return reject(new Error(`Upstream ${res.statusCode}: ${raw.slice(0, 300)}`));
                }
                try { resolve(JSON.parse(raw)); }
                catch (e) { reject(new Error(`Bad JSON from upstream: ${e.message}`)); }
            });
        });

        req.on('error', reject);
        req.write(payload);
        req.end();
    });
}

// Fetch a file as a readable stream, following redirects (blob/CDN hosts often 302).
function fetchStream(fullUrl, headers, maxRedirects = 3) {
    return new Promise((resolve, reject) => {
        const url = new URL(fullUrl);
        const req = https.request({
            hostname: url.hostname,
            path:     url.pathname + url.search,
            method:   'GET',
            headers,
        }, (res) => {
            if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location && maxRedirects > 0) {
                res.resume();
                return resolve(fetchStream(new URL(res.headers.location, fullUrl).toString(), headers, maxRedirects - 1));
            }
            if (res.statusCode < 200 || res.statusCode >= 300) {
                res.resume();
                return reject(new Error(`File fetch ${res.statusCode} for ${fullUrl}`));
            }
            resolve(res); // IncomingMessage is a readable stream
        });
        req.on('error', reject);
        req.end();
    });
}

// ── Call the upstream ADAS alarm list API — one page ─────────────────────────
async function fetchRecentlyAdasListPage(queryBody) {
    console.log("fetchRecentlyAdasListPage queryBody--------", queryBody);
    const fullUrl = EVENT_API_BASE_URL + EVENT_API_PATH;
    console.log("fetchRecentlyAdasListPage fullUrl--------", fullUrl)
    return postJson(fullUrl, {
        'key':             EVENT_API_KEY,
        'Accept-Language': 'en',
        'version':         '1.0',
        'Content-Type':    'application/json',
    }, queryBody);
}

// ── Call the upstream API repeatedly until every page has been fetched ───────
const MAX_PAGES = parseInt(process.env.EVENT_MAX_PAGES || '50', 10);   // safety cap

async function fetchAllAdasPages(queryBody) {
    const pageSize = queryBody.pageSize || 35;
    let pageNumber = queryBody.pageNumber || 1;

    let allRows = [];
    let total   = null;
    let msg     = 'Success';
    console.log("...queryBody.....",  JSON.stringify(queryBody))
    for (let page = 0; page < MAX_PAGES; page++) {
        const resp = await fetchRecentlyAdasListPage({ ...queryBody, pageNumber, pageSize });
        console.log("resp------------->>>>>>", resp)
        if (resp.code !== 200) {
            throw new Error(`Upstream error: ${resp.msg || resp.code}`);
        }

        total = resp.total;
        msg   = resp.msg;
        const rows = resp.data || [];
        allRows = allRows.concat(rows);

        log(`Fetched page ${pageNumber} — ${rows.length} rows (${allRows.length}/${total} so far)`);

        // Stop once we've collected everything the upstream says exists,
        // or once a page comes back short/empty (nothing more to fetch).
        if (rows.length === 0 || allRows.length >= total || rows.length < pageSize) {
            break;
        }
        pageNumber++;
    }

    return { code: 200, msg, total, data: allRows };
}

// ── Download + upload one alarm's video, with retries ───────────────────────
async function uploadAlarmVideoWithRetry(alarm) {
    const client = await ensureAzureContainer();
    if (!client) {
        return { uploadStatus: 'skipped', uploadError: 'Azure Blob not configured', attempts: 0 };
    }
    if (!alarm.aviPath) {
        return { uploadStatus: 'skipped', uploadError: 'No aviPath in response row', attempts: 0 };
    }

    const sourceUrl = `${EVENT_API_BASE_URL}${alarm.aviPath}`;
    const blobPath  = `${alarm.deviceId || 'unknown'}/${alarm.alarmId}.mp4`;

    let lastError = null;

    for (let attempt = 1; attempt <= UPLOAD_MAX_RETRIES; attempt++) {
        try {
            const fileStream      = await fetchStream(sourceUrl, { 'key': EVENT_API_KEY });
            const blockBlobClient = client.getBlockBlobClient(blobPath);

            await blockBlobClient.uploadStream(fileStream, 4 * 1024 * 1024, 5, {
                blobHTTPHeaders: { blobContentType: 'video/mp4' },
            });

            log(`✅ Uploaded ${alarm.alarmId} → ${blobPath} (attempt ${attempt})`);
            return {
                uploadStatus: 'uploaded',
                blobUrl:      blockBlobClient.url,
                blobPath,
                sourceUrl,
                attempts:     attempt,
            };
        } catch (e) {
            lastError = e;
            warn(`Upload attempt ${attempt}/${UPLOAD_MAX_RETRIES} failed for ${alarm.alarmId}: ${e.message}`);
            if (attempt < UPLOAD_MAX_RETRIES) {
                await sleep(UPLOAD_RETRY_DELAY_MS * attempt);   // linear backoff
            }
        }
    }

    err(`❌ All ${UPLOAD_MAX_RETRIES} upload attempts failed for ${alarm.alarmId}: ${lastError.message}`);
    return {
        uploadStatus: 'failed',
        uploadError:  lastError.message,
        sourceUrl,
        blobPath,
        attempts:     UPLOAD_MAX_RETRIES,
    };
}

// ── Save one alarm row (+ upload result) into MongoDB ───────────────────────────
// Explicit, no silent nulls: returns { saved: boolean, doc, reason }.
async function saveAlarmRecord(alarm, uploadResult, queryContext) {
    const conn = await connectMongo();
    if (!conn) {
        return { saved: false, reason: 'MongoDB not configured' };
    }

    const fields = {
        vehicleId:  alarm.vehicleId,
        deviceId:   alarm.deviceId,
        deviceName: alarm.deviceName,
        group:      alarm.group,
        alarmType:  alarm.alarmType,
        alarmName:  alarm.alarmName,
        alarmTime:  alarm.alarmTime,
        mediaType:  alarm.mediaType,
        filePath:   alarm.filePath,
        aviPath:    alarm.aviPath,
        imagePath:  alarm.imagePath,
        deviceType: alarm.deviceType,
        duration:   alarm.duration,
        driverName: alarm.driverName,
        speed:      alarm.speed,
        lon:        alarm.lon,
        lat:        alarm.lat,
        detail:     alarm.detail,
        driverImg:  alarm.driverImg,
        maxSimilar: alarm.maxSimilar,

        sourceUrl:      uploadResult.sourceUrl,
        blobUrl:        uploadResult.blobUrl,
        blobPath:       uploadResult.blobPath,
        uploadStatus:   uploadResult.uploadStatus,
        uploadError:    uploadResult.uploadError || null,
        lastAttemptAt:  new Date(),
        queryContext,
    };

    try {
        const doc = await EventAlarm.findOneAndUpdate(
            { alarmId: alarm.alarmId },
            {
                $set: fields,
                $setOnInsert: { alarmId: alarm.alarmId },
                $inc: { uploadAttempts: uploadResult.attempts || 0 },
            },
            { upsert: true, returnDocument: 'after', setDefaultsOnInsert: true }
        );
        return { saved: true, doc };
    } catch (e) {
        // Two requests racing on the same brand-new alarmId can hit a
        // duplicate-key error on the upsert — fall back to a plain update.
        if (e.code === 11000) {
            try {
                const doc = await EventAlarm.findOneAndUpdate(
                    { alarmId: alarm.alarmId },
                    { $set: fields, $inc: { uploadAttempts: uploadResult.attempts || 0 } },
                    { returnDocument: 'after' }
                );
                return { saved: true, doc };
            } catch (e2) {
                err(`Mongo save failed for ${alarm.alarmId} (after duplicate-key retry): ${e2.message}`);
                return { saved: false, reason: e2.message };
            }
        }
        err(`Mongo save failed for ${alarm.alarmId}: ${e.message}`);
        return { saved: false, reason: e.message };
    }
}

// ── Process one alarm row: skip if already uploaded, else upload + save ─────────
async function processAlarmRow(alarm, queryContext) {
    if (!alarm.aviPath) {
        const uploadResult = { uploadStatus: 'skipped', uploadError: 'No aviPath in response row', attempts: 0 };
        const { saved, reason } = await saveAlarmRecord(alarm, uploadResult, queryContext);
        return { alarmId: alarm.alarmId, deviceId: alarm.deviceId, uploadStatus: 'skipped', savedToMongo: saved, saveError: reason };
    }

    // ── Duplicate check — don't re-upload to Azure if we already have it ──────
    const conn = await connectMongo();
    if (conn) {
        const existing = await EventAlarm.findOne({ alarmId: alarm.alarmId }).lean();
        if (existing && existing.uploadStatus === 'uploaded' && existing.blobUrl) {
            log(`⏭️  ${alarm.alarmId} already uploaded — skipping (${existing.blobPath})`);
            return {
                alarmId:      alarm.alarmId,
                deviceId:     alarm.deviceId,
                uploadStatus: 'uploaded',
                blobUrl:      existing.blobUrl,
                savedToMongo: true,
                duplicate:    true,
            };
        }
    }

    const uploadResult      = await uploadAlarmVideoWithRetry(alarm);
    const { saved, reason } = await saveAlarmRecord(alarm, uploadResult, queryContext);

    return {
        alarmId:      alarm.alarmId,
        deviceId:     alarm.deviceId,
        uploadStatus: uploadResult.uploadStatus,
        uploadError:  uploadResult.uploadError,
        blobUrl:      uploadResult.blobUrl,
        attempts:     uploadResult.attempts,
        savedToMongo: saved,
        saveError:    reason,
        duplicate:    false,
    };
}

// ── Full pipeline: fetch list → process each row ─────────────────────────────
async function processEventDownload(queryBody) {
    console.log("processEventDownload QUERY BODY", JSON.stringify(queryBody))
    const upstream = await fetchAllAdasPages(queryBody);

    const rows = upstream.data || [];
    const queryContext = {
        ids:         queryBody.ids,
        startTime:   queryBody.startTime,
        endTime:     queryBody.endTime,
        queryType:   queryBody.queryType,
        queryParams: queryBody.queryParams,
    };
    console.log("processEventDownload>>>>>>>>>>>>>>>>>>>>>>>>>", queryContext)
    const settled = await Promise.allSettled(rows.map(alarm => processAlarmRow(alarm, queryContext)));

    const results = settled.map((r, i) =>
        r.status === 'fulfilled' ? r.value : { alarmId: rows[i]?.alarmId, error: r.reason.message }
    );

    return {
        total:            upstream.total,     // upstream's total across all pages
        fetched:          rows.length,        // rows returned on this page
        processed:        results.length,
        uploaded:         results.filter(r => r.uploadStatus === 'uploaded' && !r.duplicate).length,
        skippedDuplicate: results.filter(r => r.duplicate).length,
        failed:           results.filter(r => r.uploadStatus === 'failed' || r.error).length,
        savedToMongo:     results.filter(r => r.savedToMongo).length,
        results,
    };
}

// ── HTTP handler — POST /api/event-download ─────────────────────────────────────
function handleEventDownload(req, res) {
    console.log("handleEventDownload>>>>>>>>>>>", JSON.stringify(req.body));
    let body = '';
    req.on('data', c => body += c);
    req.on('end', async () => {
        try {
            const queryBody = JSON.parse(body || '{}');
            if (!queryBody.ids || !queryBody.startTime || !queryBody.endTime) {
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'ids, startTime, endTime are required' }));
                return;
            }
            const result = await processEventDownload(queryBody);
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(result));
        } catch (e) {
            err('event-download failed:', e.message);
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: e.message }));
        }
    });
}

// ── Filters available on GET /api/event-history/:deviceId ───────────────────
// To add a new filter later, just add one line here — the query-param name
// on the left becomes usable immediately, no other code changes needed.
const HISTORY_FILTERABLE_FIELDS = {
    alarmName:    { field: 'alarmName',    type: 'string' },
    alarmType:    { field: 'alarmType',    type: 'number' },
    vehicleId:    { field: 'vehicleId',    type: 'number' },
    group:        { field: 'group',        type: 'string' },
    deviceType:   { field: 'deviceType',   type: 'string' },
    driverName:   { field: 'driverName',   type: 'string' },
    uploadStatus: { field: 'uploadStatus', type: 'string' },   // pending|uploaded|failed|skipped
};

function buildEventHistoryFilter(deviceId, query) {
    const filter = { deviceId };

    // alarmTime is stored as "YYYY-MM-DD HH:mm:ss", which sorts/compares
    // correctly as a plain string — no Date parsing needed.
    if (query.startTime || query.endTime) {
        filter.alarmTime = {};
        if (query.startTime) filter.alarmTime.$gte = query.startTime;
        if (query.endTime)   filter.alarmTime.$lte = query.endTime;
    }

    for (const [param, { field, type }] of Object.entries(HISTORY_FILTERABLE_FIELDS)) {
        if (query[param] === undefined || query[param] === '') continue;
        filter[field] = type === 'number' ? Number(query[param]) : query[param];
    }

    return filter;
}

// ── HTTP handler — GET /api/event-history/:deviceId ─────────────────────────
// Query params: startTime, endTime, page, pageSize, sort,
// + anything listed in HISTORY_FILTERABLE_FIELDS above (alarmName, alarmType, ...)
async function handleEventHistory(req, res) {
    try {
        const urlObj   = new URL(req.url, 'http://localhost');
        const deviceId = decodeURIComponent(urlObj.pathname.replace('/api/event-history/', '').trim());
        const query    = Object.fromEntries(urlObj.searchParams.entries());

        if (!deviceId) {
            res.writeHead(400, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'deviceId is required, e.g. /api/event-history/15760064474' }));
            return;
        }

        const conn = await connectMongo();
        if (!conn) {
            res.writeHead(503, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: 'MongoDB not configured' }));
            return;
        }

        const page     = Math.max(parseInt(query.page     || '1',  10), 1);
        const pageSize = Math.min(Math.max(parseInt(query.pageSize || '50', 10), 1), 500);
        const sort     = query.sort || '-alarmTime';   // newest first by default

        const filter = buildEventHistoryFilter(deviceId, query);

        const [rows, total] = await Promise.all([
            EventAlarm.find(filter).sort(sort).skip((page - 1) * pageSize).limit(pageSize).lean(),
            EventAlarm.countDocuments(filter),
        ]);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            deviceId,
            filter,
            page,
            pageSize,
            total,
            totalPages: Math.ceil(total / pageSize),
            count: rows.length,
            data: rows,
        }));
    } catch (e) {
        err('event-history failed:', e.message);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
    }
}

module.exports = { handleEventDownload, handleEventHistory, processEventDownload };