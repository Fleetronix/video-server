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
//        - downloads  <EVENT_API_BASE_URL><aviPath>   (e.g. .../workoss/adas/....mp4)
//        - uploads it to Azure Blob, in its own "events" container
//        - upserts the full alarm row + blob info into MongoDB (EventAlarm collection),
//          keyed on alarmId so re-running the same query doesn't duplicate rows
//   3. Responds with a summary of what was uploaded / saved / failed.
//
// .env
//   EVENT_API_BASE_URL      = https://y.gpstracktech.com
//   EVENT_API_KEY           = 19794beb-452c-4371-9682-811f69843c9b
//   EVENT_API_PATH          = /api//alarm/recentlyAdasList   (upstream really does use a double slash)
//   MONGO_URI                = mongodb://localhost:27017/pictor
//   AZURE_STORAGE_CONNECTION_STRING = <same string ftp-service.js uses, or a dedicated one>
//   AZURE_EVENTS_CONTAINER   = events
//
// Wiring — add to ftp-service.js's HTTP dispatcher (near the other /api/* routes):
//
//   const { handleEventDownload } = require('./event-download');
//   ...
//   if (req.method === 'POST' && urlPath === '/api/event-download') {
//       return handleEventDownload(req, res);
//   }
// ─────────────────────────────────────────────────────────────────────────────

require('dotenv').config();
const https               = require('https');
const mongoose             = require('mongoose');   // npm install mongoose
const { BlobServiceClient } = require('@azure/storage-blob');

// ── Config ────────────────────────────────────────────────────────────────────
const EVENT_API_BASE_URL = process.env.EVENT_API_BASE_URL || 'https://y.gpstracktech.com';
const EVENT_API_KEY      = process.env.EVENT_API_KEY      || '';
const EVENT_API_PATH     = process.env.EVENT_API_PATH     || '/api//alarm/recentlyAdasList';

const MONGO_URI          = process.env.MONGO_URI          || null;

const AZURE_CONN_STRING      = process.env.AZURE_STORAGE_CONNECTION_STRING || null;
const AZURE_EVENTS_CONTAINER = process.env.AZURE_EVENTS_CONTAINER          || 'events';

const log  = (...a) => console.log ('[EVENT-DL]', ...a);
const warn = (...a) => console.warn('[EVENT-DL]', ...a);
const err  = (...a) => console.error('[EVENT-DL]', ...a);

// ── MongoDB (Mongoose) ──────────────────────────────────────────────────────────
let mongoReady = false;

async function ensureMongo() {
    if (mongoReady || mongoose.connection.readyState === 1) { mongoReady = true; return; }
    if (!MONGO_URI) {
        warn('MONGO_URI not set — event records will NOT be saved to MongoDB.');
        return;
    }
    await mongoose.connect(MONGO_URI);
    mongoReady = true;
    log('✅ MongoDB connected');
}

// ── Schema ───────────────────────────────────────────────────────────────────
// One document per alarm row returned by the upstream API, plus what we did with it.
const eventAlarmSchema = new mongoose.Schema({
    alarmId:      { type: String, required: true, unique: true, index: true },
    vehicleId:    Number,
    deviceId:     String,
    deviceName:   String,
    group:        String,
    alarmType:    Number,
    alarmName:    String,
    alarmTime:    String,   // kept as the upstream "YYYY-MM-DD HH:mm:ss" string
    mediaType:    mongoose.Schema.Types.Mixed,
    filePath:     mongoose.Schema.Types.Mixed,
    aviPath:      String,
    imagePath:    String,
    deviceType:   String,
    duration:     String,
    driverName:   String,
    speed:        String,
    lon:          String,
    lat:          String,
    detail:       String,
    driverImg:    String,
    maxSimilar:   Number,

    // ── Added by this pipeline ──────────────────────────────────────────────
    sourceUrl:    String,   // full URL the video was downloaded from
    blobUrl:      String,   // Azure Blob URL after upload
    blobPath:     String,   // path inside the container
    uploadStatus: { type: String, enum: ['pending', 'uploaded', 'failed', 'skipped'], default: 'pending' },
    uploadError:  String,

    // Which request produced this row (handy for auditing / re-query)
    queryContext: {
        ids:         [Number],
        startTime:   String,
        endTime:     String,
        queryType:   String,
        queryParams: [String],
    },
}, { timestamps: true });

const EventAlarm = mongoose.models.EventAlarm || mongoose.model('EventAlarm', eventAlarmSchema);

// ── Azure Blob — own container, own client (self-contained file) ───────────────
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

// ── Call the upstream ADAS alarm list API ────────────────────────────────────
async function fetchRecentlyAdasList(queryBody) {
    const fullUrl = EVENT_API_BASE_URL + EVENT_API_PATH;
    return postJson(fullUrl, {
        'key':             EVENT_API_KEY,
        'Accept-Language': 'en',
        'version':         '1.0',
        'Content-Type':    'application/json',
    }, queryBody);
}

// ── Download one alarm's video and push it to Azure Blob ───────────────────────
async function uploadAlarmVideo(alarm) {
    const client = await ensureAzureContainer();
    if (!client) {
        return { uploadStatus: 'skipped', uploadError: 'Azure Blob not configured' };
    }
    if (!alarm.aviPath) {
        return { uploadStatus: 'skipped', uploadError: 'No aviPath in response row' };
    }

    const sourceUrl = `${EVENT_API_BASE_URL}${alarm.aviPath}`;
    const blobPath  = `${alarm.deviceId || 'unknown'}/${alarm.alarmId}.mp4`;

    try {
        const fileStream      = await fetchStream(sourceUrl, { 'key': EVENT_API_KEY });
        const blockBlobClient = client.getBlockBlobClient(blobPath);

        await blockBlobClient.uploadStream(fileStream, 4 * 1024 * 1024, 5, {
            blobHTTPHeaders: { blobContentType: 'video/mp4' },
        });

        log(`✅ Uploaded ${alarm.alarmId} → ${blobPath}`);
        return { uploadStatus: 'uploaded', blobUrl: blockBlobClient.url, blobPath, sourceUrl };
    } catch (e) {
        err(`Upload failed for ${alarm.alarmId}:`, e.message);
        return { uploadStatus: 'failed', uploadError: e.message, sourceUrl, blobPath };
    }
}

// ── Save one alarm row (+ upload result) into MongoDB ───────────────────────────
async function saveAlarmRecord(alarm, uploadResult, queryContext) {
    if (!MONGO_URI) return null;
    await ensureMongo();
    if (mongoose.connection.readyState !== 1) return null;   // Mongo unreachable — don't crash the request

    return EventAlarm.findOneAndUpdate(
        { alarmId: alarm.alarmId },
        {
            $set: {
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

                sourceUrl:    uploadResult.sourceUrl,
                blobUrl:      uploadResult.blobUrl,
                blobPath:     uploadResult.blobPath,
                uploadStatus: uploadResult.uploadStatus,
                uploadError:  uploadResult.uploadError,

                queryContext,
            },
        },
        { upsert: true, new: true, setDefaultsOnInsert: true }
    );
}

// ── Full pipeline: fetch list → download+upload each → save each ────────────────
async function processEventDownload(queryBody) {
    const upstream = await fetchRecentlyAdasList(queryBody);

    if (upstream.code !== 200) {
        throw new Error(`Upstream error: ${upstream.msg || upstream.code}`);
    }

    const rows = upstream.data || [];
    const queryContext = {
        ids:         queryBody.ids,
        startTime:   queryBody.startTime,
        endTime:     queryBody.endTime,
        queryType:   queryBody.queryType,
        queryParams: queryBody.queryParams,
    };

    const settled = await Promise.allSettled(rows.map(async (alarm) => {
        const uploadResult = await uploadAlarmVideo(alarm);
        const saved         = await saveAlarmRecord(alarm, uploadResult, queryContext);
        return {
            alarmId:      alarm.alarmId,
            deviceId:     alarm.deviceId,
            uploadStatus: uploadResult.uploadStatus,
            uploadError:  uploadResult.uploadError,
            blobUrl:      uploadResult.blobUrl,
            savedToMongo: !!saved,
        };
    }));

    const results = settled.map((r, i) =>
        r.status === 'fulfilled' ? r.value : { alarmId: rows[i]?.alarmId, error: r.reason.message }
    );

    return {
        total:     upstream.total,          // upstream's total across all pages
        fetched:   rows.length,             // rows returned on this page
        processed: results.length,
        uploaded:  results.filter(r => r.uploadStatus === 'uploaded').length,
        failed:    results.filter(r => r.uploadStatus === 'failed' || r.error).length,
        results,
    };
}

// ── HTTP handler — POST /api/event-download ─────────────────────────────────────
function handleEventDownload(req, res) {
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

module.exports = { handleEventDownload, processEventDownload, EventAlarm };