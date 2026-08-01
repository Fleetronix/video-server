'use strict';

/**
 * tcp-forwarder.js
 *
 * Forwards GPS records and raw signalling packets to a remote TCP server.
 *
 * Configure via .env:
 *   FORWARD_HOST=192.168.1.100
 *   FORWARD_PORT=9000
 *
 * Usage:
 *   const forwarder = require('./tcp-forwarder');
 *
 *   // Send a GPS record (object → CSV string automatically)
 *   forwarder.sendGpsRecord(gpsRecord);
 *
 *   // Send a raw signalling packet (Buffer)
 *   forwarder.sendSignallingPacket(rawBuffer);
 */

require('dotenv').config();
const net = require('net');

// ── Config from .env ───────────────────────────────────────────────────────────
const FORWARD_HOST    = process.env.FORWARD_HOST || '127.0.0.1';
const FORWARD_PORT    = parseInt(process.env.FORWARD_PORT || '9000', 10);
const RECONNECT_DELAY = parseInt(process.env.FORWARD_RECONNECT_MS || '5000', 10);

// Master on/off switch for the whole forwarder. Set FORWARDER_ENABLED=false in
// .env to disable it completely — no socket connect, no queuing, no sends.
const FORWARDER_ENABLED = (process.env.FORWARDER_ENABLED || 'true').toLowerCase() !== 'false';

// ── Internal state ─────────────────────────────────────────────────────────────
let client        = null;
let connected     = false;
let reconnecting  = false;
let sendQueue     = [];   // buffered while disconnected

// ── Connection management ──────────────────────────────────────────────────────
function connect() {
    if (!FORWARDER_ENABLED) {
        console.log('[TCPForwarder] Disabled via FORWARDER_ENABLED=false — not connecting.');
        return;
    }
    if (reconnecting) return;
    reconnecting = true;

    console.log(`[TCPForwarder] Connecting to ${FORWARD_HOST}:${FORWARD_PORT} …`);

    client = new net.Socket();

    client.connect(FORWARD_PORT, FORWARD_HOST, () => {
        connected    = true;
        reconnecting = false;
        console.log(`[TCPForwarder] ✅ Connected to ${FORWARD_HOST}:${FORWARD_PORT}`);

        // Flush any queued messages
        while (sendQueue.length > 0) {
            const msg = sendQueue.shift();
            _write(msg);
        }
    });

    client.on('data', data => {
        // Remote server responses are logged but not processed
        console.log(`[TCPForwarder] ← received: ${data.toString().trim()}`);
    });

    client.on('close', () => {
        connected    = false;
        reconnecting = false;
        console.warn(`[TCPForwarder] Connection closed. Retrying in ${RECONNECT_DELAY}ms …`);
        setTimeout(connect, RECONNECT_DELAY);
    });

    client.on('error', err => {
        connected    = false;
        reconnecting = false;
        console.error(`[TCPForwarder] Error: ${err.message}. Retrying in ${RECONNECT_DELAY}ms …`);
        client.destroy();
        setTimeout(connect, RECONNECT_DELAY);
    });
}

// ── Internal write ─────────────────────────────────────────────────────────────
function _write(data) {
    try {
        client.write(data);
        console.log(`[TCPForwarder] ✅ Sent ${data.length} bytes to ${FORWARD_HOST}:${FORWARD_PORT}`);
    } catch (err) {
        console.error(`[TCPForwarder] Write failed: ${err.message}`);
    }
}

// ── Public API ─────────────────────────────────────────────────────────────────

/**
 * sendGpsRecord(gpsRecord)
 *
 * Converts the gpsRecord object to a comma-separated string and sends it.
 *
 * Expected field order (matches the format in the requirements):
 *   phone,datetime,latitude,longitude,speed_kmh,direction_deg,elevation_m,
 *   acc,located,mileage,voltage,satellites,signal,sensor_speed,
 *   oil_circuit,vehicle_circuit,door,alarms
 *
 * Example output:
 *   1576064474,2026-05-13 19:49:03,17.437335,78.369083,0,233,579,ON,YES,--,--,--,--,--,NORMAL,NORMAL,CLOSED,NONE
 */
function sendGpsRecord(gpsRecord) {
    const phone = `1000${gpsRecord.phone}`;
    const csv = [
        phone,
        gpsRecord.datetime,
        gpsRecord.latitude,
        gpsRecord.longitude,
        gpsRecord.speed_kmh,
        gpsRecord.direction_deg,
        gpsRecord.elevation_m,
        gpsRecord.acc,
        gpsRecord.located,
        gpsRecord.mileage,
        gpsRecord.voltage,
        gpsRecord.satellites,
        gpsRecord.signal,
        gpsRecord.sensor_speed,
        gpsRecord.oil_circuit,
        gpsRecord.vehicle_circuit,
        gpsRecord.door,
        gpsRecord.alarms,
    ].join(',') + '\n';

    // console.log(`[TCPForwarder] → GPS: ${csv.trim()}`);
    _send(Buffer.from(csv, 'utf8'));
}

/**
 * sendSignallingPacket(rawBuffer)
 *
 * Sends the raw JT/T 808 signalling packet buffer as-is to the remote server.
 *
 * @param {Buffer} rawBuffer  — the full 0x7E…0x7E framed packet
 */
function sendSignallingPacket(rawBuffer) {
    console.log(`[TCPForwarder] → Signalling packet (${rawBuffer.length} bytes)`);
    _send(rawBuffer);
}

/**
 * Internal: queue if not connected, write immediately if connected.
 */
function _send(bufferOrString) {
    if (!FORWARDER_ENABLED) {
        // Forwarder turned off — drop instead of queuing forever, but say so.
        console.warn('[TCPForwarder] ⛔ FORWARDER_ENABLED=false — packet dropped, not sent.');
        return;
    }
    if (!connected) {
        console.warn(`[TCPForwarder] Not connected — queuing (queue length: ${sendQueue.length + 1})`);
        sendQueue.push(bufferOrString);
        return;
    }
    _write(bufferOrString);
}

/**
 * sendStreamAlert(phone, gpsSnapshot)
 *
 * Sends a CSV alert line in the SAME shape as a normal GPS record (see
 * sendGpsRecord above), but using the last known GPS values for that phone
 * and 'STREAM_INTRUPPED' in the alarms field instead of the real alarm list.
 * Fired for a video-stream stall, a GPS-interruption, or both — see
 * CONFIG.alertOn in index-pictor.js for which one(s) trigger it.
 *
 * gpsSnapshot: the last object passed to sendGpsRecord() for this phone, or
 *              null/undefined if no GPS has ever been received yet.
 *
 * Example output:
 *   100015760064716,2026-07-13 11:34:50,17.437459,78.368806,0,57,497,ON,YES,0.1,0,24,24,0,NORMAL,NORMAL,CLOSED,STREAM_INTRUPPED
 */
function sendStreamAlert(phone, gpsSnapshot) {
    const paddedPhone = `1000${phone}`;
    const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
    const g = gpsSnapshot || {};

    const csv = [
        paddedPhone,
        now,                          // alert time, not the last GPS fix time
        g.latitude      ?? 0,
        g.longitude     ?? 0,
        g.speed_kmh      ?? 0,
        g.direction_deg  ?? 0,
        g.elevation_m    ?? 0,
        g.acc            ?? 'OFF',
        g.located        ?? 'NO',
        g.mileage        ?? '0',
        g.voltage        ?? '0',
        g.satellites     ?? '0',
        g.signal         ?? '0',
        g.sensor_speed   ?? '0',
        g.oil_circuit    ?? 'NORMAL',
        g.vehicle_circuit ?? 'NORMAL',
        g.door           ?? 'CLOSED',
        'STREAM_INTRUPPED',
    ].join(',') + '\n';

    console.warn(`[TCPForwarder] → ALERT: STREAM_INTRUPPED for ${phone}`);
    _send(Buffer.from(csv, 'utf8'));
}

// ── Boot ───────────────────────────────────────────────────────────────────────
connect();

module.exports = { sendGpsRecord, sendSignallingPacket, sendStreamAlert };