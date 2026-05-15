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

// ── Internal state ─────────────────────────────────────────────────────────────
let client        = null;
let connected     = false;
let reconnecting  = false;
let sendQueue     = [];   // buffered while disconnected

// ── Connection management ──────────────────────────────────────────────────────
function connect() {
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
    const phone = `10000${gpsRecord.phone}`;
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

    console.log(`[TCPForwarder] → GPS: ${csv.trim()}`);
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
    if (!connected) {
        console.warn(`[TCPForwarder] Not connected — queuing (queue length: ${sendQueue.length + 1})`);
        sendQueue.push(bufferOrString);
        return;
    }
    _write(bufferOrString);
}

// ── Boot ───────────────────────────────────────────────────────────────────────
connect();

module.exports = { sendGpsRecord, sendSignallingPacket };