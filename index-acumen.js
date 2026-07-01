'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// index-acumen.js  —  Acumen device receiver
//
// Thin wrapper around receiver-factory.js — same JT/T1078 engine as Pictor,
// listening on its own ports so Acumen devices are configured to point at a
// different TCP address without touching the Pictor fleet.
//
// Env vars (all optional, fall back to the defaults below). Change these in
// .env if Acumen devices need a different port than 3008/8081/8803:
//   ACUMEN_TCP_PORT, ACUMEN_HTTP_PORT, ACUMEN_WS_PORT, ACUMEN_SERVER_IP,
//   ACUMEN_FFMPEG_PATH, ACUMEN_STREAM_CH, ACUMEN_MAX_BUF, ACUMEN_WATCHDOG_MS
// ─────────────────────────────────────────────────────────────────────────────

require('dotenv').config();
const createReceiver = require('./receiver-factory');

if (process.env.ACUMEN_ENABLED === 'false') {
    console.log('[Acumen] Disabled via ACUMEN_ENABLED=false — not starting.');
} else {
    createReceiver({
        vendor:         'acumen',
        tcpPort:        process.env.ACUMEN_TCP_PORT    || '3008',
        httpPort:       process.env.ACUMEN_HTTP_PORT   || '8081',
        wsPort:         process.env.ACUMEN_WS_PORT     || '8803',
        serverIp:       process.env.ACUMEN_SERVER_IP   || process.env.SERVER_IP,
        ffmpegPath:     process.env.ACUMEN_FFMPEG_PATH || process.env.FFMPEG_PATH,
        streamChannel:  process.env.ACUMEN_STREAM_CH,
        maxBufferBytes: process.env.ACUMEN_MAX_BUF,
        watchdogMs:     process.env.ACUMEN_WATCHDOG_MS,
        publicDir:      './public-acumen',   // separate HLS output dir from Pictor
    });
}