'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// index-pictor.js  —  Pictor device receiver
//
// Thin wrapper around receiver-factory.js. All JT/T1078 parsing, FFmpeg/HLS
// pipeline, and HTTP/WS serving live in the shared factory — this file only
// supplies Pictor's ports and env var names.
//
// Env vars (all optional, fall back to the defaults below):
//   TCP_PORT, HTTP_PORT, WS_PORT, SERVER_IP, FFMPEG_PATH, STREAM_CH, MAX_BUF, WATCHDOG_MS
// ─────────────────────────────────────────────────────────────────────────────

require('dotenv').config();
const createReceiver = require('./receiver-factory');

if (process.env.PICTOR_ENABLED === 'false') {
    console.log('[Pictor] Disabled via PICTOR_ENABLED=false — not starting.');
} else {
    createReceiver({
        vendor:         'pictor',
        tcpPort:        process.env.TCP_PORT    || '3007',
        httpPort:       process.env.HTTP_PORT   || '8080',
        wsPort:         process.env.WS_PORT     || '8801',
        serverIp:       process.env.SERVER_IP,
        ffmpegPath:     process.env.FFMPEG_PATH,
        streamChannel:  process.env.STREAM_CH,
        maxBufferBytes: process.env.MAX_BUF,
        watchdogMs:     process.env.WATCHDOG_MS,
        publicDir:      './public',   // unchanged — preserves existing stream URLs
    });
}