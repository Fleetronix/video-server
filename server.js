'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// server.js  —  single entry point
//
// Loads both services in the SAME process so device-bus.js EventEmitter
// is shared in memory between them.
//
// Usage:
//   node server.js
//   pm2 start server.js --name pictor
// ─────────────────────────────────────────────────────────────────────────────

require('./ftp-service');
require('./index-pictor');