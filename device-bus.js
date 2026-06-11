'use strict';

/**
 * device-bus.js
 *
 * Shared event bus between index-pictor.js and ftp-service.js.
 * Neither module imports the other — they only talk through this bus.
 *
 * Events emitted by index-pictor.js:
 *   'device:connected'    { phone, socket }
 *   'device:disconnected' { phone }
 *   'device:message'      { msgId, body, seq, phone, socket }
 *
 * Events emitted by ftp-service.js:
 *   'device:send'         { phone, frame }   ← index-pictor writes this to socket
 */

const EventEmitter = require('events');
const bus = new EventEmitter();
bus.setMaxListeners(20);

module.exports = bus;