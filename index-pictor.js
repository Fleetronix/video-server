'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// index-acumen.js  —  LIVE STREAM + GPS + FTP DOWNLOAD
//
// KEY CHANGE vs index-pictor.js:
//   REMOVED: FFmpeg pipeline → HLS segments → HTTP polling
//   ADDED:   WebSocket + MSE (Media Source Extensions) direct push
//
// Why the old approach caused "fast forward then stop":
//   Camera → TCP → reassemble → MPEG-TS wrap → FFmpeg stdin pipe →
//   HLS segment files on disk → browser polls every 1s → plays
//
//   Problems:
//   1. FFmpeg probes the pipe before decoding (2-3s startup buffer)
//   2. HLS segments are 1s each → minimum 2-3s latency
//   3. No PTS in old PES header → FFmpeg guesses timing → batch dumps frames
//   4. When FFmpeg falls behind it batches and plays back fast
//
// New approach:
//   Camera → TCP → reassemble frame → read timestamp from protocol header →
//   send raw NAL unit over WebSocket → browser MSE decodes in real time
//
// Browser side uses Broadway.js (JS H.264 decoder) OR MSE depending on codec.
// ─────────────────────────────────────────────────────────────────────────────

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
require('dotenv').config();
const { WebSocketServer } = require('ws');
const tcpForwarder = require('./tcp-forwarder');
const ftpDownload  = require('./ftp-download');

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
    tcpPort:  parseInt(process.env.TCP_PORT  || '3007'),
    httpPort: parseInt(process.env.HTTP_PORT || '8080'),
    wsPort:   parseInt(process.env.WS_PORT   || '8801'),
    serverIp: process.env.SERVER_IP,
};
console.log(`[config] tcpPort:${CONFIG.tcpPort} httpPort:${CONFIG.httpPort} wsPort:${CONFIG.wsPort} serverIp:${CONFIG.serverIp}`);

if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── Device state ──────────────────────────────────────────────────────────────
const tcpSockets = {};   // { [phone]: socket }

// ── Channel state ─────────────────────────────────────────────────────────────
// Each channel tracks subpacket reassembly and whether we've seen an I-frame.
const channels = {};
function getChannel(ch) {
    if (!channels[ch]) channels[ch] = { gotIFrame: false, subpackets: [], lastPts: 0 };
    return channels[ch];
}

// ── WebSocket server ──────────────────────────────────────────────────────────
// Single WS server handles both:
//   - GPS/FTP JSON messages  (binary=false, first byte is '{')
//   - Video frame binary push (binary=true, protocol below)
//
// Video binary frame format sent to browser:
//   [0]      channel (1 byte)
//   [1]      dataType: 0=IFrame 1=PFrame 2=BFrame 3=Audio (1 byte)
//   [2..9]   pts milliseconds (8 bytes, BigInt64BE)
//   [10..]   raw NAL data
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

// Track which WS clients want video for which channel
// ws._videoChannels = Set of channel numbers
wss.on('connection', (ws, req) => {
    console.log(`[WS] Browser connected from ${req.socket.remoteAddress}`);
    ws._videoChannels = new Set([1]); // default: subscribe to ch1

    ws.on('message', raw => {
        // Binary message = video subscription control (future use)
        if (raw instanceof Buffer && raw[0] !== 0x7B) return;

        let msg;
        try { msg = JSON.parse(raw); } catch(e) {
            console.warn('[WS] Non-JSON message ignored');
            return;
        }

        // Video channel subscription: { type:'subscribe', channels:[1,2] }
        if (msg.type === 'subscribe' && Array.isArray(msg.channels)) {
            ws._videoChannels = new Set(msg.channels);
            return;
        }

        ftpDownload.handleWsMessage(msg, ws);
    });

    ws.on('close', () => console.log('[WS] Browser disconnected'));
    ws.on('error', err => console.error('[WS] Error:', err.message));
});

// ── Broadcast helpers ─────────────────────────────────────────────────────────
function broadcastJson(obj) {
    const raw = JSON.stringify(obj);
    for (const ws of wss.clients) {
        if (ws.readyState === 1) ws.send(raw);
    }
}

// Push a video frame to all subscribers of that channel.
// Prepends an 10-byte header so browser knows channel, type, and PTS.
function broadcastVideoFrame(channel, dataType, ptsMs, nalData) {
    if (wss.clients.size === 0) return;

    const header = Buffer.alloc(10);
    header[0] = channel;
    header[1] = dataType;
    // Write pts as 64-bit big-endian (BigInt)
    const ptsBig = BigInt(ptsMs);
    header.writeBigInt64BE(ptsBig, 2);

    const frame = Buffer.concat([header, nalData]);

    for (const ws of wss.clients) {
        if (ws.readyState === 1 && ws._videoChannels && ws._videoChannels.has(channel)) {
            try { ws.send(frame); } catch(e) { /* ignore closed */ }
        }
    }
}

// ── FTP download module init ──────────────────────────────────────────────────
ftpDownload.init({
    serverIp:      CONFIG.serverIp,
    ftpPort:       14992,
    pasvDataPort:  14993,
    recordingsDir: './recordings',
    wss,
    tcpSockets,
    buildFrame,
    buildAck,
});

// ── HTTP server ───────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    const urlPath = req.url.split('?')[0];
    let filePath;

    if (urlPath === '/') {
        filePath = path.join(__dirname, 'video.html');
    } else if (urlPath.startsWith('/recordings/')) {
        filePath = path.join(__dirname, urlPath);
    } else {
        filePath = path.join(__dirname, urlPath);
    }

    const ext = path.extname(filePath).toLowerCase();
    const mime = {
        '.html': 'text/html', '.js': 'application/javascript',
        '.mp4': 'video/mp4',  '.m3u8': 'application/vnd.apple.mpegurl',
        '.ts': 'video/mp2t',
    };

    fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); res.end('Not found'); return; }
        res.writeHead(200, {
            'Content-Type': mime[ext] || 'application/octet-stream',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-cache',
        });
        res.end(data);
    });
}).listen(CONFIG.httpPort, () => console.log(`✓ HTTP on :${CONFIG.httpPort}`));

// ── Subpacket reassembly + frame dispatch ─────────────────────────────────────
// Per JT/T 1078 §5.5.3 Table 19:
//   subpktMarker: 0=atomic, 1=first, 3=middle, 2=last
function processVideoPacket(rawData, channel, dataType, subpktMarker, ptsMs) {
    const ch = getChannel(channel);

    if (subpktMarker === 0) {
        // Atomic packet — whole frame in one packet
        dispatchFrame(rawData, channel, dataType, ptsMs);
    } else if (subpktMarker === 1) {
        // First subpacket — start reassembly
        ch.subpackets = [rawData];
        ch.subPts     = ptsMs;
    } else if (subpktMarker === 3) {
        // Middle subpacket
        if (ch.subpackets.length > 0) ch.subpackets.push(rawData);
    } else if (subpktMarker === 2) {
        // Last subpacket — complete and dispatch
        if (ch.subpackets.length > 0) {
            ch.subpackets.push(rawData);
            const complete = Buffer.concat(ch.subpackets);
            ch.subpackets  = [];
            dispatchFrame(complete, channel, dataType, ch.subPts || ptsMs);
        }
    }
}

function dispatchFrame(frameData, channel, dataType, ptsMs) {
    const ch = getChannel(channel);

    // Only dataTypes 0 (I), 1 (P), 2 (B) are video. 3 = audio, skip for now.
    if (dataType > 2) return;

    // Wait for first I-frame before sending P/B frames
    if (dataType === 0) {
        ch.gotIFrame = true;
    } else if (!ch.gotIFrame) {
        return;
    }

    broadcastVideoFrame(channel, dataType, ptsMs, frameData);
}

// ── JT/T 808 helpers ─────────────────────────────────────────────────────────
function unescapeBuffer(buf) {
    const out = []; let i = 0;
    while (i < buf.length) {
        if      (buf[i] === 0x7D && buf[i+1] === 0x02) { out.push(0x7E); i += 2; }
        else if (buf[i] === 0x7D && buf[i+1] === 0x01) { out.push(0x7D); i += 2; }
        else { out.push(buf[i++]); }
    }
    return Buffer.from(out);
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
    const header = Buffer.alloc(12);
    header.writeUInt16BE(msgId,       0);
    header.writeUInt16BE(body.length, 2);
    const phoneStr = String(phone).padStart(12, '0');
    Buffer.from(
        phoneStr.match(/.{2}/g).map(v => {
            const n = parseInt(v, 10);
            return ((Math.floor(n / 10) << 4) | (n % 10));
        })
    ).copy(header, 4);
    header.writeUInt16BE(Math.floor(Math.random() * 0xFFFF), 10);
    const payload = Buffer.concat([header, body]);
    let cs = 0; payload.forEach(b => cs ^= b);
    return Buffer.concat([
        Buffer.from([0x7E]),
        escapeBuffer(Buffer.concat([payload, Buffer.from([cs])])),
        Buffer.from([0x7E]),
    ]);
}

function buildAck(phone, replySeq, replyMsgId) {
    const body = Buffer.alloc(5);
    body.writeUInt16BE(replySeq,   0);
    body.writeUInt16BE(replyMsgId, 2);
    body[4] = 0;
    return buildFrame(0x8001, body, phone);
}

function buildRegisterResponse(phone, replySeq, result, authCode) {
    const authBuf = Buffer.from(authCode, 'ascii');
    const body    = Buffer.alloc(3 + authBuf.length);
    body.writeUInt16BE(replySeq, 0);
    body[2] = result;
    authBuf.copy(body, 3);
    return buildFrame(0x8100, body, phone);
}

function buildVideoRequest(phone, serverIp, serverPort, channel) {
    const ipBuf = Buffer.from(serverIp, 'ascii');
    const N     = ipBuf.length;
    const body  = Buffer.alloc(8 + N);
    body[0] = N;
    ipBuf.copy(body, 1);
    body.writeUInt16BE(serverPort, 1 + N);
    body.writeUInt16BE(0,          3 + N);  // UDP port = 0 (TCP only)
    body[5 + N] = channel;
    body[6 + N] = 1;   // video only
    body[7 + N] = 1;   // main stream
    return buildFrame(0x9101, body, phone);
}

function parseAdditionalInfo(buf) {
    const result = {};
    let i = 0;
    while (i < buf.length - 2) {
        const id  = buf[i];
        const len = buf[i + 1];
        if (i + 2 + len > buf.length) break;
        const val = buf.slice(i + 2, i + 2 + len);
        switch (id) {
            case 0x01: if (val.length >= 4) result.mileage       = val.readUInt32BE(0) / 10 + ' km';   break;
            case 0x03: if (val.length >= 2) result.sensorSpeed   = val.readUInt16BE(0) / 10 + ' km/h'; break;
            case 0x25: if (val.length >= 2) result.voltage       = val.readUInt16BE(0) / 10 + ' V';    break;
            case 0x30: if (val.length >= 1) result.signalStrength = val[0];                             break;
            case 0x31: if (val.length >= 1) result.satellites    = val[0];                              break;
            case 0xd5: result.imei = val.toString('ascii').replace(/\0/g, '').trim();                   break;
        }
        i += 2 + len;
    }
    return result;
}

// ── TCP server ────────────────────────────────────────────────────────────────
const tcpServer = net.createServer(socket => {
    socket.setNoDelay(true);
    socket.setKeepAlive(true, 10000);

    const remote = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`Device connected: ${remote}`);
    let buffer = Buffer.alloc(0);
    let phone  = null;

    socket.on('data', data => {
        try {
            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length) {

                // ── Stream data packet (JT/T 1078 §5.5.3 — 0x30316364 header) ─
                if (buffer.length - offset >= 4 &&
                    buffer[offset]   === 0x30 && buffer[offset+1] === 0x31 &&
                    buffer[offset+2] === 0x63 && buffer[offset+3] === 0x64) {

                    // Need at least 30 bytes for the full fixed header
                    if (buffer.length - offset < 30) break;

                    const dataBodyLen = buffer.readUInt16BE(offset + 28);

                    // Wait for complete packet
                    if (buffer.length - offset < 30 + dataBodyLen) break;

                    // Parse all header fields per Table 19
                    const channel      = buffer[offset + 14];
                    const byte15       = buffer[offset + 15];
                    const dataType     = (byte15 >> 4) & 0x0F;
                    const subpktMarker = byte15 & 0x0F;

                    // ── Timestamp: 8 bytes at offset+16, in milliseconds ──────
                    // This is the key field that was IGNORED before.
                    // It's the relative time of the current frame — use it for
                    // smooth playback pacing on the browser side.
                    const ptsHigh = buffer.readUInt32BE(offset + 16);
                    const ptsLow  = buffer.readUInt32BE(offset + 20);
                    const ptsMs   = ptsHigh * 0x100000000 + ptsLow;

                    const rawData = buffer.slice(offset + 30, offset + 30 + dataBodyLen);

                    processVideoPacket(rawData, channel, dataType, subpktMarker, ptsMs);

                    // Forward raw signalling to TCP forwarder if needed
                    // (stream packets are NOT forwarded — too much bandwidth)

                    offset += 30 + dataBodyLen;
                    continue;
                }

                // ── Signalling packet (JT/T 808 — 0x7E framing) ──────────────
                if (buffer[offset] === 0x7E) {
                    const end = buffer.indexOf(0x7E, offset + 1);
                    if (end === -1) break;

                    const inner     = buffer.slice(offset + 1, end);
                    const unescaped = unescapeBuffer(inner);
                    if (unescaped.length < 12) { offset = end + 1; continue; }

                    const msgId = unescaped.readUInt16BE(0);
                    phone = Array.from(unescaped.slice(4, 10), b => b.toString(16).padStart(2, '0'))
                                 .join('').replace(/^0/, '');
                    const seq  = unescaped.readUInt16BE(10);
                    const body = unescaped.slice(12);

                    // ── 0x0001: General response ─────────────────────────────
                    if (msgId === 0x0001) {
                        const replyMsgId  = body.readUInt16BE(2);
                        const replyResult = body[4];
                        const resultTxt   = ['Success','Failed','Wrong Msg','Not Supported'][replyResult] || `(${replyResult})`;
                        console.log(`[ACK] 0x${replyMsgId.toString(16).padStart(4,'0')} result:${replyResult} ${resultTxt}`);
                        ftpDownload.handleSignalling(msgId, body, seq, phone, socket);

                    // ── 0x0100: Register ─────────────────────────────────────
                    } else if (msgId === 0x0100) {
                        console.log(`[REG] phone:${phone}`);
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    // ── 0x0102: Auth — start live stream ─────────────────────
                    } else if (msgId === 0x0102) {
                        console.log(`[AUTH] phone:${phone} raw:${unescaped.slice(4,10).toString('hex')}`);
                        socket.write(buildAck(phone, seq, msgId));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
                        tcpSockets[phone] = socket;
                        console.log(`[AUTH] Registered socket for phone:${phone}, requested ch1 stream`);

                    // ── 0x0200: GPS location ─────────────────────────────────
                    } else if (msgId === 0x0200) {
                        socket.write(buildAck(phone, seq, msgId));

                        const alarmFlags = body.readUInt32BE(0);
                        const statusBits = body.readUInt32BE(4);
                        const latRaw     = body.readUInt32BE(8);
                        const lonRaw     = body.readUInt32BE(12);
                        const elevation  = body.readUInt16BE(16);
                        const speed      = body.readUInt16BE(18) / 10;
                        const direction  = body.readUInt16BE(20);

                        const south   = !!(statusBits & (1 << 2));
                        const west    = !!(statusBits & (1 << 3));
                        const lat     = latRaw / 1e6 * (south ? -1 : 1);
                        const lon     = lonRaw / 1e6 * (west  ? -1 : 1);
                        const accOn   = !!(statusBits & (1 << 0));
                        const located = !!(statusBits & (1 << 1));

                        const bcd = b => ((b >> 4) * 10 + (b & 0x0F));
                        const T   = 22;
                        const dt  = `20${String(bcd(body[T])).padStart(2,'0')}-${String(bcd(body[T+1])).padStart(2,'0')}-${String(bcd(body[T+2])).padStart(2,'0')} ${String(bcd(body[T+3])).padStart(2,'0')}:${String(bcd(body[T+4])).padStart(2,'0')}:${String(bcd(body[T+5])).padStart(2,'0')}`;

                        const extra = parseAdditionalInfo(body.slice(27));
                        const alarmList = alarmFlags !== 0 ? [
                            (alarmFlags & (1<<0)) ? 'EMERGENCY'   : null,
                            (alarmFlags & (1<<1)) ? 'OVERSPEED'   : null,
                            (alarmFlags & (1<<4)) ? 'GNSS_FAULT'  : null,
                            (alarmFlags & (1<<5)) ? 'ANTENNA_CUT' : null,
                            (alarmFlags & (1<<7)) ? 'LOW_VOLTAGE' : null,
                            (alarmFlags & (1<<8)) ? 'POWER_OFF'   : null,
                        ].filter(Boolean) : [];
                        const locationMsg = Object.assign(
                            { type: 'location', phone, lat, lon, speed, direction,
                              elevation, datetime: dt, accOn, located, alarms: alarmList,
                              imei: extra.imei || '--' },
                            extra
                        );
                        broadcastJson(locationMsg);

                        const gpsRecord = {
                            phone, datetime: dt, latitude: lat, longitude: lon,
                            speed_kmh: speed, direction_deg: direction, elevation_m: elevation,
                            acc:             accOn   ? 'ON'  : 'OFF',
                            located:         located ? 'YES' : 'NO',
                            mileage:         extra.mileage        || '0',
                            voltage:         extra.voltage        || '0',
                            satellites:      extra.satellites     || '0',
                            signal:          extra.signalStrength || '0',
                            sensor_speed:    extra.sensorSpeed    || '0',
                            oil_circuit:     !!(statusBits & (1<<10)) ? 'CUT'  : 'NORMAL',
                            vehicle_circuit: !!(statusBits & (1<<11)) ? 'CUT'  : 'NORMAL',
                            door:            !!(statusBits & (1<<13)) ? 'OPEN' : 'CLOSED',
                            alarms:          alarmFlags !== 0 ? [
                                (alarmFlags & (1<<0)) ? 'EMERGENCY'   : null,
                                (alarmFlags & (1<<1)) ? 'OVERSPEED'   : null,
                                (alarmFlags & (1<<4)) ? 'GNSS_FAULT'  : null,
                                (alarmFlags & (1<<5)) ? 'ANTENNA_CUT' : null,
                                (alarmFlags & (1<<7)) ? 'LOW_VOLTAGE' : null,
                                (alarmFlags & (1<<8)) ? 'POWER_OFF'   : null,
                            ].filter(Boolean).join('|') : 'NONE',
                        };

                        tcpForwarder.sendGpsRecord(gpsRecord);
                        const logFile = `gps_log_${new Date().toISOString().slice(0,10)}.txt`;
                        fs.appendFile(`./${logFile}`, Object.values(gpsRecord).join(',') + '\n', () => {});

                    // ── 0x1205 / 0x1206: FTP flow ────────────────────────────
                    } else if (msgId === 0x1205) {
                        const totalFiles = body.readUInt32BE(2);
                        console.log(`[0x1205] files:${totalFiles}`);
                        if (totalFiles === 0) console.warn('[0x1205] ⚠ 0 files — check time range');

                    } else if (msgId === 0x1206) {
                        ftpDownload.handleSignalling(msgId, body, seq, phone, socket);

                    } else if (ftpDownload.handleSignalling(msgId, body, seq, phone, socket)) {
                        // handled

                    } else {
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                // Neither 0x30316364 nor 0x7E — skip byte
                offset++;
            }

            buffer = buffer.slice(offset);

        } catch (err) {
            console.error('[TCP] Error:', err.message, err.stack);
        }
    });

    socket.on('close', () => {
        console.log(`Device disconnected: ${remote} phone:${phone}`);
        if (phone) delete tcpSockets[phone];
    });
    socket.on('error', err => console.error(`[TCP] socket error:`, err.message));
});

tcpServer.listen(CONFIG.tcpPort, () => console.log(`✓ TCP server on :${CONFIG.tcpPort}`));