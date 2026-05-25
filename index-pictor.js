'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// index-pictor.js  —  LIVE STREAM + GPS  (multi-device)
// Each device gets its own folder:  public/<phone>/
// HLS playlist:                     public/<phone>/<phone>_ch1.m3u8
// Segments:                         public/<phone>/<phone>_ch1_000.ts …
// ─────────────────────────────────────────────────────────────────────────────

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
require('dotenv').config();
const { WebSocketServer } = require('ws');
const { spawn }           = require('child_process');
const tcpForwarder        = require('./tcp-forwarder');

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
    tcpPort:  3007,
    httpPort: 8080,
    wsPort:   8801,
    serverIp: process.env.SERVER_IP,
};
console.log(`Server IP: ${CONFIG.serverIp}`);

// ── Ensure public folder exists ───────────────────────────────────────────────
if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── Device state ──────────────────────────────────────────────────────────────
const tcpSockets = {};  // { [phone]: socket }
const phoneByIp  = {};  // { [ip]: phone }

// devices[phone][channel] = { ffmpeg, gotIFrame, subpackets, patPmtSent, tsCounter }
const devices = {};

function getOrCreateDeviceChannel(phone, channel) {
    if (!devices[phone]) devices[phone] = {};
    if (!devices[phone][channel]) {
        devices[phone][channel] = {
            ffmpeg:     null,
            gotIFrame:  false,
            subpackets: [],
            patPmtSent: false,
            tsCounter:  0,
        };
    }
    return devices[phone][channel];
}

// ── Extract phone number from a T/98 stream packet header (bytes 4–9, BCD) ───

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

wss.on('connection', (ws, req) => {
    console.log(`[WS] Browser connected from ${req.socket.remoteAddress}`);
    ws.on('close', () => console.log('[WS] Browser disconnected'));
    ws.on('error', err => console.error('[WS] Error:', err.message));
});

// ── HTTP server ───────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    const urlPath = req.url.split('?')[0];
    let filePath;
    if (urlPath === '/') {
        filePath = './video.html';
    } else if (urlPath.startsWith('/public/')) {
        filePath = `.${urlPath}`;
    } else {
        filePath = `.${urlPath}`;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentTypes = {
        '.html': 'text/html',
        '.js':   'application/javascript',
        '.m3u8': 'application/vnd.apple.mpegurl',
        '.ts':   'video/mp2t',
    };

    if (req.method === 'HEAD') {
        fs.stat(filePath, (err, stat) => {
            if (err) { res.writeHead(404); res.end(); return; }
            res.writeHead(200, {
                'Content-Type':   contentTypes[ext] || 'application/octet-stream',
                'Content-Length': stat.size,
                'Access-Control-Allow-Origin': '*',
                'Cache-Control':  'no-cache',
            });
            res.end();
        });
        return;
    }

    fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); res.end('Not found'); return; }
        res.writeHead(200, {
            'Content-Type':   contentTypes[ext] || 'text/plain',
            'Content-Length': data.length,
            'Access-Control-Allow-Origin': '*',
            'Cache-Control':  'no-cache',
        });
        res.end(data);
    });
}).listen(CONFIG.httpPort, () => console.log(`✓ HTTP on :${CONFIG.httpPort}`));

// ── FFmpeg — HLS live stream ──────────────────────────────────────────────────
// Each device/channel gets its own FFmpeg process.
// Output lives in:  ./public/<phone>/<phone>_ch<N>.m3u8
function startFFmpeg(phone, channel) {
    const dir = `./public/${phone}`;
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

    const playlist = `${dir}/video${channel}.m3u8`;
    const segments = `${dir}/video${channel}_%03d.ts`;

    console.log(`[FFmpeg] Starting for ${phone} ch${channel} → ${playlist}`);

    const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
        '-fflags',          '+genpts+discardcorrupt+igndts',
        '-err_detect',      'ignore_err',
        '-f',               'mpegts',
        '-probesize',       '500000',
        '-analyzeduration', '1000000',
        '-i',               'pipe:0',
        '-c:v',             'libx264',
        '-preset',          'ultrafast',
        '-tune',            'zerolatency',
        '-g',               '50',
        '-keyint_min',      '25',
        '-f',               'hls',
        '-hls_time',        '1',
        '-hls_list_size',   '3',
        '-hls_flags',       'delete_segments+append_list',
        '-hls_segment_filename', segments,
        playlist,
    ]);

    ffmpeg.stderr.on('data', d => {
        console.log(`[FFmpeg ${phone} ch${channel}] ${d.toString().trim()}`);
    });

    ffmpeg.on('close', code => {
        console.log(`[FFmpeg ${phone} ch${channel}] closed (code ${code}), restarting in 1s...`);
        setTimeout(() => {
            const ch = getOrCreateDeviceChannel(phone, channel);
            ch.patPmtSent = false;
            ch.tsCounter  = 0;
            ch.gotIFrame  = false;
            ch.ffmpeg     = startFFmpeg(phone, channel);
        }, 1000);
    });

    return ffmpeg;
}

// ── Ensure FFmpeg is running for a device/channel ─────────────────────────────
function ensureFFmpeg(phone, channel) {
    const ch = getOrCreateDeviceChannel(phone, channel);
    if (!ch.ffmpeg || ch.ffmpeg.exitCode !== null) {
        ch.ffmpeg = startFFmpeg(phone, channel);
    }
    return ch;
}

// ── MPEG-TS wrapping ──────────────────────────────────────────────────────────
const TS_PACKET_SIZE = 188;
const VIDEO_PID      = 256;
const PMT_PID        = 4096;

function buildPAT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0] = 0x47; pkt[1] = 0x40; pkt[2] = 0x00; pkt[3] = 0x10;
    pkt[4] = 0x00;
    const s = pkt.slice(5);
    s[0]  = 0x00;
    s[1]  = 0xB0; s[2] = 0x0D;
    s[3]  = 0x00; s[4] = 0x01;
    s[5]  = 0xC1;
    s[6]  = 0x00; s[7] = 0x00;
    s[8]  = 0x00; s[9] = 0x01;
    s[10] = (PMT_PID >> 8) | 0xE0;
    s[11] = PMT_PID & 0xFF;
    return pkt;
}

function buildPMT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0] = 0x47;
    pkt[1] = 0x40 | ((PMT_PID >> 8) & 0x1F);
    pkt[2] = PMT_PID & 0xFF;
    pkt[3] = 0x10;
    pkt[4] = 0x00;
    const s = pkt.slice(5);
    s[0]  = 0x02;
    s[1]  = 0xB0; s[2] = 0x12;
    s[3]  = 0x00; s[4] = 0x01;
    s[5]  = 0xC1;
    s[6]  = 0x00; s[7] = 0x00;
    s[8]  = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[9]  = VIDEO_PID & 0xFF;
    s[10] = 0xF0; s[11] = 0x00;
    s[12] = 0x42;  // stream_type: AVS (0x42)
    s[13] = 0xE0 | ((VIDEO_PID >> 8) & 0x1F);
    s[14] = VIDEO_PID & 0xFF;
    s[15] = 0xF0; s[16] = 0x00;
    return pkt;
}

function wrapFrameInTS(frameData, counter) {
    const pesHdr = Buffer.from([
        0x00, 0x00, 0x01,
        0xE0,
        0x00, 0x00,
        0x80,
        0x00,
        0x00,
    ]);
    const pes = Buffer.concat([pesHdr, frameData]);

    const packets = [];
    let pos = 0, first = true, ctr = counter;

    while (pos < pes.length) {
        const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
        pkt[0] = 0x47;
        pkt[1] = (first ? 0x40 : 0x00) | ((VIDEO_PID >> 8) & 0x1F);
        pkt[2] = VIDEO_PID & 0xFF;
        pkt[3] = 0x10 | (ctr & 0x0F);
        ctr    = (ctr + 1) & 0x0F;

        const room  = TS_PACKET_SIZE - 4;
        const chunk = pes.slice(pos, pos + room);
        chunk.copy(pkt, 4);
        pos  += chunk.length;
        first = false;
        packets.push(pkt);
    }

    return { packets, nextCounter: ctr };
}

// ── Handle one complete video frame ──────────────────────────────────────────
function handleVideoFrame(frameData, channel, dataType, phone) {
    const ch = getOrCreateDeviceChannel(phone, channel);

    // If FFmpeg isn't ready yet, wait up to 3 seconds and retry
    if (!ch.ffmpeg || !ch.ffmpeg.stdin.writable) {
        if (!ch._retryCount) ch._retryCount = 0;
        if (ch._retryCount < 30) {
            ch._retryCount++;
            setTimeout(() => handleVideoFrame(frameData, channel, dataType, phone), 100);
        } else {
            console.warn(`[${phone}] ch${channel} FFmpeg not ready after 3s, dropping frame`);
        }
        return;
    }
    ch._retryCount = 0;

    const isVideo = (dataType === 0 || dataType === 1 || dataType === 2);
    if (!isVideo) return;

    if (dataType === 0) {
        ch.gotIFrame = true;
        console.log(`[${phone}] ch${channel} ✅ I_FRAME size:${frameData.length}`);
    } else {
        if (!ch.gotIFrame) return;
        console.log(`[${phone}] ch${channel} ${dataType === 1 ? 'P' : 'B'}_FRAME size:${frameData.length}`);
    }

    if (!ch.ffmpeg || !ch.ffmpeg.stdin.writable) return;

    if (!ch.patPmtSent) {
        ch.ffmpeg.stdin.write(buildPAT());
        ch.ffmpeg.stdin.write(buildPMT());
        ch.patPmtSent = true;
        console.log(`[${phone}] ch${channel} 📺 Sent PAT+PMT`);
    }

    const { packets, nextCounter } = wrapFrameInTS(frameData, ch.tsCounter);
    ch.tsCounter = nextCounter;
    for (const pkt of packets) ch.ffmpeg.stdin.write(pkt);
}

// ── Reassemble subpackets (T/98 §5.5.3) ─────────────────────────────────────
// subpktMarker: 0=atomic, 1=first, 3=middle, 2=last
function processVideoPacket(rawData, channel, dataType, subpktMarker, phone) {
    const ch = getOrCreateDeviceChannel(phone, channel);

    if (subpktMarker === 0) {
        handleVideoFrame(rawData, channel, dataType, phone);
    } else if (subpktMarker === 1) {
        ch.subpackets = [rawData];
    } else if (subpktMarker === 3) {
        if (ch.subpackets.length > 0) ch.subpackets.push(rawData);
    } else if (subpktMarker === 2) {
        if (ch.subpackets.length > 0) {
            ch.subpackets.push(rawData);
            const complete = Buffer.concat(ch.subpackets);
            ch.subpackets  = [];
            console.log(`[${phone}] ch${channel} complete frame size:${complete.length}`);
            handleVideoFrame(complete, channel, dataType, phone);
        }
    }
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
    body.writeUInt16BE(0,          3 + N);
    body[5 + N] = channel;
    body[6 + N] = 1;
    body[7 + N] = 1;
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
            case 0x01: if (val.length >= 4) result.mileage        = val.readUInt32BE(0) / 10 + ' km';   break;
            case 0x03: if (val.length >= 2) result.sensorSpeed    = val.readUInt16BE(0) / 10 + ' km/h'; break;
            case 0x25: if (val.length >= 2) result.voltage        = val.readUInt16BE(0) / 10 + ' V';    break;
            case 0x30: if (val.length >= 1) result.signalStrength = val[0];                             break;
            case 0x31: if (val.length >= 1) result.satellites     = val[0];                             break;
        }
        i += 2 + len;
    }
    return result;
}

// ── TCP server ────────────────────────────────────────────────────────────────
const tcpServer = net.createServer(socket => {
    const remote = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`Device connected: ${remote}`);
    let buffer = Buffer.alloc(0);
    let phone  = null;  // set from signalling; stream packets carry their own phone

    socket.on('data', data => {
        try {
            // If this is a video-only connection (no signalling), identify by IP
            if (!phone && phoneByIp[socket.remoteAddress]) {
                phone = phoneByIp[socket.remoteAddress];
                console.log(`[stream] Video connection identified as ${phone} by IP`);
            }

            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length - 4) {

                // ── Stream data packet (T/98 §5.5.3 — 0x30316364 header) ────
                // ── Stream data packet (T/98 §5.5.3 — 0x30316364 header) ────
                if (buffer[offset]   === 0x30 && buffer[offset+1] === 0x31 &&
                    buffer[offset+2] === 0x63 && buffer[offset+3] === 0x64) {

                    if (offset + 30 > buffer.length) break;
                    const dataBodyLen = buffer.readUInt16BE(offset + 28);
                    if (offset + 30 + dataBodyLen > buffer.length) break;

                    const byte15       = buffer[offset + 15];
                    const dataType     = (byte15 >> 4) & 0x0F;
                    const subpktMarker = byte15 & 0x0F;
                    const channel      = buffer[offset + 14];
                    const rawData      = buffer.slice(offset + 30, offset + 30 + dataBodyLen);

                    // Only process video if we know which device this is (set by signalling handshake)
                    if (phone) {
                        processVideoPacket(rawData, channel, dataType, subpktMarker, phone);
                    }

                    offset += 30 + dataBodyLen;
                    continue;
                }

                // ── Signalling packet (JT/T 808 — 0x7E framing) ─────────────
                if (buffer[offset] === 0x7E) {
                    const end = buffer.indexOf(0x7E, offset + 1);
                    if (end === -1) break;

                    const inner     = buffer.slice(offset + 1, end);
                    const unescaped = unescapeBuffer(inner);
                    if (unescaped.length < 12) { offset = end + 1; continue; }

                    const msgId = unescaped.readUInt16BE(0);
                    phone = unescaped.slice(4, 10)
                        .map(b => `${(b >> 4) & 0x0F}${b & 0x0F}`)
                        .join('')
                        .replace(/^0+/, '');
                    const seq  = unescaped.readUInt16BE(10);
                    const body = unescaped.slice(12);
                    console.log(`[signalling] msgId: 0x${msgId.toString(16).padStart(4,'0')} phone: ${phone}`);

                    // ── 0x0001: General response from device ─────────────────
                    if (msgId === 0x0001) {
                        const replyMsgId  = body.readUInt16BE(2);
                        const replyResult = body[4];
                        const resultText  = ['Success','Failed','Wrong Msg','Not Supported','Alarm Confirmed','Update Required'][replyResult] || `Unknown(${replyResult})`;
                        console.log(`[ACK] replyTo:0x${replyMsgId.toString(16).padStart(4,'0')} result:${replyResult} (${resultText})`);

                    // ── 0x0100: Device register ──────────────────────────────
                    } else if (msgId === 0x0100) {
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    // ── 0x0102: Auth complete — start live stream ────────────
                    } else if (msgId === 0x0102) {
                            socket.write(buildAck(phone, seq, msgId));
                            socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
                            tcpSockets[phone] = socket;

                            const ip = socket.remoteAddress;
                            phoneByIp[ip] = phone;
                            console.log(`[signalling] Registered socket for ${phone} ip=${ip}`);

                            ensureFFmpeg(phone, 1);

                            // Tell all browser clients a new device is live
                            const connMsg = JSON.stringify({ type: 'device_connected', phone });
                            wss.clients.forEach(c => { if (c.readyState === 1) c.send(connMsg); });

                    // ── 0x0200: Location / GPS report ────────────────────────
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

                        const bcd        = b => ((b >> 4) * 10 + (b & 0x0F));
                        const timeOffset = 22;
                        const yy = bcd(body[timeOffset]);
                        const mo = bcd(body[timeOffset + 1]);
                        const dd = bcd(body[timeOffset + 2]);
                        const hh = bcd(body[timeOffset + 3]);
                        const mm = bcd(body[timeOffset + 4]);
                        const ss = bcd(body[timeOffset + 5]);
                        const dt = `20${String(yy).padStart(2,'0')}-${String(mo).padStart(2,'0')}-${String(dd).padStart(2,'0')} ${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;

                        const alarms = [];
                        if (alarmFlags & (1<<0)) alarms.push('Emergency');
                        if (alarmFlags & (1<<1)) alarms.push('Overspeed');
                        if (alarmFlags & (1<<4)) alarms.push('GNSS Fault');
                        if (alarmFlags & (1<<5)) alarms.push('GNSS Antenna Cut');
                        if (alarmFlags & (1<<7)) alarms.push('Low Voltage');
                        if (alarmFlags & (1<<8)) alarms.push('Power Off');

                        const extra = parseAdditionalInfo(body.slice(27));
                        const locationData = {
                            type: 'location', phone, lat, lon, speed, direction,
                            elevation, datetime: dt, accOn, located, alarms, ...extra,
                        };

                        console.log(`[GPS] ${phone} lat=${lat} lon=${lon} speed=${speed}km/h dir=${direction}° dt=${dt}`);

                        wss.clients.forEach(client => {
                            if (client.readyState === 1) client.send(JSON.stringify(locationData));
                        });

                        // GPS CSV log
                        const fileName  = `gps_log_${new Date().toISOString().slice(0, 10)}.txt`;
                        const gpsRecord = {
                            phone,
                            datetime:        dt,
                            latitude:        lat,
                            longitude:       lon,
                            speed_kmh:       speed,
                            direction_deg:   direction,
                            elevation_m:     elevation,
                            acc:             accOn   ? 'ON'    : 'OFF',
                            located:         located ? 'YES'   : 'NO',
                            mileage:         extra.mileage        || '0',
                            voltage:         extra.voltage        || '0',
                            satellites:      extra.satellites     || '0',
                            signal:          extra.signalStrength || '0',
                            sensor_speed:    extra.sensorSpeed    || '0',
                            oil_circuit:     !!(statusBits & (1<<10)) ? 'CUT'    : 'NORMAL',
                            vehicle_circuit: !!(statusBits & (1<<11)) ? 'CUT'    : 'NORMAL',
                            door:            !!(statusBits & (1<<13)) ? 'OPEN'   : 'CLOSED',
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
                        fs.appendFile(`./${fileName}`, Object.values(gpsRecord).join(',') + '\n', err => {
                            if (err) console.error('[GPS LOG] write error:', err.message);
                        });

                    // ── Everything else: generic ACK ─────────────────────────
                    } else {
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                offset++;
            }

            buffer = buffer.slice(offset);

        } catch (err) {
            console.error('[TCP] Error processing data:', err.message);
        }
    });

    socket.on('close', () => {
        console.log(`Device disconnected: ${remote} phone:${phone}`);
        if (phone) delete tcpSockets[phone];
        if (socket.remoteAddress) delete phoneByIp[socket.remoteAddress];
    });
    socket.on('error', err => console.error(`[TCP] Socket error: ${err.message}`));
});

tcpServer.listen(CONFIG.tcpPort, () => console.log(`✓ TCP server on :${CONFIG.tcpPort}`));

// FFmpeg starts on demand when each device authenticates — no boot-time start needed.