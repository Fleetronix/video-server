'use strict';

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
require('dotenv').config();
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');

// ── Config ────────────────────────────────────────────────────────────────────
const CONFIG = {
    tcpPort:  3007,
    httpPort: 8080,
    wsPort:   8801,
    serverIp: process.env.SERVER_IP
};
console.log(`Server IP: ${CONFIG.serverIp}`);

// ── Create output folder ──────────────────────────────────────────────────────
if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── Device recording list (from 0x1205 responses) ────────────────────────────
// When device connects and authenticates, we send 0x9205 to ask for its SD card
// recording list. Device replies with 0x1205 which we parse and store here.
const deviceRecordings = {}; // { [phone]: [ {ch, startTime, endTime, size, ...} ] }
const tcpSockets       = {}; // { [phone]: socket } — live device connections

function broadcastRecordings() {
    const all = Object.values(deviceRecordings)
        .flat()
        .sort((a, b) => b.startTime.localeCompare(a.startTime));
    const msg = JSON.stringify({ type: 'recordings', data: all });
    console.log(`[RecDB] Broadcasting ${all.length} recordings to all browsers`);
    wss.clients.forEach(client => {
        if (client.readyState === 1) client.send(msg);
    });
}

// Build 0x9205 — Query Resource List (T/98 §5.6.1)
// Asks device for ALL video recordings on its SD card (no time filter)
function buildQueryRecordings(phone) {
    const body = Buffer.alloc(23);
    body[0] = 0;           // logical channel: 0 = all channels
    body.fill(0x00, 1, 7); // startTime BCD[6]: all 0 = no start filter
    body.fill(0x00, 7, 13);// endTime   BCD[6]: all 0 = no end filter
    body.fill(0x00, 13,21);// alarmLogo 64bits: all 0 = no alarm filter
    body[21] = 2;          // avType: 2 = Video only
    body[22] = 0;          // streamType: 0 = all streams
    return buildFrame(0x9205, body, phone);
}

// Build 0x9201 — Remote video playback request (T/98 §5.6.3)
// Tells device to stream a recorded clip to our TCP port
function buildPlaybackRequest(phone, serverIp, serverPort, channel, startTime, endTime) {
    // startTime / endTime format: 'YYYY-MM-DD HH:MM:SS'
    const toBCD = str => {
        // str like '2024-05-10 14:30:00' → extract parts
        const [datePart, timePart] = str.split(' ');
        const [Y, M, D] = datePart.split('-').map(Number);
        const [h, m, s] = timePart.split(':').map(Number);
        // BCD[6]: YY MM DD HH MM SS
        return Buffer.from([
            ((Math.floor((Y % 100) / 10) << 4) | (Y % 10)),
            (((M / 10 | 0) << 4) | (M % 10)),
            (((D / 10 | 0) << 4) | (D % 10)),
            (((h / 10 | 0) << 4) | (h % 10)),
            (((m / 10 | 0) << 4) | (m % 10)),
            (((s / 10 | 0) << 4) | (s % 10)),
        ]);
    };

    const ipBuf = Buffer.from(serverIp, 'ascii');
    const N     = ipBuf.length;
    // Table 24: 0(ipLen) 1..N(ip) N+1,N+2(tcpPort) N+3,N+4(udpPort=0)
    //           N+5(logicalCh) N+6(avType) N+7(streamType) N+8(memType)
    //           N+9(playbackMode=0 normal) N+10(speed=0) N+11..N+16(startBCD) N+17..N+22(endBCD)
    const body = Buffer.alloc(11 + N + 12);
    body[0] = N;
    ipBuf.copy(body, 1);
    body.writeUInt16BE(serverPort, 1 + N);
    body.writeUInt16BE(0,          3 + N); // UDP port = 0
    body[5 + N] = channel;
    body[6 + N] = 2; // avType: 2 = video only
    body[7 + N] = 0; // streamType: 0 = main or sub
    body[8 + N] = 0; // memType: 0 = all storage
    body[9 + N] = 0; // playback mode: 0 = normal
    body[10+ N] = 0; // speed multiplier: 0
    toBCD(startTime).copy(body, 11 + N);
    toBCD(endTime).copy(body,   17 + N);
    return buildFrame(0x9201, body, phone);
}

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

wss.on('connection', (ws, req) => {
    console.log(`[WS] Browser connected from ${req.socket.remoteAddress}`);

    ws.on('message', raw => {
        let msg;
        try { msg = JSON.parse(raw); }
        catch(e) { console.warn('[WS] Non-JSON message:', raw.toString()); return; }

        console.log(`[WS] Browser message: type=${msg.type}`, JSON.stringify(msg));

        // ── query_recordings: browser asks for recording list ─────────────
        if (msg.type === 'query_recordings') {
            const { startDate, endDate } = msg;
            console.log(`[WS] query_recordings from:${startDate} to:${endDate}`);
            console.log(`[WS] deviceRecordings keys: ${Object.keys(deviceRecordings)}`);

            let all = Object.values(deviceRecordings).flat();
            console.log(`[WS] Total recordings in store: ${all.length}`);

            if (startDate) {
                all = all.filter(r => r.startTime.split(' ')[0] >= startDate);
                console.log(`[WS] After startDate(${startDate}) filter: ${all.length}`);
            }
            if (endDate) {
                all = all.filter(r => r.startTime.split(' ')[0] <= endDate);
                console.log(`[WS] After endDate(${endDate}) filter: ${all.length}`);
            }

            all.sort((a, b) => b.startTime.localeCompare(a.startTime));
            ws.send(JSON.stringify({ type: 'recordings', data: all }));
            console.log(`[WS] Sent ${all.length} recordings to browser`);

            // If store is empty re-query all connected devices
            if (all.length === 0 && Object.keys(tcpSockets).length > 0) {
                console.log('[WS] Store empty — re-querying all devices');
                Object.entries(tcpSockets).forEach(([ph, sock]) => {
                    if (sock && !sock.destroyed) {
                        sock.write(buildQueryRecordings(ph));
                        console.log(`[WS] Re-sent 0x9205 to ${ph}`);
                    }
                });
            } else if (Object.keys(tcpSockets).length === 0) {
                console.warn('[WS] No devices connected — cannot query device');
            }
        }

        // ── playback_request: browser wants to play a recording ───────────
        if (msg.type === 'playback_request') {
            const { channel, startTime, endTime, phone: reqPhone } = msg;
            console.log(`[WS] playback_request ch:${channel} start:${startTime} end:${endTime} phone:${reqPhone}`);

            // Find the device phone for this recording
            let targetPhone = reqPhone;
            if (!targetPhone) {
                // Pick first connected device if not specified
                targetPhone = Object.keys(tcpSockets)[0];
            }

            if (!targetPhone || !tcpSockets[targetPhone] || tcpSockets[targetPhone].destroyed) {
                console.warn('[WS] No device socket available for playback');
                ws.send(JSON.stringify({ type: 'playback_error', message: 'Device not connected' }));
                return;
            }

            console.log(`[WS] Sending 0x9201 playback request to device ${targetPhone}`);
            tcpSockets[targetPhone].write(
                buildPlaybackRequest(targetPhone, CONFIG.serverIp, CONFIG.tcpPort, channel, startTime, endTime)
            );

            // Playback video will come in on the TCP stream as video frames
            // and get piped to FFmpeg just like live — browser plays /public/ch1.m3u8
            ws.send(JSON.stringify({
                type: 'playback_url',
                url:  `/public/ch${channel}.m3u8`,
                note: 'Playback stream sent to device — playing on live HLS feed'
            }));
        }
    });

    ws.on('close', () => console.log('[WS] Browser disconnected'));
    ws.on('error', err => console.error('[WS] Error:', err.message));
});

// ── HTTP server ───────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    let filePath;
    if (req.url === '/') {
        filePath = './video.html';
    } else if (req.url.startsWith('/public/')) {
        filePath = `.${req.url}`;
    } else {
        filePath = `.${req.url}`;
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentTypes = {
        '.html': 'text/html',
        '.js':   'application/javascript',
        '.m3u8': 'application/vnd.apple.mpegurl',
        '.ts':   'video/mp2t',
    };

    fs.readFile(filePath, (err, data) => {
        if (err) { res.writeHead(404); res.end('Not found'); return; }
        res.writeHead(200, {
            'Content-Type': contentTypes[ext] || 'text/plain',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-cache',
        });
        res.end(data);
    });
}).listen(CONFIG.httpPort, () => console.log(`✓ HTTP on :${CONFIG.httpPort}`));

// ── FFmpeg for HLS ────────────────────────────────────────────────────────────
function startFFmpeg(channel) {
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
        '-hls_segment_filename', `./public/ch${channel}_%03d.ts`,
        `./public/ch${channel}.m3u8`,
    ]);

    ffmpeg.stderr.on('data', d => {
        const m = d.toString().trim();
        if (m.includes('Error') || m.includes('Invalid') || m.includes('error')) {
            console.error(`FFmpeg ch${channel}:`, m);
        }
    });

    ffmpeg.on('close', code => {
        console.log(`FFmpeg ch${channel} closed (code ${code}), restarting in 1s...`);
        setTimeout(() => { channels[channel].ffmpeg = startFFmpeg(channel); }, 1000);
    });

    return ffmpeg;
}

const channels = {
    1: { ffmpeg: null, gotIFrame: false, subpackets: [] },
};

channels[1].ffmpeg = startFFmpeg(1);

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
    s[12] = 0x42;
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
    let pos = 0; let first = true; let ctr = counter;

    while (pos < pes.length) {
        const pkt  = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
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

let patPmtSent = false;
let tsCounter  = 0;

function handleVideoFrame(frameData, channel, dataType) {
    const ch = channels[channel];
    if (!ch) return;

    const isVideo = (dataType === 0 || dataType === 1 || dataType === 2);
    if (!isVideo) return;

    if (dataType === 0) {
        ch.gotIFrame = true;
        console.log(`ch${channel} ✅ I_FRAME size:${frameData.length}`);
    } else {
        if (!ch.gotIFrame) return;
        console.log(`ch${channel} ${dataType === 1 ? 'P' : 'B'}_FRAME size:${frameData.length}`);
    }

    if (!ch.ffmpeg || !ch.ffmpeg.stdin.writable) return;

    if (!patPmtSent) {
        ch.ffmpeg.stdin.write(buildPAT());
        ch.ffmpeg.stdin.write(buildPMT());
        patPmtSent = true;
        console.log(`ch${channel} 📺 Sent PAT+PMT`);
    }

    const { packets, nextCounter } = wrapFrameInTS(frameData, tsCounter);
    tsCounter = nextCounter;
    for (const pkt of packets) ch.ffmpeg.stdin.write(pkt);
}

function processVideoPacket(rawData, channel, dataType, subpktMarker) {
    const ch = channels[channel];
    if (!ch) return;

    if (subpktMarker === 0) {
        handleVideoFrame(rawData, channel, dataType);
    } else if (subpktMarker === 1) {
        ch.subpackets = [rawData];
    } else if (subpktMarker === 3) {
        if (ch.subpackets.length > 0) ch.subpackets.push(rawData);
    } else if (subpktMarker === 2) {
        if (ch.subpackets.length > 0) {
            ch.subpackets.push(rawData);
            const complete = Buffer.concat(ch.subpackets);
            ch.subpackets  = [];
            console.log(`ch${channel} complete frame size:${complete.length}`);
            handleVideoFrame(complete, channel, dataType);
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
    Buffer.from(phone.match(/.{2}/g).map(h => parseInt(h, 16))).copy(header, 4);
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
    body[7 + N] = 1;   // ← SUBSTREAM (low quality, faster streaming)
    return buildFrame(0x9101, body, phone);
}

function parseAdditionalInfo(buf) {
    const result = {};
    let i = 0;
    while (i < buf.length - 2) {
        const id  = buf[i];
        const len = buf[i+1];
        if (i + 2 + len > buf.length) break;
        const val = buf.slice(i+2, i+2+len);
        switch(id) {
            case 0x01: if (val.length >= 4) result.mileage        = val.readUInt32BE(0) / 10 + ' km';   break;
            case 0x03: if (val.length >= 2) result.sensorSpeed    = val.readUInt16BE(0) / 10 + ' km/h'; break;
            case 0x25: if (val.length >= 2) result.voltage        = val.readUInt16BE(0) / 10 + ' V';    break;
            case 0x30: if (val.length >= 1) result.signalStrength = val[0];                              break;
            case 0x31: if (val.length >= 1) result.satellites     = val[0];                              break;
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
    let phone  = null;

    socket.on('data', data => {
        try {
            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length - 4) {

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

                    processVideoPacket(rawData, channel, dataType, subpktMarker);

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

                    if (msgId === 0x0100) {
                        // Registration
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    } else if (msgId === 0x0102) {
                        // Auth success → start live substream + query recording list
                        socket.write(buildAck(phone, seq, msgId));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
                        tcpSockets[phone] = socket; // register socket for later re-query
                        console.log(`[signalling] Registered socket for phone ${phone}`);
                        // Small delay so device finishes processing auth before we query
                        setTimeout(() => {
                            if (!socket.destroyed) {
                                socket.write(buildQueryRecordings(phone));
                                console.log(`[signalling] Sent 0x9205 (query recordings) to ${phone}`);
                            }
                        }, 2000);

                    } else if (msgId === 0x1205) {
                        // Device replied with its SD card recording list
                        socket.write(buildAck(phone, seq, msgId));
                        console.log(`[Rec] 0x1205 received from ${phone} bodyLen:${body.length}`);

                        try {
                            // body layout per T/98 §5.6.2 Table 22:
                            //   byte 0-1: serial number (WORD)
                            //   byte 2-5: total number of items (DWORD)
                            //   byte 6+:  list of items (Table 23, each 28 bytes)
                            if (body.length < 6) {
                                console.warn('[Rec] 0x1205 body too short:', body.length);
                                offset = end + 1; continue;
                            }

                            const totalItems = body.readUInt32BE(2);
                            console.log(`[Rec] totalItems reported by device: ${totalItems}`);

                            const bcd = b => ((b >> 4) * 10 + (b & 0x0F));
                            const recs = [];
                            let p = 6;

                            // Each record in Table 23:
                            // 0:   logicalCh  (1 byte)
                            // 1-6: startTime  BCD[6]
                            // 7-12:endTime    BCD[6]
                            // 13-20:alarmLogo (8 bytes / 64 bits)
                            // 21:  avType     (1 byte)
                            // 22:  streamType (1 byte)
                            // 23:  memType    (1 byte)
                            // 24-27:fileSize  DWORD
                            // Total = 28 bytes per record

                            while (p + 28 <= body.length) {
                                const logicalCh = body[p];

                                const sY = bcd(body[p+1]);  const sM = bcd(body[p+2]);
                                const sD = bcd(body[p+3]);  const sH = bcd(body[p+4]);
                                const sm = bcd(body[p+5]);  const sS = bcd(body[p+6]);

                                const eY = bcd(body[p+7]);  const eM = bcd(body[p+8]);
                                const eD = bcd(body[p+9]);  const eH = bcd(body[p+10]);
                                const em = bcd(body[p+11]); const eS = bcd(body[p+12]);

                                const startTime = `20${String(sY).padStart(2,'0')}-${String(sM).padStart(2,'0')}-${String(sD).padStart(2,'0')} ${String(sH).padStart(2,'0')}:${String(sm).padStart(2,'0')}:${String(sS).padStart(2,'0')}`;
                                const endTime   = `20${String(eY).padStart(2,'0')}-${String(eM).padStart(2,'0')}-${String(eD).padStart(2,'0')} ${String(eH).padStart(2,'0')}:${String(em).padStart(2,'0')}:${String(eS).padStart(2,'0')}`;

                                const avType     = body[p + 21];
                                const streamType = body[p + 22];
                                const memType    = body[p + 23];
                                const fileSize   = body.readUInt32BE(p + 24);

                                const rec = {
                                    ch: logicalCh, startTime, endTime,
                                    avType, streamType, memType,
                                    size: fileSize,
                                    phone,
                                };
                                recs.push(rec);
                                console.log(`[Rec]  ch${logicalCh} ${startTime} → ${endTime} size:${fileSize} av:${avType} st:${streamType}`);
                                p += 28;
                            }

                            deviceRecordings[phone] = recs;
                            console.log(`[Rec] Stored ${recs.length} recordings for ${phone}`);
                            broadcastRecordings();

                        } catch(e) {
                            console.error('[Rec] Error parsing 0x1205:', e.message, e.stack);
                        }

                    } else if (msgId === 0x0200) {
                        // GPS location report
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
                        const mo = bcd(body[timeOffset+1]);
                        const dd = bcd(body[timeOffset+2]);
                        const hh = bcd(body[timeOffset+3]);
                        const mm = bcd(body[timeOffset+4]);
                        const ss = bcd(body[timeOffset+5]);
                        const dt = `20${String(yy).padStart(2,'0')}-${String(mo).padStart(2,'0')}-${String(dd).padStart(2,'0')} ${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;

                        const alarms = [];
                        if (alarmFlags & (1<<0)) alarms.push('Emergency');
                        if (alarmFlags & (1<<1)) alarms.push('Overspeed');
                        if (alarmFlags & (1<<4)) alarms.push('GNSS Fault');
                        if (alarmFlags & (1<<5)) alarms.push('GNSS Antenna Cut');
                        if (alarmFlags & (1<<7)) alarms.push('Low Voltage');
                        if (alarmFlags & (1<<8)) alarms.push('Power Off');

                        const locationData = {
                            type: 'location', phone, lat, lon, speed, direction,
                            elevation, datetime: dt, accOn, located, alarms,
                            ...parseAdditionalInfo(body.slice(27))
                        };

                        console.log(`[GPS] ${phone} lat=${lat} lon=${lon} speed=${speed}km/h dir=${direction}° dt=${dt}`);

                        wss.clients.forEach(client => {
                            if (client.readyState === 1) client.send(JSON.stringify(locationData));
                        });

                        const fileName  = `gps_log_${new Date().toISOString().slice(0,10)}.txt`;
                        const gpsRecord = {
                            phone, datetime: dt, latitude: lat, longitude: lon,
                            speed_kmh: speed, direction_deg: direction, elevation_m: elevation,
                            acc:             accOn   ? 'ON'    : 'OFF',
                            located:         located ? 'YES'   : 'NO',
                            mileage:         locationData.mileage        || '--',
                            voltage:         locationData.voltage        || '--',
                            satellites:      locationData.satellites     || '--',
                            signal:          locationData.signalStrength || '--',
                            sensor_speed:    locationData.sensorSpeed    || '--',
                            oil_circuit:     !!(statusBits & (1<<10)) ? 'CUT'  : 'NORMAL',
                            vehicle_circuit: !!(statusBits & (1<<11)) ? 'CUT'  : 'NORMAL',
                            door:            !!(statusBits & (1<<13)) ? 'OPEN' : 'CLOSED',
                            alarms: alarmFlags !== 0 ? [
                                (alarmFlags & (1<<0)) ? 'EMERGENCY'   : null,
                                (alarmFlags & (1<<1)) ? 'OVERSPEED'   : null,
                                (alarmFlags & (1<<4)) ? 'GNSS_FAULT'  : null,
                                (alarmFlags & (1<<5)) ? 'ANTENNA_CUT' : null,
                                (alarmFlags & (1<<7)) ? 'LOW_VOLTAGE' : null,
                                (alarmFlags & (1<<8)) ? 'POWER_OFF'   : null,
                            ].filter(Boolean).join('|') : 'NONE',
                        };

                        fs.appendFile(`./${fileName}`, Object.values(gpsRecord).join(',') + '\n', err => {
                            if (err) console.error('[GPS LOG] write error:', err.message);
                        });

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
            console.error('Error processing data:', err.message, err.stack);
        }
    });

    socket.on('close', () => {
        console.log(`Device disconnected: ${remote} phone:${phone}`);
        if (phone) delete tcpSockets[phone];
    });
    socket.on('error', err => console.error(`Socket error: ${err.message}`));
});

tcpServer.listen(CONFIG.tcpPort, () => {
    console.log(`✓ TCP server on :${CONFIG.tcpPort}`);
});