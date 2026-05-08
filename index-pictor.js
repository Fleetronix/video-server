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
    serverIp: process.env.SERVER_IP,
};
console.log(`Server IP: ${CONFIG.serverIp}`);

if (!fs.existsSync('./public')) fs.mkdirSync('./public');

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

function wsBroadcast(obj) {
    const msg = JSON.stringify(obj);
    wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

// ── HTTP server ───────────────────────────────────────────────────────────────
http.createServer((req, res) => {
    const filePath = req.url === '/' ? './video.html' : `.${req.url}`;
    const ext = path.extname(filePath).toLowerCase();
    const contentTypes = {
        '.html': 'text/html', '.js': 'application/javascript',
        '.m3u8': 'application/vnd.apple.mpegurl', '.ts': 'video/mp2t',
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

// ── FFmpeg: H.265 → H.264/HLS ────────────────────────────────────────────────
// The camera sends H.265 frames with NO timestamps — just raw annex-B bytes.
// We use -f hevc (raw H.265 input) with -use_wallclock_as_timestamps 1 which
// makes FFmpeg stamp each frame at the moment it reads it from stdin.
// To prevent burst playback we rate-limit writes to stdin using a token bucket:
// we only write a frame if enough real time has passed for that frame to be "due".

function startFFmpeg(channel) {
    const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
        '-fflags',          '+genpts+discardcorrupt+igndts',
        '-err_detect',      'ignore_err',
        '-use_wallclock_as_timestamps', '1',
        '-f',               'hevc',
        '-i',               'pipe:0',
        '-vf',              'scale=trunc(iw/2)*2:trunc(ih/2)*2',
        '-c:v',             'libx264',
        '-preset',          'ultrafast',
        '-tune',            'zerolatency',
        '-profile:v',       'baseline',
        '-level',           '3.1',
        '-g',               '25',
        '-keyint_min',      '25',
        '-sc_threshold',    '0',
        '-an',
        '-f',               'hls',
        '-hls_time',        '1',
        '-hls_list_size',   '6',
        '-hls_flags',       'delete_segments+append_list+independent_segments',
        '-hls_segment_type','mpegts',
        '-hls_segment_filename', `./public/ch${channel}_%05d.ts`,
        `./public/ch${channel}.m3u8`,
    ]);

    ffmpeg.stderr.on('data', d => {
        const msg = d.toString().trim();
        if (/error|invalid/i.test(msg) && !/ref with POC|frame RPS|undecodable NALU/i.test(msg)) {
            console.error(`FFmpeg ch${channel}: ${msg}`);
        }
    });

    ffmpeg.on('close', code => {
        console.log(`FFmpeg ch${channel} exited (${code}), restarting in 2s...`);
        const ch = channels[channel];
        if (ch) { ch.gotIFrame = false; ch.frameTimer = null; }
        setTimeout(() => {
            if (channels[channel]) channels[channel].ffmpeg = startFFmpeg(channel);
        }, 2000);
    });

    return ffmpeg;
}

const channels = {
    1: { ffmpeg: null, gotIFrame: false, subpackets: [], subpacketDType: 0,
         frameTimer: null, frameQueue: [], draining: false },
};
channels[1].ffmpeg = startFFmpeg(1);

// ── H.265 keyframe detection ──────────────────────────────────────────────────
// NAL unit types 16-21 = BLA_W_LP / BLA_W_RADL / BLA_N_LP / IDR_W_RADL /
//                        IDR_N_LP / CRA_NUT — all are keyframes in H.265
function isH265Keyframe(buf) {
    for (let i = 0; i < buf.length - 4; i++) {
        let sc = 0;
        if (buf[i]===0 && buf[i+1]===0 && buf[i+2]===0 && buf[i+3]===1) sc = 4;
        else if (buf[i]===0 && buf[i+1]===0 && buf[i+2]===1) sc = 3;
        if (sc > 0) {
            const o = i + sc;
            if (o < buf.length) {
                const t = (buf[o] >> 1) & 0x3F;
                if (t >= 16 && t <= 21) return true;
            }
            i = i + sc; // skip past start code
        }
    }
    return false;
}

// ── Sub-packet reassembly ─────────────────────────────────────────────────────
function processVideoPacket(rawData, channel, dataType, subpktMarker) {
    const ch = channels[channel];
    if (!ch) return;

    if (subpktMarker === 0) {
        deliverFrame(rawData, channel, dataType);
    } else if (subpktMarker === 1) {
        ch.subpackets     = [rawData];
        ch.subpacketDType = dataType;
    } else if (subpktMarker === 3) {
        if (ch.subpackets.length > 0) ch.subpackets.push(rawData);
    } else if (subpktMarker === 2) {
        if (ch.subpackets.length > 0) {
            ch.subpackets.push(rawData);
            const frame = Buffer.concat(ch.subpackets);
            const dtype = ch.subpacketDType;
            ch.subpackets = [];
            deliverFrame(frame, channel, dtype);
        }
    }
}

// ── Rate-limited frame delivery ───────────────────────────────────────────────
// The camera bursts many frames at once. Without rate-limiting, FFmpeg gets
// all frames instantly, encodes them all, and stamps them with future PTS —
// causing the browser to see "2 minutes of video" that plays in fast-forward.
//
// Fix: use a token-bucket queue. Frames are queued and released at TARGET_FPS.
// This makes FFmpeg receive frames at real-time rate regardless of camera bursts.

const TARGET_FPS    = 25;                       // release one frame every this many ms
const FRAME_INTERVAL_MS = 1000 / TARGET_FPS;   // = 40ms per frame

function scheduleQueue(ch) {
    if (ch.draining || ch.frameQueue.length === 0) return;
    ch.draining = true;

    function drainOne() {
        if (ch.frameQueue.length === 0) { ch.draining = false; return; }

        const frame = ch.frameQueue.shift();
        if (ch.ffmpeg && ch.ffmpeg.stdin.writable) {
            ch.ffmpeg.stdin.write(frame);
        }

        // If queue is building up (>2s worth), drop oldest non-keyframe to catch up
        const maxQueue = TARGET_FPS * 2;
        while (ch.frameQueue.length > maxQueue) {
            const dropped = ch.frameQueue.shift();
            // Only drop if not a keyframe (keyframes start with 0 0 0 1 + IDR NAL)
            if (isH265Keyframe(dropped)) { ch.frameQueue.unshift(dropped); break; }
        }

        ch.frameTimer = setTimeout(drainOne, FRAME_INTERVAL_MS);
    }

    drainOne();
}

function deliverFrame(frameData, channel, dataType) {
    const ch = channels[channel];
    if (!ch) return;
    if (dataType === 3 || dataType === 4) return; // audio / transparent

    const keyframe = (dataType === 0) || isH265Keyframe(frameData);

    if (keyframe) {
        if (!ch.gotIFrame) console.log(`ch${channel} ✅ First keyframe — stream starting`);
        ch.gotIFrame = true;
    } else {
        if (!ch.gotIFrame) return; // wait for first keyframe
    }

    if (!ch.ffmpeg || !ch.ffmpeg.stdin.writable) return;

    // Ensure 4-byte annex-B start code prefix
    const frame = (frameData[0]===0 && frameData[1]===0 && frameData[2]===0 && frameData[3]===1)
        ? frameData
        : Buffer.concat([Buffer.from([0,0,0,1]), frameData]);

    ch.frameQueue.push(frame);
    scheduleQueue(ch);
}

// ── JT/T 808 protocol helpers ─────────────────────────────────────────────────
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
    body[4] = 0; // success
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

// T/98 §5.5.1 Table 17 — real-time audio+video request
function buildVideoRequest(phone, serverIp, serverPort, channel) {
    const ipBuf = Buffer.from(serverIp, 'ascii');
    const N     = ipBuf.length;
    const body  = Buffer.alloc(8 + N);
    body[0] = N;
    ipBuf.copy(body, 1);
    body.writeUInt16BE(serverPort, 1 + N); // TCP port
    body.writeUInt16BE(0,          3 + N); // UDP port = 0
    body[5 + N] = channel;
    body[6 + N] = 0; // dataType 0 = Audio+Video (was 1=VideoOnly before)
    body[7 + N] = 0; // main stream
    return buildFrame(0x9101, body, phone);
}

// T/98 §5.6.1 Table 21 — query resource list
// Body = 24 bytes: ch(1) + startBCD(6) + endBCD(6) + alarmFlag(8) + avType(1) + streamType(1) + memType(1)
function buildQueryRecordings(phone) {
    const body = Buffer.alloc(24, 0); // all-zero = no filters
    body[0]  = 1;   // logical channel 1 (not 0=all, some firmware ignores 0)
    body[21] = 3;   // avType 3 = Video OR Audio+Video (catches everything)
    body[22] = 0;   // streamType 0 = all streams
    body[23] = 0;   // memType 0 = all storage
    return buildFrame(0x9205, body, phone);
}

// Parse BCD date from terminal response (6 bytes: YY MM DD HH MM SS)
function parseBCD6(buf, offset) {
    const b = i => ((buf[offset+i] >> 4) * 10 + (buf[offset+i] & 0x0F));
    return `20${String(b(0)).padStart(2,'0')}-${String(b(1)).padStart(2,'0')}-${String(b(2)).padStart(2,'0')} ${String(b(3)).padStart(2,'0')}:${String(b(4)).padStart(2,'0')}:${String(b(5)).padStart(2,'0')}`;
}

// T/98 §5.6.2 Table 22+23 — parse terminal's recording list response
function parseRecordingList(body) {
    console.log(`[Recordings] raw body hex (first 80 bytes): ${body.slice(0, 80).toString('hex')}`);
    console.log(`[Recordings] body length: ${body.length}`);

    if (body.length < 6) {
        console.log(`[Recordings] ⚠️  Body too short`);
        return [];
    }

    const seq   = body.readUInt16BE(0);
    const total = body.readUInt32BE(2);
    console.log(`[Recordings] seq=${seq} total_reported=${total}`);

    if (total === 0) {
        console.log(`[Recordings] ⚠️  Device reports 0 recordings`);
        return [];
    }

    const recordings = [];
    let i = 6;
    let entry = 0;

    while (i < body.length) {
        const remaining = body.length - i;
        if (remaining < 28) {
            console.log(`[Recordings] ⚠️  Only ${remaining} bytes left at entry ${entry}, need 28 — stopping`);
            break;
        }
        const ch         = body[i];
        const startTime  = parseBCD6(body, i + 1);
        const endTime    = parseBCD6(body, i + 7);
        const alarmHex   = body.slice(i + 13, i + 21).toString('hex');
        const avType     = body[i + 21];
        const streamType = body[i + 22];
        const memType    = body[i + 23];
        const fileSize   = body.readUInt32BE(i + 24);
        console.log(`[Recordings] entry[${entry}]: ch=${ch} start=${startTime} end=${endTime} avType=${avType} size=${fileSize} alarm=${alarmHex}`);
        recordings.push({ ch, startTime, endTime, avType, streamType, memType, size: fileSize });
        i += 28;
        entry++;
    }

    console.log(`[Recordings] parsed ${recordings.length} of ${total} reported`);
    return recordings;
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
            case 0x30: if (val.length >= 1) result.signalStrength  = val[0];                            break;
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
    let phone  = null;

    socket.on('data', data => {
        try {
            // Log first packet of every connection to diagnose video stream
            if (buffer.length === 0) {
                console.log(`[TCP] First data from ${remote}: ${data.slice(0,32).toString('hex')} (${data.length} bytes)`);
            }
            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length - 4) {

                // ── Stream data packet (T/98 §5.5.3 — magic 0x30316364 = "01cd") ──
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
                    if (unescaped.length < 13) { offset = end + 1; continue; }

                    // ── Checksum validation (XOR of all bytes except checksum itself) ──
                    // Last byte of unescaped is the checksum
                    const csReceived = unescaped[unescaped.length - 1];
                    let csCalc = 0;
                    for (let ci = 0; ci < unescaped.length - 1; ci++) csCalc ^= unescaped[ci];
                    if (csCalc !== csReceived) {
                        // Not a valid JT/T 808 frame — skip this 0x7E and keep scanning
                        offset++;
                        continue;
                    }

                    // Strip checksum byte before parsing
                    const frame = unescaped.slice(0, -1);
                    if (frame.length < 12) { offset = end + 1; continue; }

                    const msgId = frame.readUInt16BE(0);
                    phone = frame.slice(4, 10)
                        .map(b => `${(b >> 4) & 0x0F}${b & 0x0F}`)
                        .join('').replace(/^0+/, '');
                    const seq  = frame.readUInt16BE(10);
                    const body = frame.slice(12);
                    console.log(`[signalling] msgId: 0x${msgId.toString(16).padStart(4,'0')} phone: ${phone}`);

                    if (msgId === 0x0100) {
                        // Registration
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    } else if (msgId === 0x0102) {
                        // Auth success → send ACK + video stream request
                        socket.write(buildAck(phone, seq, msgId));
                        const videoReq = buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1);
                        console.log(`[VideoReq] Sending 0x9101 to device: IP=${CONFIG.serverIp} port=${CONFIG.tcpPort} ch=1`);
                        console.log(`[VideoReq] Packet hex: ${videoReq.toString('hex')}`);
                        socket.write(videoReq);

                    } else if (msgId === 0x1205) {
                        // Terminal's recording list response (T/98 §5.6.2)
                        socket.write(buildAck(phone, seq, msgId));
                        const recordings = parseRecordingList(body);
                        wsBroadcast({ type: 'recordings', data: recordings });

                    } else if (msgId === 0x0200) {
                        // Location report — query recordings on the FIRST report only
                        // (device is fully booted and SD card indexed by this point)
                        socket.write(buildAck(phone, seq, msgId));
                        if (!socket._recordingsQueried) {
                            socket._recordingsQueried = true;
                            // Retry up to 3 times with increasing delays
                            // Device may still be indexing SD card
                            let attempt = 0;
                            const queryRecordings = () => {
                                attempt++;
                                console.log(`[Recordings] Query attempt ${attempt}...`);
                                socket.write(buildQueryRecordings(phone));
                            };
                            setTimeout(queryRecordings, 1000);
                            setTimeout(queryRecordings, 10000);
                            setTimeout(queryRecordings, 30000);
                        }

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
                        const t   = 22;
                        const dt  = `20${String(bcd(body[t])).padStart(2,'0')}-${String(bcd(body[t+1])).padStart(2,'0')}-${String(bcd(body[t+2])).padStart(2,'0')} ${String(bcd(body[t+3])).padStart(2,'0')}:${String(bcd(body[t+4])).padStart(2,'0')}:${String(bcd(body[t+5])).padStart(2,'0')}`;

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
                        wsBroadcast(locationData);

                        // GPS log
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

                    } else if (msgId === 0x0001) {
                        // General response from device — ACK to our commands, no reply needed
                        const replySeq    = body.readUInt16BE(0);
                        const replyMsgId  = body.readUInt16BE(2);
                        const result      = body[4];
                        const resultStr   = ['Success','Failed','MsgErr','NotSupported','AlarmACK','Update'][result] || `0x${result.toString(16)}`;
                        console.log(`[signalling] Device ACK: replyTo=0x${replyMsgId.toString(16).padStart(4,'0')} seq=${replySeq} result=${resultStr}`);

                    } else if (msgId === 0x0002) {
                        // Heartbeat — just ACK it
                        socket.write(buildAck(phone, seq, msgId));

                    } else if (msgId === 0x1003) {
                        // Terminal uploads audio/video attributes (T/98 §5.3.3)
                        // Log the codec info — useful for confirming H.265
                        if (body.length >= 2) {
                            const videoCodec = body[7] || body[1];
                            const codecName  = {98:'H.264', 99:'H.265/HEVC', 100:'AVS'}[videoCodec] || `code${videoCodec}`;
                            console.log(`[signalling] Terminal AV attributes: videoCodec=${codecName}`);
                        }
                        socket.write(buildAck(phone, seq, msgId));

                    } else {
                        // Truly unknown — log but still ACK
                        console.log(`[signalling] ⚠️  Unhandled msgId=0x${msgId.toString(16).padStart(4,'0')} bodyLen=${body.length}`);
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                offset++;
            }

            buffer = buffer.slice(offset);

        } catch (err) {
            console.error('Error processing data:', err.message);
        }
    });

    socket.on('close', () => console.log(`Device disconnected: ${remote}`));
    socket.on('error', err => console.error(`Socket error: ${err.message}`));
});

tcpServer.listen(CONFIG.tcpPort, () => {
    console.log(`✓ TCP server on :${CONFIG.tcpPort}`);
});