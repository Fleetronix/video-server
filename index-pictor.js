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

wss.on('connection', ws => {
    ws.on('message', raw => {
        try {
            const msg = JSON.parse(raw);
            if (msg.type === 'setQuality') {
                wsBroadcast({ type: 'qualityChanged', quality: msg.quality, auto: !!msg.auto });
            }
        } catch (_) {}
    });
});

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

// ── FFmpeg quality profiles ───────────────────────────────────────────────────
const FFMPEG_PROFILES = {
    main: {
        extraArgs: [
            '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
            '-c:v', 'libx264', '-preset', 'ultrafast', '-tune', 'zerolatency',
            '-profile:v', 'baseline', '-level', '3.1',
            '-g', '25', '-keyint_min', '25', '-sc_threshold', '0',
        ],
        suffix: '',
    },
    sub: {
        extraArgs: [
            '-vf', 'scale=480:trunc(ow/a/2)*2',
            '-c:v', 'libx264', '-preset', 'ultrafast', '-tune', 'zerolatency',
            '-profile:v', 'baseline', '-level', '3.0',
            '-b:v', '400k', '-maxrate', '500k', '-bufsize', '800k',
            '-g', '25', '-keyint_min', '25', '-sc_threshold', '0',
        ],
        suffix: '_sub',
    },
};

function startFFmpeg(channel, quality = 'main') {
    const profile = FFMPEG_PROFILES[quality] || FFMPEG_PROFILES.main;
    const seg     = `./public/ch${channel}${profile.suffix}_%05d.ts`;
    const m3u8    = `./public/ch${channel}${profile.suffix}.m3u8`;

    const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
        '-fflags',          '+genpts+discardcorrupt+igndts',
        '-err_detect',      'ignore_err',
        '-use_wallclock_as_timestamps', '1',
        '-f',               'hevc',
        '-i',               'pipe:0',
        ...profile.extraArgs,
        '-an',
        '-f',               'hls',
        '-hls_time',        '1',
        '-hls_list_size',   '6',
        '-hls_flags',       'delete_segments+append_list+independent_segments',
        '-hls_segment_type','mpegts',
        '-hls_segment_filename', seg,
        m3u8,
    ]);

    ffmpeg.stderr.on('data', d => {
        const msg = d.toString().trim();
        if (/error|invalid/i.test(msg) && !/ref with POC|frame RPS|undecodable NALU/i.test(msg)) {
            console.error(`FFmpeg ch${channel}[${quality}]: ${msg}`);
        }
    });

    ffmpeg.on('close', code => {
        console.log(`FFmpeg ch${channel}[${quality}] exited (${code}), restarting in 2s...`);
        const ch = channels[channel];
        if (ch) {
            if (quality === 'main') { ch.gotIFrame = false; ch.draining = false; ch.frameQueue = []; }
            else                   { ch.gotIFrameSub = false; ch.drainingSub = false; ch.frameQueueSub = []; }
        }
        setTimeout(() => {
            if (!channels[channel]) return;
            if (quality === 'main') {
                channels[channel].ffmpeg = startFFmpeg(channel, 'main');
            } else {
                channels[channel].ffmpegSub = startFFmpeg(channel, 'sub');
                const subM3u8 = `./public/ch${channel}_sub.m3u8`;
                const pollReady = setInterval(() => {
                    if (fs.existsSync(subM3u8)) {
                        clearInterval(pollReady);
                        wsBroadcast({ type: 'streamReady', stream: 'sub', channel });
                    }
                }, 500);
            }
        }, 2000);
    });

    return ffmpeg;
}

const channels = {
    1: {
        ffmpeg: null, gotIFrame: false, subpackets: [], subpacketDType: 0,
        frameTimer: null, frameQueue: [], draining: false,
        ffmpegSub: null, gotIFrameSub: false,
        frameQueueSub: [], drainingSub: false, frameTimerSub: null,
    },
};
channels[1].ffmpeg = startFFmpeg(1, 'main');
console.log('✓ Main FFmpeg started');

// ── H.265 keyframe detection ──────────────────────────────────────────────────
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
            i = i + sc;
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
const TARGET_FPS        = 25;
const FRAME_INTERVAL_MS = 1000 / TARGET_FPS;

function scheduleQueue(ch) {
    if (ch.draining || ch.frameQueue.length === 0) return;
    ch.draining = true;
    function drainOne() {
        if (ch.frameQueue.length === 0) { ch.draining = false; return; }
        const frame = ch.frameQueue.shift();
        if (ch.ffmpeg && ch.ffmpeg.stdin.writable) ch.ffmpeg.stdin.write(frame);
        // Drop old non-keyframes if queue builds up beyond 2s
        const maxQ = TARGET_FPS * 2;
        while (ch.frameQueue.length > maxQ) {
            const dropped = ch.frameQueue.shift();
            if (isH265Keyframe(dropped)) { ch.frameQueue.unshift(dropped); break; }
        }
        ch.frameTimer = setTimeout(drainOne, FRAME_INTERVAL_MS);
    }
    drainOne();
}

function scheduleQueueSub(ch) {
    if (ch.drainingSub || ch.frameQueueSub.length === 0) return;
    ch.drainingSub = true;
    function drainOne() {
        if (ch.frameQueueSub.length === 0) { ch.drainingSub = false; return; }
        const frame = ch.frameQueueSub.shift();
        if (ch.ffmpegSub && ch.ffmpegSub.stdin.writable) ch.ffmpegSub.stdin.write(frame);
        const maxQ = TARGET_FPS * 2;
        while (ch.frameQueueSub.length > maxQ) {
            const dropped = ch.frameQueueSub.shift();
            if (isH265Keyframe(dropped)) { ch.frameQueueSub.unshift(dropped); break; }
        }
        ch.frameTimerSub = setTimeout(drainOne, FRAME_INTERVAL_MS);
    }
    drainOne();
}

function deliverFrame(frameData, channel, dataType) {
    const ch = channels[channel];
    if (!ch) return;
    if (dataType === 3 || dataType === 4) return;

    const keyframe = (dataType === 0) || isH265Keyframe(frameData);
    const frame = (frameData[0]===0 && frameData[1]===0 && frameData[2]===0 && frameData[3]===1)
        ? frameData
        : Buffer.concat([Buffer.from([0,0,0,1]), frameData]);

    // Main stream
    if (keyframe) {
        if (!ch.gotIFrame) console.log(`ch${channel} ✅ First keyframe — stream starting`);
        ch.gotIFrame = true;
    }
    if (ch.gotIFrame && ch.ffmpeg && ch.ffmpeg.stdin.writable) {
        ch.frameQueue.push(frame);
        scheduleQueue(ch);
    }

    // Sub stream — lazy start on first keyframe
    if (keyframe && !ch.ffmpegSub) {
        console.log(`ch${channel} Starting sub-stream FFmpeg...`);
        ch.ffmpegSub = startFFmpeg(channel, 'sub');
        const subM3u8 = `./public/ch${channel}_sub.m3u8`;
        const pollReady = setInterval(() => {
            if (fs.existsSync(subM3u8)) {
                clearInterval(pollReady);
                wsBroadcast({ type: 'streamReady', stream: 'sub', channel });
            }
        }, 500);
    }
    if (keyframe) ch.gotIFrameSub = true;
    if (ch.gotIFrameSub && ch.ffmpegSub && ch.ffmpegSub.stdin.writable) {
        ch.frameQueueSub.push(frame);
        scheduleQueueSub(ch);
    }
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
    // FIX: pad to 12 digits so phone always encodes as exactly 6 BCD bytes
    const phonePadded = phone.padStart(12, '0');
    Buffer.from(phonePadded.match(/.{2}/g).map(h => parseInt(h, 16))).copy(header, 4);
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
    body[6 + N] = 0; // Audio+Video
    body[7 + N] = 0; // main stream
    return buildFrame(0x9101, body, phone);
}

function buildQueryRecordings(phone) {
    const body = Buffer.alloc(24, 0);
    body[0]  = 0;   // FIX: 0 = all channels (vendor uses 0, not 1)
    body[21] = 3;   // avType 3 = Video OR Audio+Video
    body[22] = 0;   // all stream types
    body[23] = 0;   // all storage
    return buildFrame(0x9205, body, phone);
}

function parseBCD6(buf, offset) {
    const b = i => ((buf[offset+i] >> 4) * 10 + (buf[offset+i] & 0x0F));
    return `20${String(b(0)).padStart(2,'0')}-${String(b(1)).padStart(2,'0')}-${String(b(2)).padStart(2,'0')} ${String(b(3)).padStart(2,'0')}:${String(b(4)).padStart(2,'0')}:${String(b(5)).padStart(2,'0')}`;
}

function parseRecordingList(body) {
    if (body.length < 6) return [];
    const seq   = body.readUInt16BE(0);
    const total = body.readUInt32BE(2);
    console.log(`[Recordings] seq=${seq} total=${total} bodyLen=${body.length}`);
    if (total === 0) return [];

    const recordings = [];
    let i = 6;
    while (i + 28 <= body.length) {
        const ch         = body[i];
        const startTime  = parseBCD6(body, i + 1);
        const endTime    = parseBCD6(body, i + 7);
        const avType     = body[i + 21];
        const streamType = body[i + 22];
        const memType    = body[i + 23];
        const fileSize   = body.readUInt32BE(i + 24);
        console.log(`[Recordings] ch=${ch} start=${startTime} end=${endTime} size=${fileSize}`);
        recordings.push({ ch, startTime, endTime, avType, streamType, memType, size: fileSize });
        i += 28;
    }
    console.log(`[Recordings] parsed ${recordings.length} of ${total}`);
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
    let phone  = null;

    socket.on('data', data => {
        try {
            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length - 4) {

                // ── Stream data packet (magic 0x30 0x31 0x63 0x64) ───────────
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

                // ── Signalling packet (0x7E framing) ─────────────────────────
                if (buffer[offset] === 0x7E) {
                    const end = buffer.indexOf(0x7E, offset + 1);
                    if (end === -1) break;

                    const inner     = buffer.slice(offset + 1, end);
                    const unescaped = unescapeBuffer(inner);
                    if (unescaped.length < 13) { offset = end + 1; continue; }

                    const csReceived = unescaped[unescaped.length - 1];
                    let csCalc = 0;
                    for (let ci = 0; ci < unescaped.length - 1; ci++) csCalc ^= unescaped[ci];
                    if (csCalc !== csReceived) { offset++; continue; }

                    const frame = unescaped.slice(0, -1);
                    if (frame.length < 12) { offset = end + 1; continue; }

                    const msgId = frame.readUInt16BE(0);
                    phone = frame.slice(4, 10)
                        .map(b => `${(b >> 4) & 0x0F}${b & 0x0F}`)
                        .join('').replace(/^0+/, '');
                    const seq  = frame.readUInt16BE(10);
                    const body = frame.slice(12);
                    console.log(`[signalling] 0x${msgId.toString(16).padStart(4,'0')} phone:${phone}`);

                    if (msgId === 0x0100) {
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));

                    } else if (msgId === 0x0102) {
                        socket.write(buildAck(phone, seq, msgId));
                        const req = buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1);
                        console.log(`[VideoReq] → IP=${CONFIG.serverIp} port=${CONFIG.tcpPort}`);
                        socket.write(req);

                    } else if (msgId === 0x1205) {
                        socket.write(buildAck(phone, seq, msgId));
                        const recordings = parseRecordingList(body);
                        wsBroadcast({ type: 'recordings', data: recordings });

                    } else if (msgId === 0x0200) {
                        socket.write(buildAck(phone, seq, msgId));

                        // Query recordings: 1s, 15s, 60s after first location report
                        if (!socket._recordingsQueried) {
                            socket._recordingsQueried = true;
                            const q = n => {
                                console.log(`[Recordings] Query attempt ${n}...`);
                                if (socket.writable) socket.write(buildQueryRecordings(phone));
                            };
                            setTimeout(() => q(1), 1000);
                            setTimeout(() => q(2), 15000);
                            setTimeout(() => q(3), 60000);
                        }

                        const alarmFlags = body.readUInt32BE(0);
                        const statusBits = body.readUInt32BE(4);
                        const latRaw     = body.readUInt32BE(8);
                        const lonRaw     = body.readUInt32BE(12);
                        const elevation  = body.readUInt16BE(16);
                        const speed      = body.readUInt16BE(18) / 10;
                        const direction  = body.readUInt16BE(20);
                        const lat        = latRaw / 1e6 * (!!(statusBits & (1<<2)) ? -1 : 1);
                        const lon        = lonRaw / 1e6 * (!!(statusBits & (1<<3)) ? -1 : 1);
                        const accOn      = !!(statusBits & (1<<0));
                        const located    = !!(statusBits & (1<<1));
                        const bcd        = b => ((b >> 4) * 10 + (b & 0x0F));
                        const t          = 22;
                        const dt         = `20${String(bcd(body[t])).padStart(2,'0')}-${String(bcd(body[t+1])).padStart(2,'0')}-${String(bcd(body[t+2])).padStart(2,'0')} ${String(bcd(body[t+3])).padStart(2,'0')}:${String(bcd(body[t+4])).padStart(2,'0')}:${String(bcd(body[t+5])).padStart(2,'0')}`;

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
                        console.log(`[GPS] lat=${lat} lon=${lon} speed=${speed} dt=${dt}`);
                        wsBroadcast(locationData);

                        const fileName = `gps_log_${new Date().toISOString().slice(0,10)}.txt`;
                        const row = [
                            phone, dt, lat, lon, speed, direction, elevation,
                            accOn ? 'ON' : 'OFF', located ? 'YES' : 'NO',
                            locationData.mileage || '--', locationData.voltage || '--',
                            locationData.satellites || '--',
                        ].join(',') + '\n';
                        fs.appendFile(fileName, row, err => { if (err) console.error('[GPS LOG]', err.message); });

                    } else if (msgId === 0x0001) {
                        const rSeq = body.readUInt16BE(0);
                        const rId  = body.readUInt16BE(2);
                        const res  = body[4];
                        console.log(`[signalling] DeviceACK → 0x${rId.toString(16)} seq=${rSeq} result=${res}`);

                    } else if (msgId === 0x0002) {
                        socket.write(buildAck(phone, seq, msgId));

                    } else if (msgId === 0x1003) {
                        if (body.length >= 8) {
                            const codec = {98:'H.264', 99:'H.265', 100:'AVS'}[body[7]] || `code${body[7]}`;
                            console.log(`[signalling] AV attributes: codec=${codec}`);
                        }
                        socket.write(buildAck(phone, seq, msgId));

                    } else {
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                // ── Unknown byte: fast-resync to next known header ────────────
                // FIX: use indexOf instead of offset++ to avoid scanning
                // tens of thousands of bytes one at a time (was freezing Node.js)
                {
                    const nextStream = buffer.indexOf(Buffer.from([0x30,0x31,0x63,0x64]), offset + 1);
                    const next7E     = buffer.indexOf(0x7E, offset + 1);
                    let   nextSync   = -1;
                    if      (nextStream !== -1 && next7E !== -1) nextSync = Math.min(nextStream, next7E);
                    else if (nextStream !== -1)                  nextSync = nextStream;
                    else if (next7E     !== -1)                  nextSync = next7E;

                    if (nextSync !== -1) { offset = nextSync; }
                    else                 { break; }
                }
            }

            buffer = buffer.slice(offset);

        } catch (err) {
            console.error('TCP error:', err.message, err.stack);
        }
    });

    socket.on('close', () => console.log(`Device disconnected: ${remote}`));
    socket.on('error', err => console.error(`Socket error: ${err.message}`));
});

tcpServer.listen(CONFIG.tcpPort, () => {
    console.log(`✓ TCP server on :${CONFIG.tcpPort}`);
});