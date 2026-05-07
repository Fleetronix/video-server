'use strict';

const net  = require('net');
const http = require('http');
const fs   = require('fs');
const path = require('path');
require('dotenv').config();
const { WebSocketServer } = require('ws');
const { spawn } = require('child_process');
const { env } = require('process');

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

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: CONFIG.wsPort });
console.log(`✓ WebSocket on :${CONFIG.wsPort}`);

// ── HTTP server (serves video.html) ──────────────────────────────────────────
http.createServer((req, res) => {
    let filePath;
    
    if (req.url === '/') {
        filePath = './video.html';
    } else if (req.url.startsWith('/public/')) {
        filePath = `.${req.url}`;  // → ./public/ch1.m3u8
    } else {
        filePath = `.${req.url}`;
    }

    console.log('Serving:', filePath); // ← add this to debug

    const ext = path.extname(filePath).toLowerCase();
    const contentTypes = {
        '.html': 'text/html',
        '.js':   'application/javascript',
        '.m3u8': 'application/vnd.apple.mpegurl',
        '.ts':   'video/mp2t',
    };

    fs.readFile(filePath, (err, data) => {
        if (err) { 
            console.error('File not found:', filePath);
            res.writeHead(404); 
            res.end('Not found'); 
            return; 
        }
        res.writeHead(200, {
            'Content-Type': contentTypes[ext] || 'text/plain',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-cache',
        });
        res.end(data);
    });
}).listen(CONFIG.httpPort, () => {
    console.log(`✓ HTTP on :${CONFIG.httpPort}`);
});

// ── FFmpeg for HLS ────────────────────────────────────────────────────────────
function startFFmpeg(channel) {
    const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
    '-fflags',        '+genpts+discardcorrupt+igndts',
    '-err_detect',    'ignore_err',
    '-f',             'h264',
    '-i',             'pipe:0',
    '-c:v',           'libx264',
    '-preset',        'ultrafast',
    '-tune',          'zerolatency',
    '-f',             'hls',
    '-hls_time',      '1',
    '-hls_list_size', '3',
    '-hls_flags',     'delete_segments+append_list',
    `./public/ch${channel}.m3u8`,
]);
    ffmpeg.stderr.on('data', d => console.log(`FFmpeg ch${channel}:`, d.toString().trim()));
    ffmpeg.on('close', code => {
        console.log(`FFmpeg ch${channel} closed, restarting...`);
        setTimeout(() => { channels[channel].ffmpeg = startFFmpeg(channel); }, 1000);
    });
    return ffmpeg;
}

const channels = {
    1: { ffmpeg: null, gotIFrame: false, subpackets: [] },
    2: { ffmpeg: null, gotIFrame: false, subpackets: [] },
};

channels[1].ffmpeg = startFFmpeg(1);
channels[2].ffmpeg = startFFmpeg(2);

// ── Handle complete H.264 frame ───────────────────────────────────────────────
function handleVideoFrame(h264Data, channel, dataType) {
    const ch = channels[channel];
    if (!ch) return;

    if (dataType === 0) {
        ch.gotIFrame = true;
        console.log(`ch${channel} ✅ I_FRAME size:${h264Data.length}`);
    }

    if (!ch.gotIFrame) return;

    // Collect NAL units
    const nalUnits = [];
    let i = 0;
    while (i < h264Data.length - 4) {
        if (h264Data[i] === 0 && h264Data[i+1] === 0 && 
            h264Data[i+2] === 0 && h264Data[i+3] === 1) {
            
            // Find next NAL start
            let next = h264Data.length;
            for (let j = i + 4; j < h264Data.length - 4; j++) {
                if (h264Data[j] === 0 && h264Data[j+1] === 0 && 
                    h264Data[j+2] === 0 && h264Data[j+3] === 1) {
                    next = j;
                    break;
                }
            }

            const nalType = h264Data[i+4] & 0x1F;
            const nalData = h264Data.slice(i, next);
            nalUnits.push({ type: nalType, data: nalData });
            console.log(`ch${channel} NAL type:${nalType} size:${nalData.length}`);
            i = next;
        } else {
            i++;
        }
    }

    // Check if data partitioning (types 2,3,4)
    const hasDP = nalUnits.some(n => n.type === 2);
    if (hasDP) {
        const converted = convertDataPartitioning(h264Data);
        if (ch.ffmpeg && ch.ffmpeg.stdin.writable) {
        ch.ffmpeg.stdin.write(converted);
        }
        return;
        // const part2 = nalUnits.find(n => n.type === 2);
        // const part3 = nalUnits.find(n => n.type === 3);
        // const part4 = nalUnits.find(n => n.type === 4);

        // // Combine all parts into one frame
        // const parts = [part2, part3, part4].filter(Boolean).map(n => n.data);
        // const combined = Buffer.concat(parts);
        // console.log(`ch${channel} DP combined size:${combined.length}`);

        // if (ch.ffmpeg && ch.ffmpeg.stdin.writable) {
        //     ch.ffmpeg.stdin.write(combined);
        // }
        return;
    }

    // Normal frame
    if (ch.ffmpeg && ch.ffmpeg.stdin.writable) {
        ch.ffmpeg.stdin.write(h264Data);
    }
}
function convertDataPartitioning(h264Data) {
    const output = Buffer.from(h264Data); // copy
    let i = 0;
    while (i < output.length - 5) {
        if (output[i] === 0 && output[i+1] === 0 && 
            output[i+2] === 0 && output[i+3] === 1) {
            
            const nalType = output[i+4] & 0x1F;
            const nalRef  = (output[i+4] >> 5) & 0x03;

            if (nalType === 2) {
                // Convert partition A → normal slice (type 1)
                output[i+4] = (nalRef << 5) | 1;
                console.log('Converted NAL 2 → 1');
            } else if (nalType === 3 || nalType === 4) {
                // Remove partition B and C by zeroing them out
                output[i+4] = 0;
            }
            i += 4;
        } else {
            i++;
        }
    }
    return output;
}
// ── Reassemble subpackets ─────────────────────────────────────────────────────
function processVideoPacket(h264Data, channel, dataType, subpktMarker) {
    const ch = channels[channel];
    if (!ch) return;

    console.log(`ch${channel} subpkt:${subpktMarker} dataType:${dataType} bytes:${h264Data.length}`);

    if (subpktMarker === 0) {
        // Atomic — complete frame
        handleVideoFrame(h264Data, channel, dataType);

    } else if (subpktMarker === 1) {
        // First piece
        ch.subpackets = [h264Data];

    } else if (subpktMarker === 3) {
        // Middle piece
        ch.subpackets.push(h264Data);

    } else if (subpktMarker === 2) {
        // Last piece — join all
        ch.subpackets.push(h264Data);
        const complete = Buffer.concat(ch.subpackets);
        ch.subpackets = [];
        console.log(`ch${channel} complete frame NAL: ${complete[0].toString(16)} ${complete[1].toString(16)} ${complete[2].toString(16)} ${complete[3].toString(16)}`);
        handleVideoFrame(complete, channel, dataType);
    }
}

// ── Helper functions ──────────────────────────────────────────────────────────
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
    body.writeUInt16BE(0,          3 + N);
    body[5 + N] = channel;
    body[6 + N] = 1; // video only
    body[7 + N] = 0; // main stream
    return buildFrame(0x9101, body, phone);
}

function parseAdditionalInfo(buf) {
    const result = {};
    let i = 0;
    while (i < buf.length - 2) {
        const id  = buf[i];
        const len = buf[i+1];

        // ← ADD THIS CHECK
        if (i + 2 + len > buf.length) break;

        const val = buf.slice(i+2, i+2+len);

        switch(id) {
            case 0x01:
                if (val.length >= 4) result.mileage = val.readUInt32BE(0) / 10 + ' km';
                break;
            case 0x03:
                if (val.length >= 2) result.sensorSpeed = val.readUInt16BE(0) / 10 + ' km/h';
                break;
            case 0x25:
                if (val.length >= 2) result.voltage = val.readUInt16BE(0) / 10 + ' V';
                break;
            case 0x30:
                if (val.length >= 1) result.signalStrength = val[0];
                break;
            case 0x31:
                if (val.length >= 1) result.satellites = val[0];
                break;
        }
        i += 2 + len;
    }
    return result;
}

// ── TCP server ────────────────────────────────────────────────────────────────
const tcpServer = net.createServer(socket => {
    const remote = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`Device connected: ${remote}`);
    const deviceInfo = {}; // store per device
    let buffer = Buffer.alloc(0);
    let phone  = null;

    socket.on('data', data => {
        try {
            buffer = Buffer.concat([buffer, data]);
            let offset = 0;

            while (offset < buffer.length - 4) {

                // ── Video frame ───────────────────────────────────────────────
                if (buffer[offset]   === 0x30 && buffer[offset+1] === 0x31 &&
                    buffer[offset+2] === 0x63 && buffer[offset+3] === 0x64) {

                    if (offset + 30 > buffer.length) break;

                    const dataBodyLen = buffer.readUInt16BE(offset + 28);
                    if (offset + 30 + dataBodyLen > buffer.length) break;

                    const byte15       = buffer[offset + 15];
                    const dataType     = (byte15 >> 4) & 0x0F;
                    const subpktMarker = byte15 & 0x0F;
                    const channel      = buffer[offset + 14];
                    const h264Data     = buffer.slice(offset + 30, offset + 30 + dataBodyLen);

                    processVideoPacket(h264Data, channel, dataType, subpktMarker);

                    offset += 30 + dataBodyLen;
                    continue;
                }

                // ── Signalling frame ──────────────────────────────────────────
                if (buffer[offset] === 0x7E) {
                    const end = buffer.indexOf(0x7E, offset + 1);
                    if (end === -1) break;

                    const inner     = buffer.slice(offset + 1, end);
                    const unescaped = unescapeBuffer(inner);
                    if (unescaped.length < 12) { offset = end + 1; continue; }

                    const msgId = unescaped.readUInt16BE(0);
                    phone = unescaped.slice(4, 10)
                    .map(b => {
                        const high = (b >> 4) & 0x0F;
                        const low  =  b       & 0x0F;
                        return `${high}${low}`;
                    })
                    .join('')
                    .replace(/^0+/, '');
                    const seq   = unescaped.readUInt16BE(10);
                    const body  = unescaped.slice(12);
                    console.log(`[signalling] msgId: 0x${msgId.toString(16).padStart(4,'0')} phone: ${phone}`);

                    if (msgId === 0x0100) {
                        socket.write(buildRegisterResponse(phone, seq, 0, 'AUTH1234'));
                    } else if (msgId === 0x0102) {
                        // Auth → request video
                        socket.write(buildAck(phone, seq, msgId));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 1));
                        socket.write(buildVideoRequest(phone, CONFIG.serverIp, CONFIG.tcpPort, 2));

                    } 
                    else if (msgId === 0x0200) {
                        socket.write(buildAck(phone, seq, msgId));
                        
                        // Parse location
                        const alarmFlags = body.readUInt32BE(0);
                        const statusBits = body.readUInt32BE(4);
                        const latRaw     = body.readUInt32BE(8);
                        const lonRaw     = body.readUInt32BE(12);
                        const elevation  = body.readUInt16BE(16);
                        const speed      = body.readUInt16BE(18) / 10;
                        const direction  = body.readUInt16BE(20);

                        const south   = !!(statusBits & (1 << 2));
                        const west    = !!(statusBits & (1 << 3));
                        const lat     = latRaw  / 1e6 * (south ? -1 : 1);
                        const lon     = lonRaw  / 1e6 * (west  ? -1 : 1);
                        const accOn   = !!(statusBits & (1 << 0));
                        const located = !!(statusBits & (1 << 1));

                        // Parse BCD time
                        // Replace your existing BCD time parsing with this:
                        const bcd        = b => ((b >> 4) * 10 + (b & 0x0F));
                        const timeOffset = 22; // ← change 21 to 22
                        const yy = bcd(body[timeOffset]);
                        const mo = bcd(body[timeOffset+1]);
                        const dd = bcd(body[timeOffset+2]);
                        const hh = bcd(body[timeOffset+3]);
                        const mm = bcd(body[timeOffset+4]);
                        const ss = bcd(body[timeOffset+5]);
                        const dt = `20${String(yy).padStart(2,'0')}-${String(mo).padStart(2,'0')}-${String(dd).padStart(2,'0')} ${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;
                        
                        console.log(`datetime raw: ${body.slice(timeOffset, timeOffset+6).toString('hex')} → ${dt}`);
                        
                        // Parse alarm flags
                        const alarms = [];
                        if (alarmFlags & (1 << 0)) alarms.push('Emergency');
                        if (alarmFlags & (1 << 1)) alarms.push('Overspeed');
                        if (alarmFlags & (1 << 4)) alarms.push('GNSS Fault');
                        if (alarmFlags & (1 << 5)) alarms.push('GNSS Antenna Cut');
                        if (alarmFlags & (1 << 7)) alarms.push('Low Voltage');
                        if (alarmFlags & (1 << 8)) alarms.push('Power Off');

                        const locationData = {
                            type:      'location',
                            phone,
                            lat,
                            lon,
                            speed,
                            direction,
                            elevation,
                            datetime:  dt,
                            accOn,
                            located,
                            alarms,
                            // Parse additional info items
                            ...parseAdditionalInfo(body.slice(27))
                        };

                        console.log(`[GPS] ${phone} lat=${lat} lon=${lon} speed=${speed}km/h dir=${direction}° dt=${dt}`);
                        // Send to browser
                        wss.clients.forEach(client => {
                            if (client.readyState === 1) {
                                client.send(JSON.stringify(locationData));
                            }
                        });
// ── Save GPS data to file ─────────────────────────────────────────────
                        let fileName = `gps_log_${new Date().toISOString().slice(0,10)}.txt`;
                        const gpsLog = fs.createWriteStream(`./${fileName}`, { flags: 'a' });

                        const info = deviceInfo[phone] || {};

                        const gpsRecord = {
                            phone:         phone,
                            model:         info.model || '--',
                            plate:         info.plate || '--',
                            datetime:      dt,
                            latitude:      lat,
                            longitude:     lon,
                            speed_kmh:     speed,
                            direction_deg: direction,
                            elevation_m:   elevation,
                            acc:           accOn   ? 'ON'  : 'OFF',
                            located:       located ? 'YES' : 'NO',
                            mileage:       locationData.mileage        || '--',
                            voltage:       locationData.voltage        || '--',
                            satellites:    locationData.satellites     || '--',
                            signal:        locationData.signalStrength || '--',
                            sensor_speed:  locationData.sensorSpeed    || '--',
                            oil_circuit:   !!(statusBits & (1<<10)) ? 'CUT'  : 'NORMAL',
                            vehicle_circuit: !!(statusBits & (1<<11)) ? 'CUT' : 'NORMAL',
                            door:          !!(statusBits & (1<<13)) ? 'OPEN'  : 'CLOSED',
                            alarms:        alarmFlags !== 0 ? [
                                (alarmFlags & (1<<0)) ? 'EMERGENCY'   : null,
                                (alarmFlags & (1<<1)) ? 'OVERSPEED'   : null,
                                (alarmFlags & (1<<4)) ? 'GNSS_FAULT'  : null,
                                (alarmFlags & (1<<5)) ? 'ANTENNA_CUT' : null,
                                (alarmFlags & (1<<7)) ? 'LOW_VOLTAGE' : null,
                                (alarmFlags & (1<<8)) ? 'POWER_OFF'   : null,
                            ].filter(Boolean).join('|') : 'NONE',
                        };

                        // Write to file
                        const line = Object.values(gpsRecord).join(',') + '\n';
                        gpsLog.write(line);
                        //console.log(`[GPS LOG] ${gpsRecord.imei} ${gpsRecord.datetime} lat=${gpsRecord.latitude} lon=${gpsRecord.longitude}`);
                        
                    }
                    else {
                        socket.write(buildAck(phone, seq, msgId));
                    }

                    offset = end + 1;
                    continue;
                }

                offset++;
            }

            // Keep unprocessed data
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

