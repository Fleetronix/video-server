'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// playback-download.js  —  Recording download via 0x9201 (remote playback)
//
// How it works:
//   1. Browser sends { type:'get_recordings', phone, startTime, endTime }
//   2. Server sends 0x9205 → camera returns 0x1205 (recording list)
//   3. Browser sends { type:'download_clip', phone, startTime, endTime, ch }
//   4. Server sends 0x9201 → camera streams clip back over port 3007
//   5. Server saves stream to .mp4 via FFmpeg → notifies browser
//
// No FTP. No outbound connections from camera. Works on 4G NAT.
// ─────────────────────────────────────────────────────────────────────────────

const fs    = require('fs');
const path  = require('path');
const { spawn } = require('child_process');

// ── Module state ──────────────────────────────────────────────────────────────
let _wss         = null;   // WebSocketServer reference
let _tcpSockets  = null;   // { [phone]: socket }
let _buildFrame  = null;   // buildFrame(msgId, body, phone) from main
let _buildAck    = null;   // buildAck(phone, seq, msgId) from main
let _serverIp    = null;
let _tcpPort     = 3007;
let _clipsDir    = './clips';

// Active download sessions: { [phone]: { ch, startTime, endTime, ffmpeg, active, tsCounter, patPmtSent, gotIFrame, subpackets } }
const _sessions  = {};

const _log  = (...a) => console.log('[PLAYBACK]', ...a);
const _err  = (...a) => console.error('[PLAYBACK]', ...a);

// ── BCD helpers ───────────────────────────────────────────────────────────────
function _bcdEncode(yy, mo, dd, hh, mm, ss) {
    return Buffer.from([
        ((Math.floor(yy / 10) << 4) | (yy % 10)),
        ((Math.floor(mo / 10) << 4) | (mo % 10)),
        ((Math.floor(dd / 10) << 4) | (dd % 10)),
        ((Math.floor(hh / 10) << 4) | (hh % 10)),
        ((Math.floor(mm / 10) << 4) | (mm % 10)),
        ((Math.floor(ss / 10) << 4) | (ss % 10)),
    ]);
}

function _parseDateTime(dt) {
    const [date, time = '00:00:00'] = dt.split(' ');
    const [y, mo, d] = date.split('-').map(Number);
    const [h, mi, s] = time.split(':').map(Number);
    return { y, mo, d, h, mi, s };
}

function _bcdDecode(b) { return ((b >> 4) * 10 + (b & 0x0F)); }

function _parseBCDTime(buf, off) {
    const yy = _bcdDecode(buf[off]);
    const mo = _bcdDecode(buf[off + 1]);
    const dd = _bcdDecode(buf[off + 2]);
    const hh = _bcdDecode(buf[off + 3]);
    const mm = _bcdDecode(buf[off + 4]);
    const ss = _bcdDecode(buf[off + 5]);
    return `20${String(yy).padStart(2,'0')}-${String(mo).padStart(2,'0')}-${String(dd).padStart(2,'0')} ${String(hh).padStart(2,'0')}:${String(mm).padStart(2,'0')}:${String(ss).padStart(2,'0')}`;
}

// ── 0x9205 — query recording list ─────────────────────────────────────────────
function _build9205(phone, channel, startTime, endTime) {
    const s = _parseDateTime(startTime);
    const e = _parseDateTime(endTime);
    const body = Buffer.alloc(23);
    body[0] = channel;
    _bcdEncode(s.y % 100, s.mo, s.d, s.h, s.mi, s.s).copy(body, 1);
    _bcdEncode(e.y % 100, e.mo, e.d, e.h, e.mi, e.s).copy(body, 7);
    body.fill(0x00, 13, 21);  // alarm filter = none
    body[21] = 0;              // avType: audio+video
    body[22] = 0;              // streamType: all
    return _buildFrame(0x9205, body, phone);
}

// ── 0x9201 — remote playback request ─────────────────────────────────────────
// Camera streams the recording back over TCP to our server port 3007.
function _build9201(phone, channel, startTime, endTime) {
    const s = _parseDateTime(startTime);
    const e = _parseDateTime(endTime);
    const ipBuf = Buffer.from(_serverIp, 'ascii');
    const n = ipBuf.length;

    // Table 24: ipLen(1) ip(n) tcpPort(2) udpPort(2) ch(1)
    //           avType(1) streamType(1) memType(1) playMode(1) speed(1)
    //           startBCD(6) endBCD(6)
    const body = Buffer.alloc(1 + n + 2 + 2 + 1 + 1 + 1 + 1 + 1 + 1 + 6 + 6);
    let p = 0;
    body[p++] = n;
    ipBuf.copy(body, p); p += n;
    body.writeUInt16BE(_tcpPort, p); p += 2;
    body.writeUInt16BE(0,        p); p += 2;  // UDP = 0 (TCP only)
    body[p++] = channel;
    body[p++] = 0;    // avType: audio+video
    body[p++] = 1;    // streamType: main stream
    body[p++] = 99;   // memType: matches camera's actual memType
    body[p++] = 0;    // playMode: normal playback
    body[p++] = 0;    // speed: N/A for normal
    _bcdEncode(s.y % 100, s.mo, s.d, s.h, s.mi, s.s).copy(body, p); p += 6;
    _bcdEncode(e.y % 100, e.mo, e.d, e.h, e.mi, e.s).copy(body, p);

    _log(`0x9201 ch:${channel} ${startTime} → ${endTime} → ${_serverIp}:${_tcpPort}`);
    return _buildFrame(0x9201, body, phone);
}

// ── 0x9202 — stop playback ────────────────────────────────────────────────────
function _build9202(phone, channel) {
    const body = Buffer.alloc(9);
    body[0] = channel;
    body[1] = 2;  // 2 = end playback
    return _buildFrame(0x9202, body, phone);
}

// ── Parse 0x1205 recording list body ─────────────────────────────────────────
function _parse1205(body) {
    if (body.length < 6) return { total: 0, recordings: [] };
    const total = body.readUInt32BE(2);
    const recordings = [];
    let p = 6;
    while (p + 28 <= body.length) {
        const ch        = body[p];
        const startTime = _parseBCDTime(body, p + 1);
        const endTime   = _parseBCDTime(body, p + 7);
        const avType    = body[p + 21];
        const streamType= body[p + 22];
        const memType   = body[p + 23];
        const fileSize  = body.readUInt32BE(p + 24);
        recordings.push({ ch, startTime, endTime, avType, streamType, memType, fileSize });
        p += 28;
    }
    return { total, recordings };
}

// ── FFmpeg — save playback stream to MP4 ─────────────────────────────────────
function _startSaveFFmpeg(phone, ch, startTime) {
    if (!fs.existsSync(_clipsDir)) fs.mkdirSync(_clipsDir, { recursive: true });

    const tag     = startTime.replace(/[: -]/g, '');
    const tsPath  = path.join(_clipsDir, `clip_${phone}_ch${ch}_${tag}.ts`);
    const mp4Path = path.join(_clipsDir, `clip_${phone}_ch${ch}_${tag}.mp4`);

    // Remove old files if exist
    [tsPath, mp4Path].forEach(f => { try { fs.unlinkSync(f); } catch(_) {} });

    const ffmpeg = spawn('/usr/local/bin/ffmpeg', [
        '-y',
        '-fflags',          '+genpts+discardcorrupt+igndts',
        '-err_detect',      'ignore_err',
        '-f',               'mpegts',
        '-probesize',       '500000',
        '-analyzeduration', '1000000',
        '-i',               'pipe:0',
        '-c:v',             'copy',   // copy stream — no re-encode, faster
        '-an',                         // no audio for now
        '-movflags',        '+faststart',
        mp4Path,
    ]);

    ffmpeg.stderr.on('data', d => {
        const m = d.toString().trim();
        if (m.includes('error') || m.includes('Error')) {
            _err('[FFmpeg]', m);
        }
    });

    ffmpeg.on('close', code => {
        _log(`FFmpeg closed code=${code} → ${mp4Path}`);
        const sess = _sessions[phone];
        if (!sess) return;

        if (code === 0 && fs.existsSync(mp4Path)) {
            const stat = fs.statSync(mp4Path);
            _log(`Clip saved: ${mp4Path} (${(stat.size/1024/1024).toFixed(1)} MB)`);
            _broadcast({ type: 'clip_ready', url: `/clips/${path.basename(mp4Path)}`, phone, startTime: sess.startTime, endTime: sess.endTime });
        } else {
            // Fallback — try .ts file
            if (fs.existsSync(tsPath)) {
                _broadcast({ type: 'clip_ready', url: `/clips/${path.basename(tsPath)}`, phone, startTime: sess.startTime, endTime: sess.endTime });
            } else {
                _broadcast({ type: 'clip_error', message: 'FFmpeg conversion failed', phone });
            }
        }
        delete _sessions[phone];
    });

    return { ffmpeg, tsPath, mp4Path };
}

// ── MPEG-TS wrapping (same as main server) ────────────────────────────────────
const TS_PACKET_SIZE = 188;
const VIDEO_PID      = 256;
const PMT_PID        = 4096;

function _buildPAT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0]=0x47; pkt[1]=0x40; pkt[2]=0x00; pkt[3]=0x10; pkt[4]=0x00;
    const s = pkt.slice(5);
    s[0]=0x00; s[1]=0xB0; s[2]=0x0D; s[3]=0x00; s[4]=0x01; s[5]=0xC1;
    s[6]=0x00; s[7]=0x00; s[8]=0x00; s[9]=0x01;
    s[10]=(PMT_PID>>8)|0xE0; s[11]=PMT_PID&0xFF;
    return pkt;
}

function _buildPMT() {
    const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
    pkt[0]=0x47; pkt[1]=0x40|(PMT_PID>>8)&0x1F; pkt[2]=PMT_PID&0xFF; pkt[3]=0x10; pkt[4]=0x00;
    const s = pkt.slice(5);
    s[0]=0x02; s[1]=0xB0; s[2]=0x12; s[3]=0x00; s[4]=0x01; s[5]=0xC1;
    s[6]=0x00; s[7]=0x00;
    s[8]=0xE0|(VIDEO_PID>>8)&0x1F; s[9]=VIDEO_PID&0xFF; s[10]=0xF0; s[11]=0x00;
    s[12]=0x42;  // AVS stream type
    s[13]=0xE0|(VIDEO_PID>>8)&0x1F; s[14]=VIDEO_PID&0xFF; s[15]=0xF0; s[16]=0x00;
    return pkt;
}

function _wrapFrameInTS(frameData, counter) {
    const pesHdr = Buffer.from([0x00,0x00,0x01,0xE0,0x00,0x00,0x80,0x00,0x00]);
    const pes = Buffer.concat([pesHdr, frameData]);
    const packets = []; let pos=0, first=true, ctr=counter;
    while (pos < pes.length) {
        const pkt = Buffer.alloc(TS_PACKET_SIZE, 0xFF);
        pkt[0]=0x47; pkt[1]=(first?0x40:0x00)|((VIDEO_PID>>8)&0x1F);
        pkt[2]=VIDEO_PID&0xFF; pkt[3]=0x10|(ctr&0x0F); ctr=(ctr+1)&0x0F;
        const chunk = pes.slice(pos, pos + TS_PACKET_SIZE - 4);
        chunk.copy(pkt, 4); pos += chunk.length; first=false;
        packets.push(pkt);
    }
    return { packets, nextCounter: ctr };
}

// ── Handle one video frame from playback stream ───────────────────────────────
function _handlePlaybackFrame(phone, frameData, dataType) {
    const sess = _sessions[phone];
    if (!sess || !sess.active) return;

    const isVideo = (dataType === 0 || dataType === 1 || dataType === 2);
    if (!isVideo) return;

    if (dataType === 0) {
        // I-frame
        if (!sess.gotIFrame) {
            _log(`First I-frame received for ${phone} — writing to file`);
            sess.gotIFrame = true;
        }
        // Write PAT+PMT once before first I-frame
        if (!sess.patPmtSent) {
            sess.ffmpeg.stdin.write(_buildPAT());
            sess.ffmpeg.stdin.write(_buildPMT());
            sess.patPmtSent = true;
        }
    } else {
        if (!sess.gotIFrame) return;
    }

    if (!sess.ffmpeg || !sess.ffmpeg.stdin.writable) return;

    const { packets, nextCounter } = _wrapFrameInTS(frameData, sess.tsCounter);
    sess.tsCounter = nextCounter;
    for (const pkt of packets) sess.ffmpeg.stdin.write(pkt);

    // Reset inactivity timer
    if (sess.inactivityTimer) clearTimeout(sess.inactivityTimer);
    sess.inactivityTimer = setTimeout(() => {
        _log(`No frames for 8s — ending clip for ${phone}`);
        _stopSession(phone);
    }, 8000);
}

// ── Reassemble subpackets ─────────────────────────────────────────────────────
function _processPlaybackPacket(phone, rawData, dataType, subpktMarker) {
    const sess = _sessions[phone];
    if (!sess) return;

    if (subpktMarker === 0) {
        _handlePlaybackFrame(phone, rawData, dataType);
    } else if (subpktMarker === 1) {
        sess.subpackets = [rawData];
    } else if (subpktMarker === 3) {
        if (sess.subpackets.length > 0) sess.subpackets.push(rawData);
    } else if (subpktMarker === 2) {
        if (sess.subpackets.length > 0) {
            sess.subpackets.push(rawData);
            const complete = Buffer.concat(sess.subpackets);
            sess.subpackets = [];
            _handlePlaybackFrame(phone, complete, dataType);
        }
    }
}

// ── Stop a download session ───────────────────────────────────────────────────
function _stopSession(phone) {
    const sess = _sessions[phone];
    if (!sess) return;
    sess.active = false;
    if (sess.inactivityTimer) clearTimeout(sess.inactivityTimer);
    if (sess.ffmpeg && sess.ffmpeg.stdin.writable) {
        sess.ffmpeg.stdin.end();
    }
    // Send stop command to camera
    const socket = _tcpSockets[phone];
    if (socket && !socket.destroyed) {
        socket.write(_build9202(phone, sess.ch));
        _log(`Sent 0x9202 stop to ${phone}`);
    }
}

// ── Broadcast to all WS clients ───────────────────────────────────────────────
function _broadcast(obj) {
    if (!_wss) return;
    const msg = JSON.stringify(obj);
    _wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

// ── Pending 0x1205 response handler ──────────────────────────────────────────
let _pending1205 = null;  // { phone, resolve }

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/**
 * init — call once at startup
 */
function init(opts) {
    _wss        = opts.wss;
    _tcpSockets = opts.tcpSockets;
    _buildFrame = opts.buildFrame;
    _buildAck   = opts.buildAck;
    _serverIp   = opts.serverIp;
    _tcpPort    = opts.tcpPort || 3007;
    _clipsDir   = opts.clipsDir || './clips';

    if (!fs.existsSync(_clipsDir)) fs.mkdirSync(_clipsDir, { recursive: true });
    _log(`init — serverIp:${_serverIp} tcpPort:${_tcpPort} clipsDir:${_clipsDir}`);
}

/**
 * handleWsMessage — call from main WS message handler
 * Returns true if the message was handled, false otherwise.
 */
function handleWsMessage(msg, ws) {

    // ── get_recordings: query recording list for a time window ────────────────
    if (msg.type === 'get_recordings') {
        const { phone, startTime, endTime, ch = 1 } = msg;

        if (!phone || !_tcpSockets[phone] || _tcpSockets[phone].destroyed) {
            ws.send(JSON.stringify({ type: 'error', message: 'Camera not connected' }));
            return true;
        }

        _log(`get_recordings phone:${phone} ${startTime} → ${endTime}`);

        // Store pending request so handleSignalling can resolve it
        _pending1205 = { phone, ws };

        const frame = _build9205(phone, ch, startTime, endTime);
        _tcpSockets[phone].write(frame);
        _log(`Sent 0x9205 to ${phone}`);
        return true;
    }

    // ── download_clip: stream a specific clip from SD card and save ───────────
    if (msg.type === 'download_clip') {
        const { phone, startTime, endTime, ch = 1 } = msg;

        if (!phone || !_tcpSockets[phone] || _tcpSockets[phone].destroyed) {
            ws.send(JSON.stringify({ type: 'error', message: 'Camera not connected' }));
            return true;
        }

        // Stop any existing session for this phone
        if (_sessions[phone]) {
            _log(`Stopping existing session for ${phone}`);
            _stopSession(phone);
            delete _sessions[phone];
        }

        _log(`download_clip phone:${phone} ch:${ch} ${startTime} → ${endTime}`);

        // Start FFmpeg writer
        const { ffmpeg } = _startSaveFFmpeg(phone, ch, startTime);

        // Create session
        _sessions[phone] = {
            ch, startTime, endTime,
            ffmpeg, active: true,
            tsCounter: 0, patPmtSent: false,
            gotIFrame: false, subpackets: [],
            inactivityTimer: null,
        };

        // Send 0x9201
        const frame = _build9201(phone, ch, startTime, endTime);
        _tcpSockets[phone].write(frame);
        _log(`Sent 0x9201 to ${phone}`);

        ws.send(JSON.stringify({ type: 'status', message: '⏳ Downloading clip from camera...' }));
        return true;
    }

    // ── stop_clip: cancel in-progress download ────────────────────────────────
    if (msg.type === 'stop_clip') {
        const { phone } = msg;
        if (phone && _sessions[phone]) {
            _stopSession(phone);
            _log(`stop_clip: stopped session for ${phone}`);
        }
        return true;
    }

    return false;
}

/**
 * handleSignalling — call from main TCP signalling handler
 * Returns true if handled.
 */
function handleSignalling(msgId, body, seq, phone, socket) {

    // ── 0x0001 ACK for 0x9201 ────────────────────────────────────────────────
    if (msgId === 0x0001) {
        const replyMsgId = body.readUInt16BE(2);
        const result     = body[4];
        if (replyMsgId === 0x9201) {
            if (result === 0) {
                _log(`Camera accepted 0x9201 — stream incoming`);
            } else {
                _err(`Camera rejected 0x9201 result:${result}`);
                if (_sessions[phone]) {
                    _broadcast({ type: 'clip_error', message: `Camera rejected playback (result:${result})`, phone });
                    _stopSession(phone);
                    delete _sessions[phone];
                }
            }
            return true;
        }
        return false;
    }

    // ── 0x1205: Recording list response ──────────────────────────────────────
    if (msgId === 0x1205) {
        socket.write(_buildAck(phone, seq, msgId));
        const { total, recordings } = _parse1205(body);
        _log(`0x1205 — ${total} recordings found`);

        if (_pending1205 && _pending1205.phone === phone) {
            _pending1205.ws.send(JSON.stringify({
                type: 'recordings_list',
                phone,
                total,
                recordings,
            }));
            _pending1205 = null;
        } else {
            // Broadcast to all clients
            _broadcast({ type: 'recordings_list', phone, total, recordings });
        }
        return true;
    }

    // ── 0x1201: Playback complete notification ────────────────────────────────
    if (msgId === 0x1201) {
        socket.write(_buildAck(phone, seq, msgId));
        _log(`0x1201 playback complete for ${phone}`);
        if (_sessions[phone]) {
            setTimeout(() => _stopSession(phone), 2000); // small delay to flush last frames
        }
        return true;
    }

    return false;
}

/**
 * handleVideoFrame — call from main TCP data handler when stream packets arrive
 * and a download session is active for this phone.
 * Returns true if the frame was consumed by playback-download.
 */
function handleVideoFrame(phone, rawData, dataType, subpktMarker) {
    if (!_sessions[phone] || !_sessions[phone].active) return false;
    _processPlaybackPacket(phone, rawData, dataType, subpktMarker);
    return true;
}

/**
 * isActive — check if a download session is active for a phone
 */
function isActive(phone) {
    return !!((_sessions[phone] && _sessions[phone].active));
}

module.exports = { init, handleWsMessage, handleSignalling, handleVideoFrame, isActive };
