'use strict';

// ─────────────────────────────────────────────────────────────────────────────
// ftp-download.js
//
// Self-contained module that handles the full FTP download flow:
//
//   Browser  →  WS msg {type:'ftp_download', ch, startTime, endTime, phone}
//            ↓
//   Server   →  Sends 0x9206 (T/98 §5.6.5) to camera over TCP
//            ↓
//   Camera   →  Connects to our FTP server and uploads the file
//            ↓
//   Server   →  Receives 0x1206 (T/98 §5.6.6) — upload complete notify
//            ↓
//   Browser  ←  WS msg {type:'ftp_ready', url, filename}   OR   {type:'error'}
//
// Protocol references:
//   §5.6.5  Table 26 — 0x9206 file upload instruction
//   §5.6.6  Table 27 — 0x1206 file upload completion notification
//   §5.6.7  Table 28 — 0x9207 file upload control (pause / resume / cancel)
//
// Usage in index-pictor-0.js:
//   const ftpDownload = require('./ftp-download');
//   ftpDownload.init({ serverIp, ftpPort: 2121, pasvDataPort: 2122,
//                      recordingsDir: './recordings', wss, tcpSockets, buildFrame });
//   // In WS message handler:
//   ftpDownload.handleWsMessage(msg, ws);
//   // In TCP signalling handler (inside the 0x7E parser):
//   ftpDownload.handleSignalling(msgId, body, seq, phone, socket);
// ─────────────────────────────────────────────────────────────────────────────

const net  = require('net');
const fs   = require('fs');
const path = require('path');

// ── Module state ──────────────────────────────────────────────────────────────
let _serverIp       = '';
let _ftpPort = 14992;
let _pasvDataPort = 14993;
let _recordingsDir  = './recordings';
let _wss            = null;      // WebSocketServer instance
let _tcpSockets     = null;      // shared { [phone]: socket } map
let _buildFrame     = null;      // shared buildFrame(msgId, body, phone) fn
let _buildAck       = null;      // shared buildAck(phone, seq, msgId) fn

// Active download sessions: { [phone]: { filename, sessionId, status } }
const _sessions = {};

// PASV data connection slot (one at a time — permanent server)
let _pasvDataSocket = null;
let _pasvServer     = null;
let _ftpServer      = null;
let _pendingStor     = null;

// ── Helpers ───────────────────────────────────────────────────────────────────
// Phone → SN mapping (protocol phone differs from SN by one digit)
const PHONE_TO_SN = {
    '1576064472': '15760064472',
    '1576064474': '15760064474',
};
function _framePhone(phone) {
    return PHONE_TO_SN[phone] || phone;
}

function _bcdBytes(yy, mo, dd, hh, mm, ss) {
    const enc = n => ((Math.floor(n / 10) << 4) | (n % 10));
    return Buffer.from([enc(yy), enc(mo), enc(dd), enc(hh), enc(mm), enc(ss)]);
}

function _parseDateTime(dtStr, fallbackTime) {
    const [date, time = fallbackTime] = dtStr.split(' ');
    const [y, mo, d] = date.split('-').map(Number);
    const [h, mi, s] = time.split(':').map(Number);
    return { y, mo, d, h, mi, s };
}

function _broadcast(obj) {
    if (!_wss) return;
    const raw = JSON.stringify(obj);
    _wss.clients.forEach(c => { if (c.readyState === 1) c.send(raw); });
}

function _log(...args)  { console.log ('[FTP-DL]', ...args); }
function _warn(...args) { console.warn('[FTP-DL]', ...args); }
function _err(...args)  { console.error('[FTP-DL]', ...args); }

// ── 0x9205 frame builder — query recording list ────────────────────────────
// T/98 §5.6.1 Table 21: send this BEFORE 0x9206 so camera verifies file exists
function _build9205(phone, channel, startTime, endTime) {
    const framePhone = _framePhone(phone);
    const s = _parseDateTime(startTime, '00:00:00');
    const e = _parseDateTime(endTime,   '23:59:59');

    const body = Buffer.alloc(23);
    body[0] = channel;                                               // logical channel (0=all)
    _bcdBytes(s.y%100,s.mo,s.d,s.h,s.mi,s.s).copy(body, 1);       // start BCD
    _bcdBytes(e.y%100,e.mo,e.d,e.h,e.mi,e.s).copy(body, 7);       // end BCD
    body.fill(0x00, 13, 21);                                        // alarm logo — no filter
    body[21] = 0;                                                    // avType: 0=audio+video (matches camera)
    body[22] = 0;                                                    // stream: all

    _log(`0x9205 query — ch:${channel} ${startTime} → ${endTime}`);
    // return _buildFrame(0x9205, body, phone);
    return _buildFrame(0x9205, body, framePhone);
}

// ── 0x9206 frame builder ─────────────────────────────────────────────────────
// T/98 §5.6.5 Table 26:
//   ipLen(1)  ip(k)  port(2)  userLen(1)  user(l)
//   passLen(1)  pass(m)  pathLen(1)  path(n)
//   logicalCh(1)  startBCD(6)  endBCD(6)
//   alarmLogo(8)  avType(1)  streamType(1)  storageType(1)  taskCondition(1)
function _build9206(phone, channel, startTime, endTime) {
    const framePhone = _framePhone(phone);
    const ftpUser    = 'anonymous';
    const ftpPass    = 'anonymous';
    // Filename on the FTP server — unique per request
    //const tag        = startTime.replace(/[: -]/g, '');
    //const uploadPath = `/`;  // root path — camera picks its own filename
    const tag = `${phone}-${Date.now()}-1`;
    const uploadPath = `/`;
    const s = _parseDateTime(startTime, '00:00:00');
    const e = _parseDateTime(endTime,   '23:59:59');

    const ipBuf   = Buffer.from(_serverIp, 'ascii');
    const userBuf = Buffer.from(ftpUser,   'ascii');
    const passBuf = Buffer.from(ftpPass,   'ascii');
    const pathBuf = Buffer.from(uploadPath,'ascii');

    const k = ipBuf.length, l = userBuf.length, m = passBuf.length, n = pathBuf.length;
    // Total body = 1+k + 2 + 1+l + 1+m + 1+n + 1 + 6 + 6 + 8 + 4
    const body = Buffer.alloc(1+k + 2 + 1+l + 1+m + 1+n + 1 + 6 + 6 + 8 + 4);
    let p = 0;

    // FTP server IP
    body[p++] = k;                          ipBuf.copy(body, p);   p += k;
    // FTP port (TCP only)
    body.writeUInt16BE(_ftpPort, p);        p += 2;
    // Username
    body[p++] = l;                          userBuf.copy(body, p); p += l;
    // Password
    body[p++] = m;                          passBuf.copy(body, p); p += m;
    // Upload path
    body[p++] = n;                          pathBuf.copy(body, p); p += n;
    // Logical channel number
    body[p++] = channel;
    // Start time BCD (YY MM DD HH mm SS — 2-digit year)
    _bcdBytes(s.y % 100, s.mo, s.d, s.h, s.mi, s.s).copy(body, p); p += 6;
    // End time BCD
    _bcdBytes(e.y % 100, e.mo, e.d, e.h, e.mi, e.s).copy(body, p); p += 6;
    // Alarm logo — 8 bytes all 0 (no alarm filter)
    body.fill(0x00, p, p + 8);              p += 8;
    // avType: 0 = audio+video — matches what camera actually stores
    body[p++] = 0;
    // streamType: 1 = main stream (explicit)
    body[p++] = 1;
    // storageType: 0 = any, 1 = main, 2 = disaster — use 0
    body[p++] = 0;   // storageType: 99 — matches camera memType
    // taskCondition bits: bit0=WiFi bit1=LAN bit2=3G/4G — 0xFF = allow all
    body[p++] = 0xFF;

    _log(`0x9206 body breakdown:
      FTP IP     : ${_serverIp} (len ${k})
      FTP port   : ${_ftpPort}
      Username   : ${ftpUser} (len ${l})
      Password   : ${ftpPass} (len ${m})
      Upload path: ${uploadPath} (len ${n})
      Channel    : ${channel}
      Start BCD  : ${startTime}
      End BCD    : ${endTime}
      alarmLogo  : 0x0000000000000000
      avType     : 0 (audio+video)
      streamType : 0 (main stream)
      storageType: 0 (all storage)
      taskCond   : 0x0F
      body hex   : ${body.toString('hex')}`);

    // return { frame: _buildFrame(0x9206, body, phone), uploadPath };
    return { frame: _buildFrame(0x9206, body, framePhone), uploadPath };
}

// ── 0x9207 control frame ─────────────────────────────────────────────────────
// T/98 §5.6.7 Table 28:  replySerial(2)  control(1)
//   control: 0=pause  1=continue  2=cancel
function _build9207(phone, sessionId, control) {
    const body = Buffer.alloc(3);
    body.writeUInt16BE(sessionId & 0xFFFF, 0);
    body[2] = control;
    return _buildFrame(0x9207, body, _framePhone(phone));
}

// ── FTP server ────────────────────────────────────────────────────────────────
function _makeFtpHandler() {
    return ftpSock => {
        _log(`FTP control connection from ${ftpSock.remoteAddress}:${ftpSock.remotePort} → local:${ftpSock.localPort}`);

        let dataSocket   = null;
        let uploadStream = null;
        let uploadPath   = null;
        let currentDir   = '/';

        const reply = (code, msg) => {
            _log(`→ FTP ${code} ${msg}`);
            ftpSock.write(`${code} ${msg}\r\n`);
        };

        reply(220, 'FTP Server Ready');

        ftpSock.on('data', data => {
            const lines = data.toString().split('\r\n').filter(Boolean);
            lines.forEach(line => {
                _log(`← FTP cmd: ${line.trim()}`);
                const [cmd, ...args] = line.trim().split(' ');
                const arg = args.join(' ');

                switch (cmd.toUpperCase()) {
                    case 'USER':
                        reply(331, 'Please specify the password');
                        break;

                    case 'PASS':
                        reply(230, 'User logged in, proceed');
                        break;
                                        case 'SIZE':
                        reply(213, '0');  // or actual file size if exists
                        break;

                    case 'MDTM':
                        reply(213, '20260101000000');
                        break;

                    case 'DELE':
                        reply(250, 'Delete operation successful');
                        break;

                    case 'RNFR':
                        reply(350, 'Ready for RNTO');
                        break;

                    case 'RNTO':
                        reply(250, 'Rename successful');
                        break;

                    case 'SYST':
                        reply(215, 'UNIX Type: L8');
                        break;

                    case 'TYPE':
                        reply(200, 'Type set to I');
                        break;

                    case 'PWD':
                    case 'XPWD':
                        reply(257, `"${currentDir}" is current directory`);
                        break;

                    case 'CWD':
                    currentDir = arg.startsWith('/') ? arg : path.join(currentDir, arg);
                    reply(250, `Directory changed to ${currentDir}`);
                    break;

                    case 'MKD': {
                        const dirPath = path.join(_recordingsDir, arg.replace(/^\//, ''));
                        try {
                            fs.mkdirSync(dirPath, { recursive: true });
                            _log(`MKD created: ${dirPath}`);
                            reply(257, `"/${arg}" created`);
                        } catch (e) {
                            _warn(`MKD failed: ${e.message}`);
                            reply(550, 'Failed to create directory');
                        }
                        break;
                    }

                    case 'EPSV':
                        // Some devices try EPSV first — refuse so they fall back to PASV
                        reply(502, 'EPSV not supported, use PASV');
                        break;

                    case 'PASV': {
                        // Reset the slot so the next device connection fills it fresh
                        _pasvDataSocket = null;
                        dataSocket      = null;

                        const ip = _serverIp.split('.');
                        const p1 = Math.floor(_pasvDataPort / 256);
                        const p2 = _pasvDataPort % 256;
                        const resp = `Entering Passive Mode (${ip.join(',')},${p1},${p2})`;
                        _log(`PASV → ${_serverIp}:${_pasvDataPort} (p1=${p1} p2=${p2})`);
                        reply(227, resp);
                        break;
                    }

                    case 'LIST':
                    case 'NLST': {
                        reply(150, 'Here comes the directory listing');
                        const _sendListing = () => {
                            if (_pasvDataSocket) {
                                const ds = _pasvDataSocket;
                                _pasvDataSocket = null;
                                // Must attach error handler BEFORE writing — camera closes
                                // the data socket immediately after receiving data, which
                                // causes ECONNRESET if Node is still reading. This was
                                // crashing PM2.
                                ds.on('error', err => {
                                    if (err.code !== 'ECONNRESET') _warn(`LIST data socket: ${err.message}`);
                                });
                                let listing = '';
                                try {
                                    const absDir  = path.join(_recordingsDir, currentDir);
                                    const entries = fs.readdirSync(absDir);
                                    listing = entries.map(name => {
                                        const full = path.join(absDir, name);
                                        const stat = fs.statSync(full);
                                        const isDir = stat.isDirectory();
                                        const size  = stat.size;
                                        const d     = stat.mtime;
                                        const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                                        const dateStr = `${months[d.getMonth()]} ${String(d.getDate()).padStart(2,' ')} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
                                        const perms = isDir ? 'drwxr-xr-x' : '-rw-r--r--';
                                        return `${perms} 1 ftp ftp ${size} ${dateStr} ${name}`;
                                    }).join('\r\n') + '\r\n';
                                } catch (e) {
                                    _warn(`LIST read error: ${e.message}`);
                                }
                                // Send 226 only AFTER data socket fully closes
                                ds.on('close', () => reply(226, 'Directory send OK'));
                                ds.end(listing);
                            } else {
                                setTimeout(_sendListing, 100);
                            }
                        };
                        _sendListing();
                        break;
                    }

                                        case 'STOR': {
                        const filename = path.basename(arg || `rec_${Date.now()}.mp4`);
                        uploadPath   = path.join(_recordingsDir, filename);
                        uploadStream = fs.createWriteStream(uploadPath);
                        _log(`STOR → writing to ${uploadPath}`);
                        reply(150, 'Ok to send data');

                        const _onComplete = () => {
                            const fname = path.basename(uploadPath);
                            _log(`✅ File transfer complete: ${fname}`);
                            reply(226, 'Transfer complete');
                            _broadcast({ type: 'ftp_ready', url: `/recordings/${fname}`, filename: fname });
                            _pasvDataSocket = null;
                        };

                        const _onError = (err) => {
                            _err('Data socket error:', err?.message || err);
                            reply(426, 'Connection closed, transfer aborted');
                            try { uploadStream.destroy(); } catch(e) {}
                        };

                        const handleDataSocket = (s) => {
                            _log(`Piping data → ${uploadPath}`);
                            dataSocket = s;
                            // Error handler must be attached before pipe to prevent
                            // unhandled ECONNRESET if camera drops connection mid-upload
                            s.on('error', err => {
                                if (err.code !== 'ECONNRESET') _onError(err);
                                else _warn('Data socket ECONNRESET during STOR (camera closed connection)');
                            });
                            uploadStream.on('error', err => _onError(err));
                            // Use finish (not close) on the writeStream — called exactly once
                            uploadStream.on('finish', _onComplete);
                            // end the writeStream when data socket signals end of data
                            s.on('end', () => {
                                if (!uploadStream.writableEnded) uploadStream.end();
                            });
                            s.pipe(uploadStream, { end: false }); // we control end manually above
                        };

                        if (_pasvDataSocket) {
                            handleDataSocket(_pasvDataSocket);
                            _pasvDataSocket = null;
                        } else {
                            _pendingStor = handleDataSocket;
                            setTimeout(() => {
                                if (_pendingStor === handleDataSocket) {
                                    _pendingStor = null;
                                    _err('No data connection after 30s');
                                    reply(425, 'No data connection established');
                                }
                            }, 30000);
                        }
                        break;
                    }

                    case 'QUIT':
                        reply(221, 'Goodbye');
                        ftpSock.end();
                        break;

                    case 'NOOP':
                        reply(200, 'OK');
                        break;

                    case 'AUTH':
                    case 'AUTH TLS':
                    case 'AUTH SSL':
                        reply(431, 'No TLS support');
                        break;

                    case 'FEAT':
                        ftpSock.write('211-Features:\r\n211 End\r\n');
                        break;

                    default:
                        _warn(`Unhandled FTP cmd: ${cmd}`);
                        reply(202, 'Command not implemented');
                }
            });
        });

        ftpSock.on('close', () => {
            _log('FTP control connection closed');
            if (uploadStream) { try { uploadStream.end(); } catch (_) {} }
        });
        ftpSock.on('error', err => _err('FTP control socket error:', err.message));
    };
}

function _startFtpServer() {
    // ── Permanent PASV data server ──────────────────────────────────────────
    // Stays open forever — re-creating per PASV causes EADDRINUSE on 2nd upload
        _pasvServer = net.createServer(s => {
            _log(`PASV data connection from ${s.remoteAddress}:${s.remotePort}`);
            if (_pendingStor) {
                _pendingStor(s);
                _pendingStor = null;
            } else {
                _pasvDataSocket = s;
                setTimeout(() => { if (_pasvDataSocket === s) { s.end(); _pasvDataSocket = null; } }, 30000);
            }
        });
    _pasvServer.listen(_pasvDataPort, '0.0.0.0', () => {
        _log(`✓ PASV data server on :${_pasvDataPort}`);
    });
    _pasvServer.on('error', err => _err('PASV server error:', err.message));

    // ── FTP control on configured port (default 2121) ───────────────────────
    _ftpServer = net.createServer(_makeFtpHandler());
    _ftpServer.listen(_ftpPort, '0.0.0.0', () => {
        _log(`✓ FTP control server on :${_ftpPort}`);
    });
    _ftpServer.on('error', err => _err(`FTP server :${_ftpPort} error:`, err.message));

    // ── Port 21 fallback — many cameras ignore the port in 0x9206 ──────────
    // Run: sudo setcap cap_net_bind_service=+ep $(which node)
    // OR:  sudo iptables -t nat -A PREROUTING -p tcp --dport 21 -j REDIRECT --to-port 2121
    const ftp21 = net.createServer(_makeFtpHandler());
    ftp21.listen(21, '0.0.0.0', () => {
        _log(`✓ FTP control server on :21 (port-21 fallback)`);
    });
    ftp21.on('error', err => {
        // Port 21 needs root/cap — log the fix and continue without it
        _warn(`Port 21 unavailable (${err.message}). Camera may use port 21 instead of ${_ftpPort}.`);
        _warn(`Fix: sudo iptables -t nat -A PREROUTING -p tcp --dport 21 -j REDIRECT --to-port ${_ftpPort}`);
    });
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Call once at startup.
 * @param {object} opts
 *   serverIp      — public IP camera uses to reach us
 *   ftpPort       — FTP control port (default 2121)
 *   pasvDataPort  — FTP PASV data port (default 2122)
 *   recordingsDir — where to save files (default './recordings')
 *   wss           — WebSocketServer instance
 *   tcpSockets    — shared { [phone]: socket } reference
 *   buildFrame    — shared buildFrame(msgId, body, phone) function
 *   buildAck      — shared buildAck(phone, seq, msgId) function
 */
function init(opts) {
    _serverIp      = opts.serverIp;
    _ftpPort       = opts.ftpPort       || 2121;
    _pasvDataPort  = opts.pasvDataPort  || 2122;
    _recordingsDir = opts.recordingsDir || './recordings';
    _wss           = opts.wss;
    _tcpSockets    = opts.tcpSockets;
    _buildFrame    = opts.buildFrame;
    _buildAck      = opts.buildAck;

    if (!fs.existsSync(_recordingsDir)) fs.mkdirSync(_recordingsDir, { recursive: true });

    _log(`init — serverIp:${_serverIp} ftpPort:${_ftpPort} pasvPort:${_pasvDataPort}`);
    _startFtpServer();
}

/**
 * Call from the WebSocket 'message' handler in index-pictor-0.js.
 * Handles: ftp_download, ftp_cancel
 */
function handleWsMessage(msg, ws) {

    // ── ftp_download ─────────────────────────────────────────────────────────
    if (msg.type === 'ftp_download') {
        const { ch, startTime, endTime, phone: reqPhone } = msg;

        // Pick the first connected socket if phone not specified
        const phone = reqPhone || Object.keys(_tcpSockets)[0];
        const phoneStr = String(phone).padStart(12, '0');
        _log(`▶ ftp_download — phone:${phoneStr} ch:${ch} ${startTime} → ${endTime}`);

        // Validation
        if (!phone) {
            const m = 'No device connected';
            _err(m);
            ws.send(JSON.stringify({ type: 'error', message: m }));
            return;
        }
        if (!_tcpSockets[phone] || _tcpSockets[phone].destroyed) {
            const m = `Device socket not available for ${phoneStr}`;
            _err(m);
            ws.send(JSON.stringify({ type: 'error', message: m }));
            return;
        }
        if (!ch || !startTime || !endTime) {
            const m = 'Missing ch / startTime / endTime';
            _err(m);
            ws.send(JSON.stringify({ type: 'error', message: m }));
            return;
        }
        // Cancel any previous stuck download
        if (_sessions[phone] && _tcpSockets[phone] && !_tcpSockets[phone].destroyed) {
            _tcpSockets[phone].write(_build9207(phone, 0, 2));
            _log(`Sent 0x9207 cancel for previous session`);
            delete _sessions[phone];
        }
        // Step 1 — send 0x9205 (query list) so camera verifies file exists on SD card
        const queryFrame = _build9205(phone, ch, startTime, endTime);
        _tcpSockets[phone].write(queryFrame);
        _log(`Sent 0x9205 pre-query to ${phoneStr}`);

        // Step 2 — after 3 s, send 0x9206 (FTP upload request)
        setTimeout(() => {
            if (!_tcpSockets[phone] || _tcpSockets[phone].destroyed) {
                _err(`Socket gone before 0x9206 could be sent`);
                return;
            }
            const { frame, uploadPath } = _build9206(phone, ch, startTime, endTime);
            _log(`Sending 0x9206 to ${phone} (frame ${frame.length} bytes)`);
            _log(`Frame hex: ${frame.toString('hex')}`);
            _tcpSockets[phone].write(frame);

            _sessions[phone] = {
                ch, startTime, endTime,
                ftpPath: uploadPath,
                sentAt: Date.now(),
            };
            _log(`Session saved for ${phone}:`, _sessions[phone]);
        }, 3000);

        // Tell browser we're waiting
        ws.send(JSON.stringify({
            type: 'status',
            message: `⏳ Requested recording ch${ch} ${startTime} → ${endTime}. Waiting for camera to upload...`,
        }));
        return;
    }

    // ── ftp_cancel ───────────────────────────────────────────────────────────
    if (msg.type === 'ftp_cancel') {
        const phone = msg.phone || Object.keys(_tcpSockets)[0];
        const sess  = _sessions[phone];
        if (!sess) {
            ws.send(JSON.stringify({ type: 'status', message: 'No active download to cancel' }));
            return;
        }
        if (_tcpSockets[phone] && !_tcpSockets[phone].destroyed) {
            // sessionId = 0 means cancel all — device matches by context
            _tcpSockets[phone].write(_build9207(phone, 0, 2));
            _log(`Sent 0x9207 cancel to ${phone}`);
        }
        delete _sessions[phone];
        ws.send(JSON.stringify({ type: 'status', message: '🛑 Download cancelled' }));
    }
}

/**
 * Call from the TCP signalling parser in index-pictor-0.js whenever a 0x7E
 * frame is decoded.  Only handles msgIds relevant to FTP download; all others
 * are ignored so the caller can still handle them normally.
 *
 * @returns {boolean} true if the message was consumed here (caller should skip its own handling)
 */
function handleSignalling(msgId, body, seq, phone, socket) {

    // ── 0x0001: Device general response to our 0x9206 ────────────────────────
    if (msgId === 0x0001) {
        const replyMsgId  = body.readUInt16BE(2);
        const replySeq    = body.readUInt16BE(0);
        const replyResult = body[4];
        if (replyMsgId === 0x9206) {
            const resultText = ['Success','Failed','Wrong Msg','Not Supported'][replyResult] || `Unknown(${replyResult})`;
            _log(`0x0001 ack for 0x9206 — seq:${replySeq} result:${replyResult} (${resultText})`);
            if (replyResult !== 0) {
                _err(`Camera rejected 0x9206 with result ${replyResult}`);
                _broadcast({ type: 'error', message: `Camera rejected download request (code ${replyResult})` });
            } else {
                _log(`Camera accepted 0x9206 — waiting for FTP upload...`);
                _broadcast({ type: 'status', message: '✅ Camera accepted request, uploading via FTP...' });
            }
            return true; // consumed
        }
        return false;
    }

    // ── 0x1206: File upload completion notification ───────────────────────────
    // T/98 §5.6.6 Table 27:  replySerial(2)  result(1)
    //   result: 0=success  1=failed
    if (msgId === 0x1206) {
        // Always ACK the device
        // socket.write(_buildAck(phone, seq, 0x1206));
        const framePhone = _framePhone(phone);
        socket.write(_buildAck(framePhone, seq, 0x1206));

        const rawHex      = body.toString('hex');
        const replySerial = body.readUInt16BE(0);
        const result      = body[2];

        _log(`0x1206 received — phone:${phone} replySerial:${replySerial} result:${result}`);
        _log(`0x1206 raw body hex: ${rawHex}`);

        if (result === 0) {
            _log(`✅ Camera finished uploading for ${phone} — file should arrive via FTP`);
            // The FTP STOR handler will send ftp_ready once the file lands
            // Broadcast a status in the meantime
            _broadcast({ type: 'status', message: '📦 Camera upload complete, saving file...' });
        } else {
            const hints = [
                `Result code: ${result}`,
                `Is port ${_ftpPort} open on the server firewall?`,
                `Can camera reach ${_serverIp}:${_ftpPort}?`,
                `Is the time range correct? Does the SD card have that recording?`,
            ].join(' | ');
            _err(`❌ 0x1206 FAILED for ${phone} — ${hints}`);
            _broadcast({
                type: 'error',
                message: `Camera upload failed (code ${result}). ${hints}`,
            });
            delete _sessions[phone];
        }
        return true; // consumed
    }

    return false; // not handled here
}

/**
 * Expose active sessions so index-pictor-0 can log them if needed.
 */
function getSessions() {
    return _sessions;
}

module.exports = { init, handleWsMessage, handleSignalling, getSessions };