'use strict';
/*
 * Servidor online de Quake (relay por WebSocket). Sin dependencias: solo Node.js.
 *
 *   node server\server.js            (desde la carpeta WinQuake)
 *   luego abri http://localhost:8765
 *
 * - Sirve el juego (index.html, index.js, index.data, ...) desde la carpeta WinQuake.
 * - Cada navegador se conecta por WebSocket y recibe un id numerico (#12).
 * - Quien crea una partida multijugador se anuncia como "sala"; todos ven la lista.
 * - Los paquetes del juego se reenvian entre jugadores que hicieron "HELLO" entre si.
 *
 * Variables: PORT (8765), HOST (0.0.0.0), STATIC_ROOT, MAX_CLIENTS (500), MAX_PER_IP (40)
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = +process.env.PORT || 8765;
const HOST = process.env.HOST || '0.0.0.0';
const STATIC_ROOT = path.resolve(process.env.STATIC_ROOT || path.join(__dirname, '..'));
const MAX_CLIENTS = +process.env.MAX_CLIENTS || 500;
const MAX_PER_IP = +process.env.MAX_PER_IP || 40;

const MAX_WS_FRAME = 16 * 1024;     // tamano maximo de un frame WebSocket entrante
const MAX_PAYLOAD = 8400;           // payload maximo de un paquete de juego
const MAX_LINKS_CLIENT = 8;         // salas a las que un jugador puede estar unido a la vez
const MAX_LINKS_HOST = 40;
const ROOM_TTL_MS = 8000;           // una sala desaparece si no se actualiza en este tiempo

const T = { HELLO: 1, ACCEPT: 2, REJECT: 3, RELIABLE: 4, UNRELIABLE: 5, CLOSE: 6 };
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const clients = new Map();          // id -> Client
let nextId = 1;
const perIp = new Map();

function log() { console.log('[' + new Date().toLocaleTimeString() + ']', ...arguments); }

function cleanText(s, max) {
  return String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f|"\\]/g, '').slice(0, max);
}

/* ---------- Salas ---------- */
function roomsList() {
  const now = Date.now(), out = [];
  for (const c of clients.values()) {
    if (c.listening && c.room && now - c.room.t < ROOM_TTL_MS) {
      out.push({ id: c.id, name: c.room.name, map: c.room.map, players: c.room.players, max: c.room.max });
    }
  }
  out.sort((a, b) => a.id - b.id);
  return out;
}
let lastRoomsJson = '';
function pushRooms(force) {
  const json = JSON.stringify({ t: 'rooms', rooms: roomsList() });
  if (!force && json === lastRoomsJson) return;
  lastRoomsJson = json;
  const buf = Buffer.from(json);
  for (const c of clients.values()) wsSend(c, 1, buf);
}
setInterval(() => pushRooms(false), 1000);
setInterval(() => pushRooms(true), 10000);

/* ---------- WebSocket (RFC 6455, lo minimo necesario) ---------- */
function wsSend(c, op, payload) {
  if (c.closed || !c.socket.writable) return;
  if (c.socket.writableLength > 4 * 1024 * 1024) return kill(c, 1013);
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
  c.socket.write(Buffer.concat([head, payload]));
}
function sendJson(c, obj) { wsSend(c, 1, Buffer.from(JSON.stringify(obj))); }

function mk(id, type, payload) {
  const p = payload || Buffer.alloc(0);
  const b = Buffer.allocUnsafe(5 + p.length);
  b.writeUInt32BE(id >>> 0, 0); b[4] = type; p.copy(b, 5);
  return b;
}

function kill(c, code) {
  if (c.closed) return;
  try {
    const b = Buffer.alloc(2); b.writeUInt16BE(code || 1000, 0);
    c.socket.write(Buffer.concat([Buffer.from([0x88, 2]), b]));
  } catch (e) { /* ignorar */ }
  c.socket.end();
  dropClient(c);
}

function dropClient(c) {
  if (c.closed) return;
  c.closed = true;
  clients.delete(c.id);
  const n = (perIp.get(c.ip) || 1) - 1;
  if (n <= 0) perIp.delete(c.ip); else perIp.set(c.ip, n);
  for (const pid of c.links) {
    const p = clients.get(pid);
    if (p) { sendBin(p, mk(c.id, T.CLOSE)); p.links.delete(c.id); }
  }
  c.links.clear();
  if (c.listening && c.room) log('Sala cerrada: "' + c.room.name + '" (#' + c.id + ')');
  log('Jugador #' + c.id + ' desconectado. Conectados: ' + clients.size);
  pushRooms(false);
}
function sendBin(c, buf) { wsSend(c, 2, buf); }

function onData(c, chunk) {
  c.buf = c.buf.length ? Buffer.concat([c.buf, chunk]) : chunk;
  for (;;) {
    const b = c.buf;
    if (b.length < 2) return;
    const fin = !!(b[0] & 0x80), op = b[0] & 0x0f, masked = !!(b[1] & 0x80);
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) {
      if (b.length < 10) return;
      if (b.readUInt32BE(2) !== 0) return kill(c, 1009);
      len = b.readUInt32BE(6); off = 10;
    }
    if (!masked) return kill(c, 1002);
    if (len > MAX_WS_FRAME) return kill(c, 1009);
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4);
    const data = Buffer.allocUnsafe(len);
    for (let i = 0; i < len; i++) data[i] = b[off + 4 + i] ^ mask[i & 3];
    c.buf = b.subarray(off + 4 + len);

    c.frames++; c.bytes += len + off;
    if (c.frames > 4000 || c.bytes > 6e6) return kill(c, 1008);

    if (op === 8) return kill(c, 1000);
    if (op === 9) { wsSend(c, 10, data); continue; }
    if (op === 10) { c.lastSeen = Date.now(); continue; }
    c.lastSeen = Date.now();

    let kind = op, payload = data;
    if (op === 0) {                          // continuacion
      if (!c.frag) return kill(c, 1002);
      c.frag.parts.push(data); c.frag.size += len;
      if (c.frag.size > MAX_WS_FRAME * 2) return kill(c, 1009);
      if (!fin) continue;
      kind = c.frag.op; payload = Buffer.concat(c.frag.parts); c.frag = null;
    } else if (!fin) {
      c.frag = { op, parts: [data], size: len };
      continue;
    }
    if (kind === 2) onBinary(c, payload);
    else if (kind === 1) onText(c, payload);
  }
}

/* ---------- Mensajes ---------- */
function onText(c, buf) {
  if (buf.length > 1024) return;
  let m; try { m = JSON.parse(buf.toString('utf8')); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  if (m.t === 'listen') {
    c.listening = !!m.on;
    if (!c.listening) { if (c.room) log('Sala cerrada: "' + c.room.name + '" (#' + c.id + ')'); c.room = null; }
    pushRooms(false);
  } else if (m.t === 'room') {
    const isNew = !c.room;
    c.room = {
      name: cleanText(m.name, 32) || ('Sala #' + c.id),
      map: cleanText(m.map, 16),
      players: Math.max(0, Math.min(32, m.players | 0)),
      max: Math.max(1, Math.min(32, m.max | 0)),
      t: Date.now()
    };
    if (isNew && c.listening) log('Sala nueva: "' + c.room.name + '" (#' + c.id + ') mapa ' + c.room.map);
  } else if (m.t === 'rooms') {
    sendJson(c, { t: 'rooms', rooms: roomsList() });
  }
}

function onBinary(c, data) {
  if (data.length < 5 || data.length > 5 + MAX_PAYLOAD) return;
  const dst = data.readUInt32BE(0), type = data[4];
  if (type < 1 || type > 6) return;
  const payload = data.subarray(5);
  const peer = clients.get(dst);

  if (type === T.HELLO) {
    const fresh = peer && peer.listening && peer.room && Date.now() - peer.room.t < ROOM_TTL_MS * 2;
    if (!peer || peer === c || !fresh) {
      return sendBin(c, mk(dst, T.REJECT, Buffer.from('La sala ya no existe.')));
    }
    if (!c.links.has(dst)) {
      if (c.links.size >= MAX_LINKS_CLIENT || peer.links.size >= MAX_LINKS_HOST) {
        return sendBin(c, mk(dst, T.REJECT, Buffer.from('Demasiadas conexiones.')));
      }
      c.links.add(dst); peer.links.add(c.id);
    }
    return sendBin(peer, mk(c.id, T.HELLO));
  }

  // el resto solo se reenvia entre jugadores ya vinculados
  if (!peer || !c.links.has(dst)) return;
  sendBin(peer, mk(c.id, type, payload));
  if (type === T.CLOSE || type === T.REJECT) { c.links.delete(dst); peer.links.delete(c.id); }
}

/* ---------- HTTP (juego + estado) ---------- */
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.wasm': 'application/wasm',
  '.data': 'application/octet-stream', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png',
  '.css': 'text/css; charset=utf-8', '.ico': 'image/x-icon', '.json': 'application/json'
};

const server = http.createServer((req, res) => {
  let url;
  try { url = new URL(req.url, 'http://x'); } catch (e) { res.writeHead(400); return res.end(); }
  const headers = { 'Cache-Control': 'no-cache', 'X-Content-Type-Options': 'nosniff' };

  if (url.pathname === '/rooms') {
    res.writeHead(200, Object.assign({ 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }, headers));
    return res.end(JSON.stringify(roomsList()));
  }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405); return res.end(); }

  let rel;
  try { rel = decodeURIComponent(url.pathname); } catch (e) { res.writeHead(400); return res.end(); }
  if (rel === '/') rel = '/index.html';
  const file = path.resolve(STATIC_ROOT, '.' + rel);
  const ext = path.extname(file).toLowerCase();
  if (file.indexOf(STATIC_ROOT + path.sep) !== 0 || !MIME[ext]) { res.writeHead(404); return res.end('No encontrado'); }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) { res.writeHead(404); return res.end('No encontrado'); }
    res.writeHead(200, Object.assign({ 'Content-Type': MIME[ext], 'Content-Length': st.size }, headers));
    if (req.method === 'HEAD') return res.end();
    fs.createReadStream(file).pipe(res);
  });
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const ip = (req.headers['x-forwarded-for'] || socket.remoteAddress || '?').toString().split(',')[0].trim();
  if (clients.size >= MAX_CLIENTS || (perIp.get(ip) || 0) >= MAX_PER_IP) {
    socket.end('HTTP/1.1 503 Service Unavailable\r\n\r\n'); return;
  }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);

  const c = {
    id: nextId++, socket, ip, buf: Buffer.alloc(0), frag: null, closed: false,
    listening: false, room: null, links: new Set(), lastSeen: Date.now(), frames: 0, bytes: 0
  };
  clients.set(c.id, c);
  perIp.set(ip, (perIp.get(ip) || 0) + 1);
  socket.on('data', (d) => onData(c, d));
  socket.on('close', () => dropClient(c));
  socket.on('error', () => dropClient(c));
  log('Jugador #' + c.id + ' conectado (' + ip + '). Conectados: ' + clients.size);
  sendJson(c, { t: 'hello', id: c.id });
  sendJson(c, { t: 'rooms', rooms: roomsList() });
});

setInterval(() => { for (const c of clients.values()) { c.frames = 0; c.bytes = 0; } }, 1000);
setInterval(() => {
  const now = Date.now();
  for (const c of clients.values()) {
    if (now - c.lastSeen > 70000) kill(c, 1001);
    else wsSend(c, 9, Buffer.alloc(0));
  }
}, 20000);

process.on('uncaughtException', (e) => log('Error:', e && e.stack || e));

server.listen(PORT, HOST, () => {
  log('Servidor Quake online en http://localhost:' + PORT);
  log('Sirviendo el juego desde: ' + STATIC_ROOT);
  log('Para jugar con otros, tienen que poder llegar a este puerto (' + PORT + ').');
});
