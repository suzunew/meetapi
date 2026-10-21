'use strict';
const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const port = Number(process.env.PORT) || 3000;
const renderUrl = (process.env.RENDER_EXTERNAL_URL || '').replace(/\/$/, '');
const staticDir = path.join(__dirname, 'public');
const rooms = new Map();

app.disable('x-powered-by');
app.use(express.json({ limit: '16kb' }));
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  res.setHeader('Permissions-Policy', 'camera=(self), microphone=(self), display-capture=(self)');
  next();
});
app.get('/api/health', (_req, res) => res.json({ ok: true, service: 'suzuzora-meet', rooms: rooms.size, uptimeSeconds: Math.floor(process.uptime()) }));
app.get('/api/config', (_req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');
  const iceServers = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' }
  ];
  if (process.env.TURN_URL && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
    iceServers.push({ urls: process.env.TURN_URL.split(',').map(v => v.trim()).filter(Boolean), username: process.env.TURN_USERNAME, credential: process.env.TURN_CREDENTIAL });
  }
  res.json({ iceServers });
});
function sendIndex(req, res) {
  const fs = require('fs');
  const pageUrl = (process.env.RENDER_EXTERNAL_URL || `${req.protocol}://${req.get('host')}`).replace(/\/$/, '');
  fs.readFile(path.join(staticDir, 'index.html'), 'utf8', (err, html) => {
    if (err) return res.status(500).send('Unable to load suzuzora meet.');
    res.setHeader('Cache-Control', 'no-cache');
    res.type('html').send(html.replace(/%%MEET_SERVER_URL%%/g, pageUrl));
  });
}
app.get('/', sendIndex);
app.use(express.static(staticDir, { maxAge: '1h', etag: true, index: false }));
app.get('*', sendIndex);

const io = new Server(server, {
  cors: { origin: process.env.ALLOWED_ORIGIN || '*', methods: ['GET', 'POST'] },
  maxHttpBufferSize: 1e6,
  pingInterval: 25000,
  pingTimeout: 20000
});

function cleanName(value) {
  return String(value || '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 40) || '参加者';
}
function cleanRoom(value) {
  const room = String(value || '').trim().toUpperCase();
  return /^[A-Z0-9_-]{4,32}$/.test(room) ? room : '';
}
function cleanState(value) {
  value = value && typeof value === 'object' ? value : {};
  return { micOn: value.micOn !== false, videoOn: value.videoOn === true, handRaised: value.handRaised === true, sharing: value.sharing === true };
}
function ensureRoom(id) {
  let room = rooms.get(id);
  if (!room) {
    room = { peers: new Map(), messages: [] };
    rooms.set(id, room);
  }
  return room;
}
function participantFor(socket, name, extra = {}) {
  return { id: socket.id, name, joinedAt: Date.now(), ...cleanState(extra) };
}
function leaveRoom(socket) {
  const roomId = socket.data.roomId;
  if (!roomId) return;
  const room = rooms.get(roomId);
  if (room) {
    const old = room.peers.get(socket.id);
    room.peers.delete(socket.id);
    socket.leave(roomId);
    if (old) socket.to(roomId).emit('peer-left', { id: socket.id, name: old.name });
    if (room.peers.size === 0) rooms.delete(roomId);
  }
  socket.data.roomId = null;
  socket.data.name = null;
}

io.on('connection', socket => {
  socket.data.roomId = null;
  socket.on('join-room', (payload, ack) => {
    const reply = typeof ack === 'function' ? ack : () => {};
    const roomId = cleanRoom(payload?.roomId);
    const name = cleanName(payload?.name);
    if (!roomId) return reply({ ok: false, message: '会議コードが正しくありません。' });
    if (socket.data.roomId) leaveRoom(socket);
    const room = ensureRoom(roomId);
    if (room.peers.size >= Number(process.env.MAX_ROOM_SIZE || 8)) return reply({ ok: false, message: `この会議は満員です（上限 ${Number(process.env.MAX_ROOM_SIZE || 8)} 人）。` });
    const peers = Array.from(room.peers.values()).map(p => ({ ...p }));
    const participant = participantFor(socket, name, payload?.clientState);
    room.peers.set(socket.id, participant);
    socket.join(roomId);
    socket.data.roomId = roomId;
    socket.data.name = name;
    reply({ ok: true, self: { ...participant }, peers, messages: room.messages.slice(-100) });
    socket.to(roomId).emit('peer-joined', { ...participant });
    const systemMessage = { type: 'system', text: `${name} が参加しました`, createdAt: Date.now() };
    room.messages.push(systemMessage);
    if (room.messages.length > 150) room.messages.shift();
    socket.to(roomId).emit('chat-message', systemMessage);
  });
  socket.on('leave-room', () => leaveRoom(socket));
  socket.on('participant-state', payload => {
    const roomId = socket.data.roomId;
    if (!roomId) return;
    const room = rooms.get(roomId);
    const p = room?.peers.get(socket.id);
    if (!p) return;
    Object.assign(p, cleanState(payload));
    io.to(roomId).emit('participant-state', { id: p.id, name: p.name, micOn: p.micOn, videoOn: p.videoOn, handRaised: p.handRaised, sharing: p.sharing });
  });
  socket.on('signal', payload => {
    const roomId = socket.data.roomId;
    const to = String(payload?.to || '');
    if (!roomId || !to || to === socket.id || !payload?.signal || typeof payload.signal !== 'object') return;
    const room = rooms.get(roomId);
    if (!room?.peers.has(to) || !room.peers.has(socket.id)) return;
    const signal = payload.signal;
    if (signal.description && (!['offer', 'answer'].includes(signal.description.type) || typeof signal.description.sdp !== 'string' || signal.description.sdp.length > 200000)) return;
    if (signal.candidate && typeof signal.candidate.candidate === 'string' && signal.candidate.candidate.length > 5000) return;
    io.to(to).emit('signal', { from: socket.id, fromName: socket.data.name, signal });
  });
  socket.on('chat-send', payload => {
    const roomId = socket.data.roomId;
    const text = String(payload?.text || '').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '').trim().slice(0, 1500);
    if (!roomId || !text) return;
    const room = rooms.get(roomId);
    const p = room?.peers.get(socket.id);
    if (!room || !p) return;
    const msg = { type: 'message', id: `${Date.now()}-${socket.id}`, name: p.name, text, createdAt: Date.now() };
    room.messages.push(msg);
    if (room.messages.length > 150) room.messages.splice(0, room.messages.length - 150);
    io.to(roomId).emit('chat-message', msg);
  });
  socket.on('disconnect', () => leaveRoom(socket));
});

server.listen(port, '0.0.0.0', () => console.log(`suzuzora meet listening on ${port}${renderUrl ? ` (${renderUrl})` : ''}`));
