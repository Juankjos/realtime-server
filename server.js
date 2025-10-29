import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';

const app = express();
app.use(cors());
app.get('/', (_, res) => res.send('WS up'));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// Estado en memoria por room
const roomsState = new Map(); // `report:${id}` -> { lastLoc, lastDest }
const chatCache  = new Map(); // `report:${id}` -> ChatMsg[]  (limitar tamaño)

/** Helpers */
const asNum = (v, d = 0) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : d;
};
const asRole = (r) => (r === 'tech' || r === 'client') ? r : 'system';

io.on('connection', (socket) => {
    const { reportId, tecId, role } = socket.handshake.query || {};
    if (!reportId) { socket.disconnect(true); return; }

    const room = `report:${reportId}`;
    const safeRole = asRole(role);
    const safeTecId = tecId ? asNum(tecId, null) : null;

    console.log('[srv] connect id=', socket.id, 'role=', safeRole, 'reportId=', reportId, 'tecId=', safeTecId);

    socket.join(room);
    socket.emit('room:joined', { room, role: safeRole, reportId, tecId: safeTecId });

  // Re-emite último estado live si existe
  const state = roomsState.get(room);
  if (state?.lastDest) socket.emit('destination:live', state.lastDest);
  if (state?.lastLoc)  socket.emit('location:live',  state.lastLoc);

  // Ubicación en vivo
  socket.on('location:update', (msg = {}) => {
    const payload = {
      reportId: asNum(reportId),
      tecId: safeTecId,
      lat: asNum(msg.lat),
      lng: asNum(msg.lng),
      speed: asNum(msg.speed),
      bearing: asNum(msg.bearing),
        ts: asNum(msg.ts, Date.now()),
        };
        if (!Number.isFinite(payload.lat) || !Number.isFinite(payload.lng)) return; // guard

        const st = roomsState.get(room) || {};
        st.lastLoc = payload;
        roomsState.set(room, st);

        io.to(room).emit('location:live', payload);
    });

    // Destino en vivo
    socket.on('destination:update', (msg = {}) => {
        const payload = {
        reportId: asNum(reportId),
        tecId: safeTecId,
        lat: asNum(msg.lat),
        lng: asNum(msg.lng),
        address: (msg.address ?? null),
        ts: Date.now(),
    };
    if (!Number.isFinite(payload.lat) || !Number.isFinite(payload.lng)) return; // guard

    const st = roomsState.get(room) || {};
    st.lastDest = payload;
    roomsState.set(room, st);

    io.to(room).emit('destination:live', payload);
  });

  // Historial de chat
  socket.on('chat:history:get', async (req = {}) => {
    const limit = asNum(req.limit, 50);
    const list = (chatCache.get(room) || []).slice(-limit);
    socket.emit('chat:history', list);
  });

  // Envío de chat
  socket.on('chat:send', async (msg = {}) => {
    let text = String(msg.text || '').trim();
    if (!text) return;
    if (text.length > 2000) text = text.slice(0, 2000);

    const payload = {
      reportId: asNum(reportId),
      from: asRole(msg.from) === 'system' ? safeRole : asRole(msg.from), // prioriza rol del socket
      senderId: safeTecId ?? (Number.isFinite(asNum(msg.senderId)) ? asNum(msg.senderId) : null),
      text,
      ts: asNum(msg.ts, Date.now()),
    };

    // (1) Persistencia en SQL aquí si aplica…

    // (2) Cache en memoria (capped)
    const arr = chatCache.get(room) || [];
    arr.push(payload);
    if (arr.length > 500) arr.splice(0, arr.length - 500);
    chatCache.set(room, arr);

    // (3) Broadcast
    io.to(room).emit('chat:message', payload);
    });

    socket.on('disconnect', (reason) => {
    console.log('[srv] disconnect', socket.id, reason);
    // No limpiamos roomsState/chatCache para mantener estado a recargas
    });
});

server.listen(3001, '0.0.0.0', () => console.log('🟢 WS on http://localhost:3001'));
