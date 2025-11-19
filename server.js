import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import mysql from 'mysql2/promise';

const app = express();
app.use(cors());
app.get('/', (_, res) => res.send('WS up'));

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

// ---------- DB POOL ----------
const pool = mysql.createPool({
  host: process.env.DB_HOST || '127.0.0.1',
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASS || '',
  database: process.env.DB_NAME || 'clientes',
  waitForConnections: true,
  connectionLimit: 10,
  maxIdle: 10,
  idleTimeout: 60000,
});

// ---------- HELPERS ----------
const asNum = (v, d = 0) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const asRole = (r) => (r === 'tech' || r === 'client') ? r : 'system';

// Estado en memoria (opcional) para loc/dest
const roomsState = new Map(); // `report:${id}` -> { lastLoc, lastDest }

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

  // --------- LOCATION ---------
  socket.on('location:update', (msg = {}, cb) => {
    const payload = {
      reportId: asNum(reportId),
      tecId: safeTecId,
      lat: asNum(msg.lat),
      lng: asNum(msg.lng),
      speed: asNum(msg.speed),
      bearing: asNum(msg.bearing),
      ts: asNum(msg.ts, Date.now()),
    };
    if (!Number.isFinite(payload.lat) || !Number.isFinite(payload.lng)) {
      if (typeof cb === 'function') cb({ ok: false, reason: 'invalid_coords' });
      return;
    }

    const st = roomsState.get(room) || {};
    st.lastLoc = payload;
    roomsState.set(room, st);

    io.to(room).emit('location:live', payload);
    if (typeof cb === 'function') cb({ ok: true });
  });

  // --------- DESTINATION ---------
  socket.on('destination:update', (msg = {}, cb) => {
    const payload = {
      reportId: asNum(reportId),
      tecId: safeTecId,
      lat: asNum(msg.lat),
      lng: asNum(msg.lng),
      address: (msg.address ?? null),
      ts: Date.now(),
    };
    if (!Number.isFinite(payload.lat) || !Number.isFinite(payload.lng)) {
      if (typeof cb === 'function') cb({ ok: false, reason: 'invalid_coords' });
      return;
    }

    const st = roomsState.get(room) || {};
    st.lastDest = payload;
    roomsState.set(room, st);

    io.to(room).emit('destination:live', payload);
    if (typeof cb === 'function') cb({ ok: true });
  });

  // --------- CHAT: HISTORY (con paginación opcional) ---------
  // Cliente puede enviar { limit?, before_ts? }
  socket.on('chat:history:get', async (req = {}) => {
    try {
      const limit = Math.min(Math.max(asNum(req.limit, 50), 1), 200); // 1..200
      const beforeTs = asNum(req.before_ts, 0); // si 0 => no aplica filtro

      const params = [asNum(reportId)];
      let sql = `
        SELECT report_id AS reportId, from_role AS \`from\`, sender_id AS senderId, \`text\`, ts
        FROM chat_messages
        WHERE report_id = ?
      `;
      if (beforeTs > 0) {
        sql += ` AND ts < ? `;
        params.push(beforeTs);
      }
      sql += ` ORDER BY ts DESC LIMIT ?`;
      params.push(limit);

      const [rows] = await pool.query(sql, params);
      // devolvemos ascendente para pintar natural en UI
      const ordered = [...rows].reverse();
      socket.emit('chat:history', ordered);
    } catch (e) {
      console.error('[chat] history error', e);
      socket.emit('chat:history', []); // fallback
    }
  });

  // --------- CHAT: SEND (INSERT + broadcast) ---------
  socket.on('chat:send', async (msg = {}) => {
    try {
      let text = String(msg.text || '').trim();
      if (!text) return;
      if (text.length > 2000) text = text.slice(0, 2000);

      const payload = {
        reportId: asNum(reportId),
        from: (asRole(msg.from) === 'system') ? safeRole : asRole(msg.from), // confía más en el socket
        senderId: (safeTecId ?? (Number.isFinite(asNum(msg.senderId)) ? asNum(msg.senderId) : null)),
        text,
        ts: asNum(msg.ts, Date.now()),
      };

      // INSERT en DB
      await pool.query(
        `INSERT INTO chat_messages (report_id, from_role, sender_id, text, ts)
         VALUES (?, ?, ?, ?, ?)`,
        [payload.reportId, payload.from, payload.senderId, payload.text, payload.ts]
      );

      // Broadcast a todos en la sala
      io.to(room).emit('chat:message', payload);
    } catch (e) {
      console.error('[chat] send error', e);
      // no emitimos nada en error
    }
  });

  socket.on('disconnect', (reason) => {
    console.log('[srv] disconnect', socket.id, reason);
  });
});

const PORT = Number(process.env.PORT || 3001);
server.listen(PORT, '0.0.0.0', () => console.log(`🟢 WS on http://localhost:${PORT}`));
