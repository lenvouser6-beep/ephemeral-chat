const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;
const MESSAGE_TTL = 5 * 60 * 1000; // 5 minutes in ms

// ── In-memory store ────────────────────────────────────────────────
const messages = new Map();   // id → { id, user, text, color, timestamp }
const users = new Map();      // ws → { name, color }

const COLORS = [
  '#6C5CE7', '#00B894', '#E17055', '#0984E3',
  '#D63031', '#E84393', '#00CEC9', '#FDCB6E',
  '#A29BFE', '#55EFC4', '#FF7675', '#74B9FF',
];
let colorIdx = 0;

// ── Serve static files ────────────────────────────────────────────
app.use(express.static(path.join(__dirname, 'public')));

// ── Broadcast helper ───────────────────────────────────────────────
function broadcast(data, exclude) {
  const payload = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client !== exclude && client.readyState === 1) {
      client.send(payload);
    }
  });
}

// ── Schedule message expiry ────────────────────────────────────────
function scheduleExpiry(id) {
  setTimeout(() => {
    if (messages.delete(id)) {
      broadcast({ type: 'delete', id });
    }
  }, MESSAGE_TTL);
}

// ── WebSocket handling ─────────────────────────────────────────────
wss.on('connection', ws => {
  const color = COLORS[colorIdx++ % COLORS.length];

  ws.on('message', raw => {
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    // ── Join ───────────────────────────────────────────────────────
    if (data.type === 'join') {
      const name = (data.name || 'Anon').slice(0, 20);
      users.set(ws, { name, color });

      // Send existing (still-alive) messages
      ws.send(JSON.stringify({
        type: 'history',
        messages: [...messages.values()],
        you: { name, color },
        onlineCount: users.size,
      }));

      broadcast({ type: 'online', count: users.size });
      broadcast({
        type: 'system',
        text: `${name} joined the chat`,
      });
      return;
    }

    // ── Chat message ───────────────────────────────────────────────
    if (data.type === 'msg') {
      const user = users.get(ws);
      if (!user) return;

      const text = (data.text || '').trim().slice(0, 1000);
      if (!text) return;

      const msg = {
        id: crypto.randomUUID(),
        user: user.name,
        color: user.color,
        text,
        timestamp: Date.now(),
      };

      messages.set(msg.id, msg);
      scheduleExpiry(msg.id);

      broadcast({ type: 'msg', ...msg });
      // Also send back to sender so they see it rendered
      ws.send(JSON.stringify({ type: 'msg', ...msg }));
    }
  });

  ws.on('close', () => {
    const user = users.get(ws);
    users.delete(ws);
    if (user) {
      broadcast({ type: 'online', count: users.size });
      broadcast({
        type: 'system',
        text: `${user.name} left the chat`,
      });
    }
  });
});

// ── Start ──────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`✨ Ephemeral Chat running → http://localhost:${PORT}`);
});
