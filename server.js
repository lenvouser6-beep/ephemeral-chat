const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');
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

// ── Debug helpers ─────────────────────────────────────────────────
function dbg(...args) {
  if (process.env.DEBUG || process.env.NODE_ENV === 'development') {
    console.debug('[debug]', ...args);
  }
}

function logRequest(req) {
  console.log(`${new Date().toISOString()}  ${req.method} ${req.path}`);
}

// ── Serve static files with explicit fallback and debug logging ────
const publicDir = path.join(__dirname, 'public');
const staticMiddleware = express.static(publicDir);

// Log all requests
app.use((req, res, next) => {
  logRequest(req);
  next();
});

// Use static middleware but detect "misses" so we can fallback to index.html
app.use((req, res, next) => {
  staticMiddleware(req, res, (err) => {
    if (err) return next(err);
    // static didn't handle the request (miss)
    dbg(`static miss -> ${req.path}`);
    next();
  });
});

// Explicit fallback for single-page apps or unmatched routes
app.get('*', (req, res) => {
  const reqPath = req.path;
  const indexPath = path.join(publicDir, 'index.html');

  // If index.html exists, serve it and log; otherwise render a helpful error page
  fs.access(indexPath, fs.constants.R_OK, (err) => {
    if (err) {
      console.error(`index.html not found when trying to serve ${reqPath}:`, err);
      res.status(500).send(`<!doctype html><html><head><title>Server error</title></head><body><h1>500 — index.html missing</h1><p>Requested path: <code>${reqPath}</code></p><p>Looked for: <code>${indexPath}</code></p><pre>${String(err)}</pre></body></html>`);
      return;
    }

    res.sendFile(indexPath, (sendErr) => {
      if (sendErr) {
        console.error(`Failed to send index.html for ${reqPath}:`, sendErr);
        res.status(500).send(`<!doctype html><html><head><title>Server error</title></head><body><h1>500 — Failed to send index.html</h1><p>Requested path: <code>${reqPath}</code></p><pre>${String(sendErr)}</pre></body></html>`);
      } else {
        dbg(`Served index.html for ${reqPath}`);
      }
    });
  });
});

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
  console.log('New websocket connection — assigned color', color);

  ws.on('message', raw => {
    let data;
    try { data = JSON.parse(raw); } catch (e) { dbg('Invalid JSON from ws:', raw); return; }

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
    console.log('Websocket closed — remaining users:', users.size);
  });

  ws.on('error', (err) => {
    console.error('Websocket error:', err);
  });
});

// ── Start ──────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`✨ Ephemeral Chat running → http://localhost:${PORT}`);
});
