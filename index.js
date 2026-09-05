const {
  default: makeWASocket,
  DisconnectReason,
  fetchLatestBaileysVersion,
  BufferJSON
} = require('@whiskeysockets/baileys');
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const qrcode = require('qrcode');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const PORT = process.env.PORT || 3000;

// PostgreSQL Connection Pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const sessions = new Map();
const qrCodes = new Map();

// Initialize DB and Auto-Cleanup messages older than 90 days
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS auth_state (
      session_id TEXT DEFAULT 'default',
      id TEXT NOT NULL,
      data JSONB,
      PRIMARY KEY (session_id, id)
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      session_id TEXT DEFAULT 'default',
      jid TEXT,
      from_me BOOLEAN,
      sender_name TEXT,
      message_text TEXT,
      timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
  `);

  try {
    await pool.query(`ALTER TABLE auth_state ADD COLUMN IF NOT EXISTS session_id TEXT DEFAULT 'default';`);
    await pool.query(`ALTER TABLE messages ADD COLUMN IF NOT EXISTS session_id TEXT DEFAULT 'default';`);
  } catch (err) {}

  // Delete messages older than 90 days automatically
  await pool.query(`DELETE FROM messages WHERE timestamp < NOW() - INTERVAL '90 days';`);
  console.log('Database initialized & 90-day retention policy enforced.');
}

// Multi-Tenant Auth Store
async function usePostgresAuthState(sessionId) {
  const readData = async (type, id) => {
    try {
      const res = await pool.query(
        'SELECT data FROM auth_state WHERE session_id = $1 AND id = $2',
        [sessionId, `${type}-${id}`]
      );
      if (res.rows.length > 0) {
        return JSON.parse(JSON.stringify(res.rows[0].data), BufferJSON.reviver);
      }
    } catch (err) {}
    return null;
  };

  const writeData = async (data, type, id) => {
    try {
      const value = JSON.stringify(data, BufferJSON.replacer);
      await pool.query(
        'INSERT INTO auth_state (session_id, id, data) VALUES ($1, $2, $3) ON CONFLICT (session_id, id) DO UPDATE SET data = $3',
        [sessionId, `${type}-${id}`, value]
      );
    } catch (err) {}
  };

  const removeData = async (type, id) => {
    try {
      await pool.query('DELETE FROM auth_state WHERE session_id = $1 AND id = $2', [sessionId, `${type}-${id}`]);
    } catch (err) {}
  };

  const creds = (await readData('creds', 'main')) || (require('@whiskeysockets/baileys').initAuthCreds());

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(type, id);
              if (type === 'app-state-sync-key' && value) {
                value = require('@whiskeysockets/baileys').proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            })
          );
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              if (value) tasks.push(writeData(value, category, id));
              else tasks.push(removeData(category, id));
            }
          }
          await Promise.all(tasks);
        }
      }
    },
    saveCreds: () => writeData(creds, 'creds', 'main')
  };
}

// Save Messages and Emit to Frontend in Realtime
async function saveMessages(sessionId, messagesArray) {
  for (const m of messagesArray) {
    const text = m.message?.conversation || m.message?.extendedTextMessage?.text || JSON.stringify(m.message || {});
    if (!text || text === '{}') continue;

    const jid = m.key.remoteJid;
    const fromMe = m.key.fromMe;
    const senderName = m.pushName || (fromMe ? 'Me' : jid.split('@')[0]);

    const res = await pool.query(
      'INSERT INTO messages (session_id, jid, from_me, sender_name, message_text) VALUES ($1, $2, $3, $4, $5) RETURNING *',
      [sessionId, jid, fromMe, senderName, text]
    );

    io.emit('new-message', res.rows[0]);
  }
}

// Spin up a WhatsApp instance
async function startSession(sessionId) {
  if (sessions.has(sessionId)) return;

  const { state, saveCreds } = await usePostgresAuthState(sessionId);
  const { version } = await fetchLatestBaileysVersion();

  const sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: false,
    markOnlineOnConnect: true,
    
    // FIX FOR LOGIN DISCONNECT / SCAN FAILURE:
    syncFullHistory: false,                   // Disables heavy initial sync payload
    browser: ['Ubuntu', 'Chrome', '20.0.04'], // Custom user-agent for faster handshake
    connectTimeoutMs: 60000,                 // 60 second socket timeout
    defaultQueryTimeoutMs: 0,
    keepAliveIntervalMs: 10000
  });

  sessions.set(sessionId, sock);
  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (qr) {
      qrcode.toDataURL(qr, (err, url) => {
        if (!err) qrCodes.set(sessionId, url);
      });
    }
    if (connection === 'open') {
      console.log(`Session [${sessionId}] connected!`);
      qrCodes.delete(sessionId);
      io.emit('session-update');
    }
    if (connection === 'close') {
      sessions.delete(sessionId);
      io.emit('session-update');
      const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
      if (shouldReconnect) startSession(sessionId);
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type === 'notify') {
      await saveMessages(sessionId, messages);
      await sock.sendPresenceUpdate('available');
    }
  });
}

// Restore active sessions
async function restoreAllSessions() {
  await initDB();
  const res = await pool.query('SELECT DISTINCT session_id FROM auth_state WHERE session_id IS NOT NULL');
  for (const row of res.rows) {
    startSession(row.session_id);
  }
}

// API Routes
app.get('/api/chats', async (req, res) => {
  try {
    const query = `
      SELECT DISTINCT ON (session_id, jid) session_id, jid, sender_name, message_text, timestamp
      FROM messages
      WHERE timestamp >= NOW() - INTERVAL '90 days'
      ORDER BY session_id, jid, timestamp DESC;
    `;
    const { rows } = await pool.query(query);
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/messages', async (req, res) => {
  const { session_id, jid } = req.query;
  try {
    const { rows } = await pool.query(
      'SELECT * FROM messages WHERE session_id = $1 AND jid = $2 AND timestamp >= NOW() - INTERVAL \'90 days\' ORDER BY timestamp ASC',
      [session_id, jid]
    );
    res.json(rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>WhatsApp Web Console</title>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <script src="/socket.io/socket.io.js"></script>
      <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
        body { display: flex; height: 100vh; background-color: #111b21; color: #e9edef; }
        #sidebar { width: 30%; border-right: 1px solid #222d34; display: flex; flex-direction: column; background: #111b21; }
        #chat-window { width: 70%; display: flex; flex-direction: column; background: #0b141a; }
        .header { padding: 10px 16px; background: #202c33; display: flex; justify-content: space-between; align-items: center; font-weight: bold; }
        .add-box { padding: 10px; background: #111b21; border-bottom: 1px solid #222d34; }
        input, button { padding: 8px; border-radius: 6px; border: none; }
        input { background: #2a3942; color: white; width: 65%; }
        button { background: #00a884; color: white; font-weight: bold; cursor: pointer; width: 30%; }
        #chat-list { overflow-y: auto; flex: 1; }
        .chat-item { padding: 12px 16px; border-bottom: 1px solid #222d34; cursor: pointer; display: flex; flex-direction: column; }
        .chat-item:hover { background: #202c33; }
        .chat-title { font-weight: bold; display: flex; justify-content: space-between; margin-bottom: 4px; }
        .chat-preview { font-size: 0.85em; color: #8696a0; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
        .account-badge { font-size: 0.7em; background: #00a884; padding: 2px 6px; border-radius: 4px; color: black; margin-left: 5px; }
        #message-list { flex: 1; padding: 20px; overflow-y: auto; display: flex; flex-direction: column; gap: 8px; }
        .msg { max-width: 65%; padding: 8px 12px; border-radius: 8px; font-size: 0.9em; line-height: 1.4; word-wrap: break-word; }
        .msg.me { background: #005c4b; align-self: flex-end; color: #e9edef; }
        .msg.them { background: #202c33; align-self: flex-start; color: #e9edef; }
        .msg-time { font-size: 0.65em; color: #8696a0; text-align: right; margin-top: 4px; }
      </style>
    </head>
    <body>
      <div id="sidebar">
        <div class="header">WhatsApp Accounts</div>
        <div class="add-box">
          <form action="/create-session" method="POST">
            <input type="text" name="sessionId" placeholder="Device Name" required />
            <button type="submit">Add Device</button>
          </form>
        </div>
        <div id="chat-list">Loading chats...</div>
      </div>
      <div id="chat-window">
        <div class="header" id="active-chat-header">Select a chat to view messages</div>
        <div id="message-list"></div>
      </div>

      <script>
        const socket = io();
        let currentSession = null;
        let currentJid = null;

        async function loadChats() {
          const res = await fetch('/api/chats');
          const chats = await res.json();
          const list = document.getElementById('chat-list');
          list.innerHTML = '';

          chats.forEach(c => {
            const div = document.createElement('div');
            div.className = 'chat-item';
            div.onclick = () => openChat(c.session_id, c.jid, c.sender_name);
            div.innerHTML = \`
              <div class="chat-title">
                <span>\${c.sender_name || c.jid.split('@')[0]}</span>
                <span class="account-badge">\${c.session_id}</span>
              </div>
              <div class="chat-preview">\${c.message_text}</div>
            \`;
            list.appendChild(div);
          });
        }

        async function openChat(sessionId, jid, name) {
          currentSession = sessionId;
          currentJid = jid;
          document.getElementById('active-chat-header').innerText = \`\${name || jid.split('@')[0]} [\${sessionId}]\`;

          const res = await fetch(\`/api/messages?session_id=\${sessionId}&jid=\${jid}\`);
          const messages = await res.json();
          const msgList = document.getElementById('message-list');
          msgList.innerHTML = '';

          messages.forEach(m => {
            const div = document.createElement('div');
            div.className = \`msg \${m.from_me ? 'me' : 'them'}\`;
            div.innerHTML = \`
              <div>\${m.message_text}</div>
              <div class="msg-time">\${new Date(m.timestamp).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'})}</div>
            \`;
            msgList.appendChild(div);
          });
          msgList.scrollTop = msgList.scrollHeight;
        }

        socket.on('new-message', (msg) => {
          loadChats();
          if (currentSession === msg.session_id && currentJid === msg.jid) {
            openChat(currentSession, currentJid, msg.sender_name);
          }
        });

        socket.on('session-update', loadChats);
        loadChats();
      </script>
    </body>
    </html>
  `);
});

app.post('/create-session', (req, res) => {
  const { sessionId } = req.body;
  const cleanId = sessionId.trim().replace(/[^a-zA-Z0-9_-]/g, '');
  if (!cleanId) return res.redirect('/');
  
  startSession(cleanId);
  res.redirect(`/scan?id=${cleanId}`);
});

app.get('/scan', (req, res) => {
  const { id } = req.query;
  const qr = qrCodes.get(id);

  if (!qr && sessions.has(id)) {
    return res.send(`
      <body style="font-family:sans-serif;text-align:center;padding:40px;background:#111b21;color:white;">
        <h2>Device "${id}" Connected Successfully!</h2>
        <br><a href="/" style="color:#00a884;">Open Web Console</a>
      </body>
    `);
  }

  res.send(`
    <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta http-equiv="refresh" content="3">
      </head>
      <body style="font-family:sans-serif;display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;background:#111b21;color:white;">
        <h2>Scan QR Code for Device: ${id}</h2>
        ${qr ? `<img src="${qr}" style="width:250px;height:250px;margin:20px;border-radius:8px;" />` : '<p>Generating QR Code... please wait.</p>'}
        <br><a href="/" style="color:#00a884;">Back to Console</a>
      </body>
    </html>
  `);
});

server.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  await restoreAllSessions();
});
    
