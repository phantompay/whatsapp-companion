const {
  default: makeWASocket,
  DisconnectReason,
  fetchLatestBaileysVersion,
  BufferJSON
} = require('@whiskeysockets/baileys');
const express = require('express');
const qrcode = require('qrcode');
const { Pool } = require('pg');

const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json());

const PORT = process.env.PORT || 3000;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

const sessions = new Map(); // Stores active sockets
const qrCodes = new Map();  // Stores active QR codes per session

// Initialize Database Tables
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS auth_state (
      session_id TEXT,
      id TEXT,
      data JSONB,
      PRIMARY KEY (session_id, id)
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      session_id TEXT,
      jid TEXT,
      from_me BOOLEAN,
      sender_name TEXT,
      message_text TEXT,
      timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
  `);
  console.log('Database initialized successfully.');
}

// Multi-tenant Auth Store
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
    } catch (err) {
      console.error(`Error reading ${type}-${id}:`, err.message);
    }
    return null;
  };

  const writeData = async (data, type, id) => {
    try {
      const value = JSON.stringify(data, BufferJSON.replacer);
      await pool.query(
        'INSERT INTO auth_state (session_id, id, data) VALUES ($1, $2, $3) ON CONFLICT (session_id, id) DO UPDATE SET data = $3',
        [sessionId, `${type}-${id}`, value]
      );
    } catch (err) {
      console.error(`Error writing ${type}-${id}:`, err.message);
    }
  };

  const removeData = async (type, id) => {
    try {
      await pool.query('DELETE FROM auth_state WHERE session_id = $1 AND id = $2', [sessionId, `${type}-${id}`]);
    } catch (err) {
      console.error(`Error removing ${type}-${id}:`, err.message);
    }
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
              if (value) {
                tasks.push(writeData(value, category, id));
              } else {
                tasks.push(removeData(category, id));
              }
            }
          }
          await Promise.all(tasks);
        }
      }
    },
    saveCreds: () => writeData(creds, 'creds', 'main')
  };
}

// Save messages mapped to specific account
async function saveMessages(sessionId, messagesArray) {
  for (const m of messagesArray) {
    const text = m.message?.conversation || m.message?.extendedTextMessage?.text || JSON.stringify(m.message || {});
    await pool.query(
      'INSERT INTO messages (session_id, jid, from_me, sender_name, message_text) VALUES ($1, $2, $3, $4, $5)',
      [sessionId, m.key.remoteJid, m.key.fromMe, m.pushName || 'Unknown', text]
    );
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
    syncFullHistory: true
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
      console.log(`Session [${sessionId}] connected successfully!`);
      qrCodes.delete(sessionId);
    }

    if (connection === 'close') {
      sessions.delete(sessionId);
      const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
      if (shouldReconnect) {
        startSession(sessionId);
      }
    }
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type === 'notify') {
      await saveMessages(sessionId, messages);
      await sock.sendPresenceUpdate('available');
    }
  });
}

// Reconnect all saved accounts on boot
async function restoreAllSessions() {
  await initDB();
  const res = await pool.query('SELECT DISTINCT session_id FROM auth_state');
  for (const row of res.rows) {
    console.log(`Restoring session: ${row.session_id}`);
    startSession(row.session_id);
  }
}

// Keeping status online across all sessions
setInterval(async () => {
  for (const [id, sock] of sessions.entries()) {
    try {
      await sock.sendPresenceUpdate('available');
    } catch (e) {}
  }
}, 25000);

// Routes
app.get('/health', (req, res) => res.status(200).send('OK'));

app.get('/', async (req, res) => {
  const activeList = Array.from(sessions.keys());
  let html = `
    <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
          body { font-family: sans-serif; padding: 20px; max-width: 500px; margin: auto; }
          .card { border: 1px solid #ccc; padding: 15px; border-radius: 8px; margin-bottom: 15px; }
          input, button { padding: 10px; width: 100%; margin-top: 5px; box-sizing: border-box; }
          button { background: #25D366; border: none; color: white; font-weight: bold; cursor: pointer; border-radius: 4px; }
        </style>
      </head>
      <body>
        <h2>WhatsApp Multi-Account Manager</h2>
        <div class="card">
          <h3>Add New Account / Device</h3>
          <form action="/create-session" method="POST">
            <input type="text" name="sessionId" placeholder="Enter Device Name (e.g., Business1)" required />
            <button type="submit" style="margin-top:10px;">Generate QR Code</button>
          </form>
        </div>
        <div class="card">
          <h3>Active Accounts (${activeList.length})</h3>
          <ul>
            ${activeList.length > 0 ? activeList.map(s => `<li><b>${s}</b> - Connected</li>`).join('') : '<li>No connected accounts yet.</li>'}
          </ul>
        </div>
      </body>
    </html>
  `;
  res.send(html);
});

app.post('/create-session', (req, res) => {
  const { sessionId } = req.body;
  const cleanId = sessionId.trim().replace(/[^a-zA-Z0-0_-]/g, '');
  if (!cleanId) return res.redirect('/');
  
  startSession(cleanId);
  res.redirect(`/scan?id=${cleanId}`);
});

app.get('/scan', (req, res) => {
  const { id } = req.query;
  const qr = qrCodes.get(id);

  if (!qr && sessions.has(id)) {
    return res.send(`
      <body style="font-family:sans-serif;text-align:center;padding:40px;">
        <h2>Device "${id}" is Connected!</h2>
        <a href="/">Go back to dashboard</a>
      </body>
    `);
  }

  res.send(`
    <html>
      <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta http-equiv="refresh" content="3">
      </head>
      <body style="font-family:sans-serif;display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;">
        <h2>Scan QR for Device: ${id}</h2>${qr ? `<img src="${qr}" style="width:250px;height:250px;" />` : '<p>Generating QR Code... please wait.</p>'}
        <br><a href="/">Back to Dashboard</a>
      </body>
    </html>
  `);
});

app.listen(PORT, async () => {
  console.log(`Server started on port ${PORT}`);
  await restoreAllSessions();
});
  
