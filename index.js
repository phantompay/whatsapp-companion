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
const PORT = process.env.PORT || 3000;

let qrCodeData = null;
let sock = null;

// Initialize PostgreSQL connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Database initialization
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS auth_state (
      id TEXT PRIMARY KEY,
      data JSONB
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      jid TEXT,
      from_me BOOLEAN,
      sender_name TEXT,
      message_text TEXT,
      timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
  `);
  console.log('Database initialized successfully.');
}

// Custom Baileys Auth Store using Postgres
async function usePostgresAuthState() {
  const readData = async (type, id) => {
    try {
      const res = await pool.query('SELECT data FROM auth_state WHERE id = $1', [`${type}-${id}`]);
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
        'INSERT INTO auth_state (id, data) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET data = $2',
        [`${type}-${id}`, value]
      );
    } catch (err) {
      console.error(`Error writing ${type}-${id}:`, err.message);
    }
  };

  const removeData = async (type, id) => {
    try {
      await pool.query('DELETE FROM auth_state WHERE id = $1', [`${type}-${id}`]);
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

// Save messages to database
async function saveMessages(messagesArray) {
  for (const m of messagesArray) {
    const text = m.message?.conversation || m.message?.extendedTextMessage?.text || JSON.stringify(m.message || {});
    await pool.query(
      'INSERT INTO messages (jid, from_me, sender_name, message_text) VALUES ($1, $2, $3, $4)',
      [m.key.remoteJid, m.key.fromMe, m.pushName || 'Unknown', text]
    );
  }
}

// Main WhatsApp Client Setup
async function startWhatsApp() {
  await initDB();
  const { state, saveCreds } = await usePostgresAuthState();
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    auth: state,
    printQRInTerminal: true,
    markOnlineOnConnect: true,
    syncFullHistory: true
  });

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      qrcode.toDataURL(qr, (err, url) => {
        if (!err) qrCodeData = url;
      });
    }

    if (connection === 'open') {
      console.log('Connected to WhatsApp successfully!');
      qrCodeData = null;
      startPresenceLoop();
    }

    if (connection === 'close') {
      const shouldReconnect = lastDisconnect?.error?.output?.statusCode !== DisconnectReason.loggedOut;
      if (shouldReconnect) startWhatsApp();
    }
  });

  sock.ev.on('messaging-history.set', async ({ messages }) => {
    console.log(`History sync received: ${messages.length} messages.`);
    await saveMessages(messages);
  });

  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type === 'notify') {
      await saveMessages(messages);
      if (sock) await sock.sendPresenceUpdate('available');
    }
  });
}

function startPresenceLoop() {
  setInterval(async () => {
    if (sock) {
      try {
        await sock.sendPresenceUpdate('available');
      } catch (err) {
        console.error('Presence error:', err.message);
      }
    }
  }, 25000);
}

// Routes
app.get('/health', (req, res) => res.status(200).send('OK'));

app.get('/', (req, res) => {
  if (qrCodeData) {
    return res.send(`
      <html>
        <body style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;font-family:sans-serif;">
          <h2>Scan QR Code to link WhatsApp Companion Device</h2>
          <img src="${qrCodeData}" alt="QR Code" />
        </body>
      </html>
    `);
  }
  res.send(`
    <html>
      <body style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;font-family:sans-serif;">
        <h2>WhatsApp Bot Status: ACTIVE</h2>
        <p>Presence set to Online every 25 seconds.</p>
        <p>Session data and messages synced to Supabase Postgres.</p>
      </body>
    </html>
  `);
});

app.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
  startWhatsApp();
});
                   
