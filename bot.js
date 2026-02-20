import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  jidNormalizedUser,
  makeCacheableSignalKeyStore,
  downloadContentFromMessage,
  fetchLatestBaileysVersion,
} from "@whiskeysockets/baileys";
import express from "express";
import http from "http";
import { Server } from "socket.io";
import Database from "better-sqlite3";
import P from "pino";
import fs from "fs";
import { Boom } from "@hapi/boom";

// --- 1. DATABASE SETUP ---
const db = new Database("system.db");
db.exec(`
    CREATE TABLE IF NOT EXISTS admins (id INTEGER PRIMARY KEY, name TEXT, phone TEXT, jid TEXT, status TEXT DEFAULT 'ACTIVE');
    CREATE TABLE IF NOT EXISTS clients (id INTEGER PRIMARY KEY, name TEXT, phone TEXT, jid TEXT, status TEXT DEFAULT 'ACTIVE');
    CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, order_id TEXT, customer TEXT, buyer_jid TEXT, content TEXT, admin_name TEXT, seller_forward_id TEXT, buyer_msg_id TEXT, status TEXT, time DATETIME DEFAULT CURRENT_TIMESTAMP);
    CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT);
`);

const app = express();
const server = http.createServer(app);
const io = new Server(server);
app.use(express.json());
app.use(express.static("public"));

let sock;
let qrCode = null;
let connectionStatus = "Disconnected";

const cleanPhone = (num) => (num ? num.replace(/\D/g, "") : "");

io.on("connection", (socket) => {
  socket.emit("connection_status", {
    status: connectionStatus,
    phone: sock?.user?.id ? sock.user.id.split(":")[0] : null,
  });
  if (qrCode && connectionStatus !== "Connected") socket.emit("qr", qrCode);
});

// --- 2. WHATSAPP ENGINE ---
async function startBot() {
  const { state, saveCreds } = await useMultiFileAuthState("auth_session");
  const { version } = await fetchLatestBaileysVersion();

  sock = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, P({ level: "silent" })),
    },
    printQRInTerminal: true,
    logger: P({ level: "silent" }),
    browser: ["OrderMaster Final", "Chrome", "1.0.0"],
    keepAliveIntervalMs: 10000, // Keeps connection "Hot"
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;
    if (qr) {
      qrCode = qr;
      io.emit("qr", qr);
    }
    if (connection === "open") {
      connectionStatus = "Connected";
      qrCode = null;
      io.emit("connection_status", {
        status: "Connected",
        phone: sock.user.id.split(":")[0],
      });
      console.log("✅ SYSTEM ONLINE");
    }
    if (connection === "close") {
      const code = (lastDisconnect.error instanceof Boom)?.output?.statusCode;
      if (code !== DisconnectReason.loggedOut) startBot();
    }
  });

  sock.ev.on("messages.upsert", async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.fromMe) return;

    const senderJid = jidNormalizedUser(msg.key.remoteJid);

    // 🎭 REACTION LOGIC (Mirror Admin emoji to Buyer)
    if (msg.message.reactionMessage) {
      const r = msg.message.reactionMessage;
      const admin = db
        .prepare("SELECT * FROM admins WHERE jid = ?")
        .get(senderJid);
      if (admin) {
        const order = db
          .prepare(
            "SELECT buyer_jid, buyer_msg_id FROM orders WHERE seller_forward_id = ?",
          )
          .get(r.key.id);
        if (order) {
          await sock.sendMessage(order.buyer_jid, {
            react: {
              text: r.text || "",
              key: {
                remoteJid: order.buyer_jid,
                fromMe: false,
                id: order.buyer_msg_id,
              },
            },
          });
        }
      }
      return;
    }

    const text =
      msg.message.conversation ||
      msg.message.extendedTextMessage?.text ||
      msg.message.documentMessage?.caption ||
      msg.message.imageMessage?.caption ||
      msg.message.documentMessage?.fileName ||
      "";

    // 🛡️ AUTHENTICATION & AUTO-LINKING
    let admin = db.prepare("SELECT * FROM admins WHERE jid = ?").get(senderJid);
    let client = db
      .prepare("SELECT * FROM clients WHERE jid = ?")
      .get(senderJid);

    if (!admin && !client) {
      const potentialNumbers = text.match(/\d{8,15}/g) || [];
      for (const rawNum of potentialNumbers) {
        const num = cleanPhone(rawNum);
        const pAdmin = db
          .prepare("SELECT * FROM admins WHERE REPLACE(phone, '+', '') = ?")
          .get(num);
        const pClient = db
          .prepare("SELECT * FROM clients WHERE REPLACE(phone, '+', '') = ?")
          .get(num);

        if (pAdmin) {
          db.prepare("UPDATE admins SET jid = ? WHERE id = ?").run(
            senderJid,
            pAdmin.id,
          );
          admin = db
            .prepare("SELECT * FROM admins WHERE jid = ?")
            .get(senderJid);
          await sock.sendMessage(senderJid, {
            text: `✅ Admin Linked: ${admin.name}`,
          });
          break;
        }
        if (pClient) {
          db.prepare("UPDATE clients SET jid = ? WHERE id = ?").run(
            senderJid,
            pClient.id,
          );
          client = db
            .prepare("SELECT * FROM clients WHERE jid = ?")
            .get(senderJid);
          await sock.sendMessage(senderJid, {
            text: `✅ Client Linked: ${client.name}`,
          });
          break;
        }
      }
    }

    if (!admin && !client) return;

    // 📦 CASE A: BUYER -> ADMIN FORWARDING
    if (client && !admin) {
      const orderNums = text.match(/\d{8,17}/g);
      if (!orderNums) return;

      const targetAdmin = db
        .prepare("SELECT * FROM admins WHERE status = 'ACTIVE' LIMIT 1")
        .get();
      if (targetAdmin) {
        const sent = await sock.sendMessage(targetAdmin.jid, { text: text });
        db.prepare(
          "INSERT INTO orders (order_id, customer, buyer_jid, content, admin_name, seller_forward_id, buyer_msg_id, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ).run(
          orderNums[0],
          client.name,
          senderJid,
          text,
          targetAdmin.name,
          sent.key.id,
          msg.key.id,
          "VALIDATED",
        );
        io.emit("new_order", {
          order_id: orderNums[0],
          customer: client.name,
          admin: targetAdmin.name,
          status: "VALIDATED",
        });
      }
    }

    // 📄 CASE B: ADMIN -> BUYER (STRICT 2M DELAY ON DUPLICATE)
    if (admin) {
      const isMedia = msg.message.documentMessage || msg.message.imageMessage;
      if (!isMedia) return;

      const orderMatch = text.match(/\d{4,17}/);
      if (orderMatch) {
        const last4 = orderMatch[0].slice(-4);
        const record = db
          .prepare(
            "SELECT * FROM orders WHERE order_id LIKE ? ORDER BY time DESC LIMIT 1",
          )
          .get(`%${last4}`);

        if (record) {
          // --- 🛡️ STRICT RULE: ONE-TIME DELIVERY ---
          if (record.status === "DELIVERED") {
            console.log(
              `[STRICT] Blocked resend for ${last4}. Triggering 2-minute delay.`,
            );

            // Capture the current message key and sender for the timer closure
            const targetKey = msg.key;
            const targetAdminJid = senderJid;

            setTimeout(async () => {
              try {
                if (sock) {
                  await sock.sendMessage(targetAdminJid, {
                    react: { text: "❓", key: targetKey },
                  });
                  console.log(
                    `[DELAY] Sent '?' reaction to Admin for duplicate order ending in ${last4}`,
                  );
                }
              } catch (e) {
                console.error("Delayed Reaction Error:", e.message);
              }
            }, 120000); // 120,000 ms = 2 Minutes

            return; // STOP HERE
          }

          // --- NORMAL FIRST-TIME DELIVERY ---
          try {
            const type = msg.message.documentMessage ? "document" : "image";
            const mediaContent =
              msg.message.documentMessage || msg.message.imageMessage;
            const stream = await downloadContentFromMessage(mediaContent, type);
            let buffer = Buffer.from([]);
            for await (const chunk of stream)
              buffer = Buffer.concat([buffer, chunk]);

            await sock.sendMessage(record.buyer_jid, {
              [type]: buffer,
              mimetype: mediaContent.mimetype,
              fileName: mediaContent.fileName || `Order_${record.order_id}.pdf`,
            });

            // Mark as Delivered so it can never be sent again
            db.prepare(
              "UPDATE orders SET status = 'DELIVERED' WHERE id = ?",
            ).run(record.id);
            console.log(`[SUCCESS] PDF Delivered to ${record.customer}`);
          } catch (err) {
            console.error("Forwarding failed:", err);
          }
        } else {
          // NO MATCH FOUND - React with 🚫 after 2 minutes to look human
          const targetKey = msg.key;
          const targetAdminJid = senderJid;
          setTimeout(async () => {
            try {
              await sock.sendMessage(targetAdminJid, {
                react: { text: "🚫", key: targetKey },
              });
            } catch (e) { }
          }, 120000);
        }
      }
    }
  });
}

// --- 3. API ROUTES ---
app.get("/api/stats", (req, res) => {
  const total = db.prepare("SELECT COUNT(*) as c FROM orders").get().c;
  const pending = db
    .prepare("SELECT COUNT(*) as c FROM orders WHERE status = 'VALIDATED'")
    .get().c;
  res.json({ total, pending, delivered: 0, activeAdmins: 1 });
});
app.get("/api/orders", (req, res) =>
  res.json(db.prepare("SELECT * FROM orders ORDER BY time DESC").all()),
);
app.get("/api/admins", (req, res) =>
  res.json(db.prepare("SELECT * FROM admins").all()),
);
app.get("/api/clients", (req, res) =>
  res.json(db.prepare("SELECT * FROM clients").all()),
);

app.post("/api/admins", (req, res) => {
  db.prepare("INSERT INTO admins (name, phone, jid) VALUES (?, ?, ?)").run(
    req.body.name,
    req.body.phone,
    `${req.body.phone}@s.whatsapp.net`,
  );
  res.json({ success: true });
});
app.post("/api/clients", (req, res) => {
  db.prepare("INSERT INTO clients (name, phone, jid) VALUES (?, ?, ?)").run(
    req.body.name,
    req.body.phone,
    `${req.body.phone}@s.whatsapp.net`,
  );
  res.json({ success: true });
});

// --- NEW DELETE ROUTES ---
app.delete("/api/admins/:id", (req, res) => {
  db.prepare("DELETE FROM admins WHERE id = ?").run(req.params.id);
  res.json({ success: true });
});
app.delete("/api/clients/:id", (req, res) => {
  db.prepare("DELETE FROM clients WHERE id = ?").run(req.params.id);
  res.json({ success: true });
});

app.post("/api/whatsapp/logout", async (req, res) => {
  if (sock) {
    try {
      await sock.logout();
    } catch (e) { }
  }
  fs.rmSync("./auth_session", { recursive: true, force: true });
  process.exit(0);
});

// --- 4. START SERVER ---
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Master Server Live: http://localhost:${PORT}`);
  startBot();
});
