import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  jidNormalizedUser,
  makeCacheableSignalKeyStore,
  downloadContentFromMessage,
  fetchLatestBaileysVersion,
} from "@whiskeysockets/baileys";
import express from "express";
import { createServer } from "http";
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
`);

const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer);
app.use(express.json());
app.use(express.static("public"));

let sock;
let qrCode = null;
let connectionStatus = "Disconnected";

const cleanPhone = (num) => (num ? num.replace(/\D/g, "") : "");
const ALLOWED_LENGTHS = [6, 7, 8, 9, 10, 12, 13, 17];

io.on("connection", (socket) => {
  socket.emit("connection_status", {
    status: connectionStatus,
    phone: sock?.user?.id ? sock.user.id.split(":")[0] : null,
  });
  if (qrCode && connectionStatus !== "Connected") socket.emit("qr", qrCode);
});

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
    browser: ["OrderMaster Admin", "Chrome", "1.0.0"],
    keepAliveIntervalMs: 10000,
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

    // Get text from every possible source (Caption, Message, Filename)
    const text =
      msg.message.conversation ||
      msg.message.extendedTextMessage?.text ||
      msg.message.documentMessage?.caption ||
      msg.message.imageMessage?.caption ||
      msg.message.documentMessage?.fileName ||
      "";

    // 🛡️ 1. ADMIN IDENTIFICATION (UPGRADED)
    // Check by JID OR by Phone Number contained in the LID string
    let admin = db
      .prepare("SELECT * FROM admins WHERE jid = ? OR phone = ?")
      .get(senderJid, senderJid.split("@")[0]);

    if (!admin && senderJid.endsWith("@lid")) {
      const hiddenPn = msg.verifiedName || msg.key.remoteJidAlt;
      const potentialPn = hiddenPn ? cleanPhone(hiddenPn.split("@")[0]) : null;
      if (potentialPn) {
        admin = db
          .prepare("SELECT * FROM admins WHERE REPLACE(phone, '+', '') = ?")
          .get(potentialPn);
        if (admin) {
          db.prepare("UPDATE admins SET jid = ? WHERE id = ?").run(
            senderJid,
            admin.id,
          );
          console.log(`[SYNC] Admin ${admin.name} linked to LID.`);
        }
      }
    }

    // 🎭 2. REACTION LOGIC
    if (msg.message.reactionMessage && admin) {
      const r = msg.message.reactionMessage;
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
      return;
    }

    const isMedia = !!(msg.message.imageMessage || msg.message.documentMessage);
    const isText = !!(
      msg.message.conversation || msg.message.extendedTextMessage
    );

    // 📄 3. ADMIN LOGIC (PDF/IMAGE RESPONSE)
    if (admin) {
      if (!isMedia) return; // Ignore Admin text to prevent loops

      const orderMatch = text.match(/\d{4,17}/);
      if (orderMatch) {
        const last4 = orderMatch[0].slice(-4);
        console.log(
          `[ADMIN] Media received. Searching for Order ending in: ${last4}`,
        );

        const record = db
          .prepare(
            "SELECT * FROM orders WHERE order_id LIKE ? AND status = 'VALIDATED' ORDER BY time DESC LIMIT 1",
          )
          .get(`%${last4}`);

        if (record) {
          try {
            console.log(
              `[SUCCESS] Found Order #${record.order_id}. Sending to Buyer: ${record.buyer_jid}`,
            );

            const mediaType = msg.message.documentMessage
              ? "document"
              : "image";
            const mediaSubObject =
              msg.message.documentMessage || msg.message.imageMessage;

            const stream = await downloadContentFromMessage(
              mediaSubObject,
              mediaType,
            );
            let buffer = Buffer.from([]);
            for await (const chunk of stream)
              buffer = Buffer.concat([buffer, chunk]);

            await sock.sendMessage(record.buyer_jid, {
              [mediaType]: buffer,
              mimetype: mediaSubObject.mimetype,
              fileName:
                mediaSubObject.fileName || `Order_${record.order_id}.pdf`,
            
            });

            db.prepare(
              "UPDATE orders SET status = 'DELIVERED' WHERE id = ?",
            ).run(record.id);
            console.log(`[DELIVERED] PDF successfully sent to Buyer.`);
          } catch (err) {
            console.error("[ERROR] PDF Send failed:", err);
          }
        } else {
          console.log(`[NOT FOUND] No open order matches '${last4}'`);
          const alreadyDone = db
            .prepare(
              "SELECT id FROM orders WHERE order_id LIKE ? AND status = 'DELIVERED'",
            )
            .get(`%${last4}`);
          const emoji = alreadyDone ? "❓" : "🚫";
          setTimeout(async () => {
            try {
              await sock.sendMessage(senderJid, {
                react: { text: emoji, key: msg.key },
              });
            } catch (e) {}
          }, 120000);
        }
      }
      return; // Exit Admin logic
    }

    // 📦 4. BUYER LOGIC (STRICT TEXT ONLY)
    if (!admin) {
      if (isMedia || !isText) return;

      const allNumbers = text.match(/\d+/g) || [];
      const validOrders = allNumbers.filter((num) =>
        ALLOWED_LENGTHS.includes(num.length),
      );

      if (validOrders.length > 0) {
        const targetAdmin = db
          .prepare("SELECT * FROM admins WHERE status = 'ACTIVE' LIMIT 1")
          .get();
        if (targetAdmin) {
          const client = db
            .prepare("SELECT * FROM clients WHERE jid = ?")
            .get(senderJid);
          const customerName = client
            ? client.name
            : `@${senderJid.split("@")[0]}`;

          console.log(
            `[BUYER] Order ${validOrders[0]} detected. Forwarding to Admin...`,
          );
          const sent = await sock.sendMessage(targetAdmin.jid, { text: text });

          db.prepare(
            "INSERT INTO orders (order_id, customer, buyer_jid, content, admin_name, seller_forward_id, buyer_msg_id, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
          ).run(
            validOrders[0],
            customerName,
            senderJid,
            text,
            targetAdmin.name,
            sent.key.id,
            msg.key.id,
            "VALIDATED",
          );

          io.emit("new_order", {
            order_id: validOrders[0],
            customer: customerName,
            admin: targetAdmin.name,
            status: "VALIDATED",
          });
        }
      }
    }
  });
}

// --- API ---
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
    } catch (e) {}
  }
  fs.rmSync("./auth_session", { recursive: true, force: true });
  process.exit(0);
});

const PORT = process.env.PORT || 3050;
httpServer.listen(PORT, () => {
  console.log(`🚀 Master Server Live: http://localhost:${PORT}`);
  startBot();
});
