const express=require("express");
const http=require("http");
const WebSocket=require("ws");
const axios=require("axios");

const app=express();
app.use(express.json());
const server = http.createServer(app);
const wss=new WebSocket.Server({server});
const PORT=parseInt(process.env.PORT)||4000;

const REPLICAS=[
    process.env.REPLICA1_URL||"http://replica1:3001",
  process.env.REPLICA2_URL||"http://replica2:3002",
  process.env.REPLICA3_URL||"http://replica3:3003",
];

//leader discovery
let currentLeader=null;
async function discoverLeader(){
    for (const url of REPLICAS) {
    try {
      const res = await axios.get(`${url}/status`, { timeout: 300 });
      if (res.data.role === "leader") {
        if (currentLeader !== url) {
          console.log(`[GATEWAY] New leader discovered: ${url}`);
          currentLeader = url;
        }
        return;
      }
    } catch {}
  }
  console.log("[GATEWAY] No leader found yet — election in progress?");
  currentLeader = null;
}

// Poll every 200ms
setInterval(discoverLeader, 200);
discoverLeader(); // run immediately on startup

//WebSocket clients
const clients = new Set();

function broadcastStroke(stroke, excludeWs = null) {
  for (const client of clients) {
    if (client === excludeWs) continue;
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify({ type: "stroke", stroke }));
    }
  }
}

wss.on("connection", (ws) => {
  clients.add(ws);
  console.log(`[GATEWAY] Client connected. Total: ${clients.size}`);

  ws.on("message", async (raw) => {
    let stroke;
    try {
      stroke = JSON.parse(raw);
    } catch {
      return;
    }

    console.log("[GATEWAY] Stroke received:", stroke);

    // Forward to leader
    if (!currentLeader) {
      ws.send(JSON.stringify({ error: "No leader available, try again shortly" }));
      return;
    }

    try {
      await axios.post(`${currentLeader}/append-entries`, {
        term: 0,           // real term injected by leader in Day 3
        leaderId: "gateway-forwarded",
        entry: stroke,
        prevLogIndex: -1,
      });
      // Keep clients in sync immediately after leader accepts the entry.
      broadcastStroke(stroke, ws);
    } catch (err) {
      console.error("[GATEWAY] Failed to reach leader:", err.message);
      currentLeader = null; // force rediscovery
    }
  });

  ws.on("close", () => {
    clients.delete(ws);
    console.log(`[GATEWAY] Client disconnected. Total: ${clients.size}`);
  });
});

//Broadcast endpoint — called by leader after commit
app.post("/broadcast", (req, res) => {
  const { stroke } = req.body;
  console.log("[GATEWAY] Broadcasting committed stroke to", clients.size, "clients");
  broadcastStroke(stroke);

  res.json({ ok: true, delivered: clients.size });
});

//Health check
app.get("/status", (req, res) => {
  res.json({ ok: true, leader: currentLeader, clients: clients.size });
});

//Start
server.listen(PORT, () => {
  console.log(`[GATEWAY] Listening on port ${PORT}`);
});