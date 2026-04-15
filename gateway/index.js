const express=require("express");
const http=require("http");
const WebSocket=require("ws");
const axios=require("axios");

const app=express();
app.use(express.json());
app.use((req,res,next)=>{
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Methods","GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers","Content-Type");
  if(req.method==="OPTIONS") return res.sendStatus(204);
  next();
});

const server=http.createServer(app);
const wss=new WebSocket.Server({server});
const PORT=parseInt(process.env.PORT)||4000;

const REPLICAS=[
  process.env.REPLICA1_URL||"http://replica1:3001",
  process.env.REPLICA2_URL||"http://replica2:3002",
  process.env.REPLICA3_URL||"http://replica3:3003",
];

let currentLeader=null;
let lastBroadcastIndex=-1;
let consecutiveLeaderMisses=0;
let lastLeaderHealthyAt=0;

async function discoverLeader(){
  const results=await Promise.allSettled(
    REPLICAS.map(url=>
      axios.get(url+"/status",{timeout:800})
        .then(res=>({url,role:res.data.role,term:res.data.term}))
        .catch(()=>null)
    )
  );

  let found=null;
  for(const r of results){
    if(r.status==="fulfilled"&&r.value&&r.value.role==="leader"){
      if(!found||r.value.term>found.term) found=r.value;
    }
  }

  if(found){
    consecutiveLeaderMisses=0;
    lastLeaderHealthyAt=Date.now();
    if(currentLeader!==found.url){
      console.log("[GATEWAY] New leader: "+found.url);
      // FIX: only reset lastBroadcastIndex when leader URL actually changes
      // to a DIFFERENT replica — not when recovering from a null state back
      // to the same replica. Ask the new leader what its commitIndex is and
      // set our index to match so we don't re-pull the entire history.
      const prevLeader=currentLeader;
      currentLeader=found.url;
      if(prevLeader!==null){
        // Genuine leader change — new replica, reset and re-pull
        lastBroadcastIndex=-1;
      } else {
        // Recovering from null (brief election gap) back to same or new leader
        // Sync our index to what the leader has already committed so we don't
        // re-broadcast the entire history to clients who already have it
        try{
          const s=await axios.get(currentLeader+"/status",{timeout:300});
          // Set to commitIndex so poll only pulls NEW strokes going forward
          lastBroadcastIndex=s.data.commitIndex;
          console.log("[GATEWAY] Resuming from commitIndex "+lastBroadcastIndex);
        } catch(e){
          lastBroadcastIndex=-1;
        }
      }
    }
  } else {
    // Keep leader sticky during status-poll misses. We only clear leader from
    // the write path when all direct intake probes fail.
    consecutiveLeaderMisses++;
  }
}

setInterval(discoverLeader,200);
discoverLeader();

const clients=new Set();

// Pending queue — strokes that failed during failover gap
const pendingStrokes=[];
let retryTimer=null;
let isRetrying=false;
let recoveryTickRunning=false;
const deliveredStrokeIds=new Set();
const deliveredStrokeOrder=[];
const MAX_DELIVERED_STROKES=5000;

function markDelivered(stroke){
  if(!stroke || !stroke.id) return true;
  if(deliveredStrokeIds.has(stroke.id)) return false;
  deliveredStrokeIds.add(stroke.id);
  deliveredStrokeOrder.push(stroke.id);
  if(deliveredStrokeOrder.length>MAX_DELIVERED_STROKES){
    const oldId=deliveredStrokeOrder.shift();
    deliveredStrokeIds.delete(oldId);
  }
  return true;
}

function broadcastStroke(stroke, excludeWs = null){
  if(!markDelivered(stroke)) return false;
  for(const client of clients){
    if(client === excludeWs) continue;
    if(client.readyState===WebSocket.OPEN){
      client.send(JSON.stringify({type:"stroke",stroke}));
    }
  }
  return true;
}

// Poll fallback — pulls any committed strokes from leader we haven't sent yet
// Only runs when we have a leader and only pulls genuinely new entries
async function pollAndBroadcastCommitted(){
  if(!currentLeader) return;
  try{
    const res=await axios.get(currentLeader+"/sync-log",{
      params:{from:lastBroadcastIndex+1},
      timeout:800,
    });
    lastLeaderHealthyAt=Date.now();
    const entries=(res.data&&res.data.entries)||[];
    if(!entries.length) return;
    // Safety cap: never pull more than 20 at once to avoid flooding browsers
    const batch=entries.slice(0,20);
    for(const item of batch){
      const entryIndex=(typeof item.index==="number") ? item.index : lastBroadcastIndex+1;
      if(entryIndex>lastBroadcastIndex){
        broadcastStroke(item.entry);
        lastBroadcastIndex=entryIndex;
      }
    }
    if(batch.length>0){
      console.log("[GATEWAY] Poll pulled "+batch.length+" stroke(s), index now "+lastBroadcastIndex);
    }
  } catch(e){}
}

setInterval(pollAndBroadcastCommitted,100);

async function runRecoveryTick(){
  if(recoveryTickRunning) return;
  recoveryTickRunning=true;
  try{
    await discoverLeader();
    await pollAndBroadcastCommitted();
    if(pendingStrokes.length>0 && !isRetrying){
      flushPending();
    }
  } finally{
    recoveryTickRunning=false;
  }
}

async function forwardToLeader(stroke){
  const attemptIntake = async (url) => {
    const res=await axios.post(url+"/intake",{stroke},{timeout:1200});
    if(res.data && (res.data.committed || res.data.accepted || res.data.ok)){
      currentLeader=url;
      consecutiveLeaderMisses=0;
      lastLeaderHealthyAt=Date.now();
      return true;
    }
    return false;
  };

  if(!currentLeader) await discoverLeader();
  if(currentLeader){
    try{
      if(await attemptIntake(currentLeader)){
        return {ok:true};
      }
    } catch(err){
      // fall through to leader rediscovery + direct probing
    }
  }

  await discoverLeader();
  if(currentLeader){
    try{
      if(await attemptIntake(currentLeader)){
        return {ok:true};
      }
    } catch(err){
      // fall through to probing all replicas
    }
  }

  // Active fallback: probe every replica directly. A follower returns not-leader,
  // but the actual leader should accept and commit, even if status polling lagged.
  for(const url of REPLICAS){
    try{
      if(await attemptIntake(url)){
        return {ok:true};
      }
    } catch(err){
      // keep probing remaining replicas
    }
  }

  // Final confirm step: discovery might still find a reachable leader even if
  // intake attempts failed transiently (timeout/load spike).
  await discoverLeader();
  if(currentLeader){
    return {ok:false,reason:"leader-busy"};
  }

  console.log("[GATEWAY] No leader — election in progress");
  return {ok:false,reason:"no-reachable-leader"};
}

// FIX: retry queue now drains properly and stops when empty
// Old code could stack multiple retryTimers
async function flushPending(){
  if(isRetrying||!pendingStrokes.length) return;
  isRetrying=true;
  while(pendingStrokes.length>0){
    const stroke=pendingStrokes[0];
    const result=await forwardToLeader(stroke);
    if(result.ok){
      pendingStrokes.shift();
    } else {
      // Back off before retrying
      await runRecoveryTick();
      await new Promise(r=>setTimeout(r,300));
    }
  }
  isRetrying=false;
}

wss.on("connection",(ws)=>{
  clients.add(ws);
  console.log("[GATEWAY] Client connected. Total: "+clients.size);
  ws.on("message",async(raw)=>{
    let stroke;
    try{ stroke=JSON.parse(raw); } catch(e){ return; }
    if(!stroke.id){
      stroke.id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    const result=await forwardToLeader(stroke);
    if(!result.ok){
      pendingStrokes.push(stroke);
      runRecoveryTick();
      if(!isRetrying) flushPending();
    }
  });
  ws.on("close",()=>{
    clients.delete(ws);
    console.log("[GATEWAY] Client disconnected. Total: "+clients.size);
  });
});

// Fast-path broadcast called by leader after commit
app.post("/broadcast",(req,res)=>{
  const{stroke,index}=req.body;
  broadcastStroke(stroke);
  if(typeof index==="number"){
    lastBroadcastIndex=Math.max(lastBroadcastIndex,index);
  } else{
    lastBroadcastIndex++;
  }
  res.json({ok:true,delivered:clients.size});
});

app.get("/status",(req,res)=>{
  res.json({ok:true,leader:currentLeader,clients:clients.size,
    pending:pendingStrokes.length,lastBroadcastIndex});
});

server.listen(PORT,()=>{
  console.log("[GATEWAY] Listening on port "+PORT);
});