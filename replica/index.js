const express=require("express")
const app=express()
app.use(express.json());

//Node identitiy
const REPLICA_ID=process.env.REPLICA_ID || "replica1";
const PORT=parseInt(process.env.PORT) || 3001;
const PEERS=(process.env.PEERS||"").split(",").filter(Boolean);
const log=(...args)=>console.log(`[${REPLICA_ID}]`,...args);

//Stubbed state
let state={
    role:"follower",
    term:0,
    votedFor:null,
    log:[],
    commitIndex:-1,
}

//RPC endpoints
app.post("/request-vote",(req,res)=>{
    const{term,candidateId}=req.body;
    log(`/request-vote from ${candidateId} (term ${term})`);
    res.json({voteGranted:true, term:state.term});
});
app.post("/append-entries",(req,res)=>{
    const{term,leaderId,entry,prevLogIndex}=req.body;
    log(`/append-entries from ${leaderId}-entry:`,entry||"(heartbeat)");
    res.json({success:true,logLength:state.log.length});
});
app.post("/heartbeat",(req,res)=>{
    const{term,leaderId}=req.body;
    log(`/heartbeat from ${leaderId}`);
    res.json({success:true});
});
app.get("sync-log",(req,res)=>{
    const fromIndex=parseInt(req.query.from)||0;
    log(`/sync-log requested from index ${fromIndex}`);
    const missing = state.log.slice(fromIndex).filter(e=>e.committed);
    res.json({entried:missing});
});
app.get("/status",(req,res)=>{
    res.json({
        id:REPLICA_ID,
        role:state.role,
        logLength:state.log.length,
        peers:PEERS,
    });
});
//Start
app.listen(PORT,()=>{
    log(`Replica started on port ${PORT}. Peers: ${PEERS.join(",")||"none"}`);
})