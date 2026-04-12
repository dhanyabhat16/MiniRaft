const express=require("express");
const axios=require("axios");
const app=express();
app.use(express.json());

//Node identitiy
const REPLICA_ID=process.env.REPLICA_ID || "replica1";
const PORT=parseInt(process.env.PORT) || 3001;
const PEERS=(process.env.PEERS||"").split(",").filter(Boolean);
const GATEWAY_URL = process.env.GATEWAY_URL || "http://gateway:4000";

//state
let role="follower";
let term=0;
let votedFor=null;
let strokeLog=[];
let commitIndex=-1;

//timers
let electionTimer=null;
let heartbeatTimer=null;
const HEARTBEAT_INTERVAL=150;
const ELECTION_MIN=500;
const ELECTION_MAX=800;

//better logging
const rlog=(...args)=>console.log(`[${REPLICA_ID}][T${term}][${role.toUpperCase()}]`,...args);

//timer helpers
function randomTimeout() {
  return Math.floor(Math.random() * (ELECTION_MAX - ELECTION_MIN)) + ELECTION_MIN;
}
function resetElectionTimer() {
  if (electionTimer) clearTimeout(electionTimer);
  electionTimer = setTimeout(startElection, randomTimeout());
}
function stopElectionTimer(){
    if(electionTimer) clearTimeout(electionTimer);
    electionTimer=null;
}
function stopHeartbeatTimer(){
    if(heartbeatTimer) clearInterval(heartbeatTimer);
    heartbeatTimer=null;
}

//Roles
function becomeFollower(newTerm){
    rlog("-> FOLLOWER (new term "+newTerm+")");
    role="follower";
    term=newTerm;
    votedFor=null;
    stopHeartbeatTimer();
    stopElectionTimer();
}
function becomeLeader(){
    rlog("-> LEADER elected");
    role="leader";
    stopElectionTimer();
    sendHeartbeats();
    heartbeatTimer=setInterval(sendHeartbeats,HEARTBEAT_INTERVAL);
}

//election logic 
async function startElection(){
    role="candidate";
    term+=1;
    votedFor=REPLICA_ID;
    let votes=1;
    rlog("Starting election");
    await Promise.all(
        PEERS.map(async(peer)=>{
            try{
                const res=await axios.post(peer+"/request-vote",{term:term,candidateId:REPLICA_ID,},{timeout:300});
                //Higher term: step down
                if (res.data.term>term){
                    becomeFollower(res.data.term);
                    return;
                }
                if(res.data.voteGranted){
                    votes++;
                    rlog("Vote from",peer,"| total:",votes);
                    if(votes>=2 && role=="candidate"){
                        becomeLeader();
                    }
                }
            } catch(e){
                rlog("Vote requested failed to"+peer);
            }
        })
    );
    //split vote case
    if(role=="candidate"){
        rlog("Split vote - retrying");
        resetElectionTimer();
    }
}
//Heartbeats
async function sendHeartbeats(){
    PEERS.forEach(async (peer)=>{
        try{
            const res=await axios.post(
                peer+"/heartbeat",
                {
                    term:term,
                    leaderId:REPLICA_ID,
                },
                {timeout:300}
            );
            //step down if higher term seen
            if(res.data.term>term){
                rlog("Higher term detected -> stepping down");
                becomeFollower(res.data.term);
            }
        } catch(e){}
    });
}
//replication
async function replicateEntry(entry){
    const index=strokeLog.length;
    strokeLog.push({entry,term,committed:false});
    rlog("Appended entry at index "+index);
    let acks=1;
    await Promise.all(
        PEERS.map(async(peer)=>{
            try{
                const res=await axios.post(peer+"/append-entries",{
                    term:term,
                    leaderId:REPLICA_ID,
                    entry,
                    prevLogIndex:index-1,
                    entryIndex:index,
                },{timeout:500});
                if(res.data.term>term){
                    becomeFollower(res.data.term);
                    return;
                }
                if(res.data.success){
                    acks++;
                    rlog("ACK from",peer,"| total:",acks);
                } else if(res.data.logLength!==undefined){
                    await syncFollower(peer, res.data.logLength);
                }
            } catch(e){
                rlog("Replication failed to "+peer);
            }
        })
    );
    if(acks>=2){
        strokeLog[index].committed=true;
        commitIndex=index;
        rlog("Committed index "+index);
        try{
            await axios.post(GATEWAY_URL+"/broadcast",{stroke:entry});
        } catch(e){
            rlog("Gateway notify failed");
        }
    } else{
        rlog("Commit failed - insufficient ACKs: ",acks);
    }
}
async function syncFollower(peer, fromIndex) {
  const missing = strokeLog.slice(fromIndex).filter(e => e.committed);

  try {
    await axios.post(peer + "/sync-log", {
      entries: missing,
      fromIndex,
    });
  } catch (e) {
    rlog("Sync failed to " + peer);
  }
}

//RPC endpoints
app.post("/request-vote",(req,res)=>{
    const{term:candidateTerm,candidateId}=req.body;
    rlog(`/request-vote from ${candidateId} (term ${candidateTerm})`);
    if(candidateTerm>term){
        becomeFollower(candidateTerm);
    }
    const grantVote= candidateTerm>=term && (votedFor===null || votedFor==candidateId);
    if(grantVote){
        votedFor=candidateId;
        resetElectionTimer();
    } 
    rlog((grantVote ? "Granting" : "Denying") + " vote to " + candidateId);
    res.json({ voteGranted: grantVote, term });
});

app.post("/heartbeat",(req,res)=>{
    const{term:leaderTerm}=req.body;
    if(leaderTerm>term){
        becomeFollower(leaderTerm);
    } else if(leaderTerm===term){
        if(role!=="follower"){
            becomeFollower(leaderTerm);
        } else{
            resetElectionTimer(); //refresh
        }
    }
    res.json({success:true,term});
});

app.post("/append-entries",(req,res)=>{
    const { term: leaderTerm, entry, prevLogIndex, entryIndex } = req.body;
    if(leaderTerm<term){
        return res.json({success:false,term});
    }
    if(leaderTerm>term) becomeFollower(leaderTerm);
    resetElectionTimer();
    if(prevLogIndex>=0 && strokeLog.length<=prevLogIndex){
        return res.json({success:false,logLength:strokeLog.length});
    }
    if(entry){
        strokeLog[entryIndex]={entry,term:leaderTerm,committed:false};
    }
    res.json({success:true,logLength:strokeLog.length,term});
});

app.post("/sync-log",(req,res)=>{
    const{entries,fromIndex}=req.body;
    entries.forEach((item,i)=>{
        strokeLog[fromIndex+i]={
            entry:item.entry,
            term:item.term,
            committed:true,
        };
    });
    commitIndex=strokeLog.filter(e=>e && e.committed).length-1;
    res.json({ok:true});
});

app.get("/sync-log",(req,res)=>{
    const fromIndex=parseInt(req.query.from)||0;
    const missing = strokeLog.slice(fromIndex).filter(e=>e.committed);
    res.json({entries:missing});
});

app.post("/intake", async (req,res)=>{
    if (role!=="leader"){
        return res.status(400).json({error:"Not leader"});
    }
    await replicateEntry(req.body.stroke);
    res.json({ok:true,commitIndex});
});

app.get("/status",(req,res)=>{
    res.json({
        id:REPLICA_ID,
        role,
        term,
        logLength:strokeLog.length,
        commitIndex,
        peers:PEERS,
    });
});

//Start
app.listen(PORT,()=>{
    rlog("Started on port "+PORT);
    resetElectionTimer();
});