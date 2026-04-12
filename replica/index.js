const express=require("express");
const axios=require("axios");
const app=express();
app.use(express.json());

//Node identitiy
const REPLICA_ID=process.env.REPLICA_ID || "replica1";
const PORT=parseInt(process.env.PORT) || 3001;
const PEERS=(process.env.PEERS||"").split(",").filter(Boolean);

//state
let role="follower";
let term=0;
let votedFor=null;
let log=[];
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
    const requests=PEERS.map(async(peer)=>{
        try{
            const res=await axios.post(`${peer}/request-vote`,{term:term,candidateId:REPLICA_ID,},{timeout:300});
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
        } catch(err){
            rlog("Vote requested failed to",peer);
        }
    });
    await Promise.all(requests);
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
                `${peer}/heartbeat`,
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
        } catch(err){}
    });
}

//RPC endpoints
app.post("/request-vote",(req,res)=>{
    const{term:candidateTerm,candidateId}=req.body;
    rlog(`/request-vote from ${candidateId} (term ${candidateTerm})`);
    if(candidateTerm>term){
        becomeFollower(candidateTerm);
    }
    const grantVote=candidateTerm>=term && (votedFor===null || votedFor==candidateId);
    if(grantVote){
        votedFor=candidateId;
        rlog("Granting vote to",candidateId);
        resetElectionTimer();
    } else{
        rlog("Denying vote to",candidateId);
    }
    res.json({voteGranted:grantVote, term:term});
});
app.post("/heartbeat",(req,res)=>{
    const{term:leaderTerm,leaderId}=req.body;
    if(leaderTerm>term){
        becomeFollower(leaderTerm);
    } else if(leaderTerm===term){
        if(role!=="follower"){
            becomeFollower(leaderTerm);
        } else{
            resetElectionTimer(); //refresh
        }
    }
    res.json({success:true,term:term});
});
//stub
app.post("/append-entries",(req,res)=>{
    rlog(`/append-entries from ${req.body.leaderId}`);
    res.json({success:true,logLength:log.length});
});
app.get("/sync-log",(req,res)=>{
    const fromIndex=parseInt(req.query.from)||0;
    rlog(`/sync-log requested from index ${fromIndex}`);
    const missing = log.slice(fromIndex).filter((e)=>e.committed);
    res.json({entries:missing});
});
app.get("/status",(req,res)=>{
    res.json({
        id:REPLICA_ID,
        role:role,
        term:term,
        logLength:log.length,
        peers:PEERS,
    });
});

//Start
app.listen(PORT,()=>{
    rlog(`Started on port ${PORT}`);
    resetElectionTimer();
});