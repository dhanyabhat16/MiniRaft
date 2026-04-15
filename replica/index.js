const express=require("express");
const axios=require("axios");
const app=express();
app.use(express.json());
app.use((req, res, next) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    if (req.method === "OPTIONS") return res.sendStatus(204);
    next();
});

//Node identitiy
const REPLICA_ID=process.env.REPLICA_ID || "replica1";
const PORT=parseInt(process.env.PORT) || 3001;
const PEERS=(process.env.PEERS||"").split(",").filter(Boolean);
const GATEWAY_URL = process.env.GATEWAY_URL || "http://gateway:4000";
const SELF_URL = "http://" + REPLICA_ID + ":" + PORT;

//state
let role="follower";
let term=0;
let votedFor=null;
let strokeLog=[];
let commitIndex=-1;
let replicationQueue=Promise.resolve();
const pendingCommitEntries=[];
let pendingRetryRunning=false;
let electionInProgress=false;
let lastLeaderContactAt=Date.now();
let catchUpInFlight=false;

//timers
let electionTimer=null;
let heartbeatTimer=null;
const HEARTBEAT_INTERVAL=150;
const ELECTION_MIN=500;
const ELECTION_MAX=800;
const STARTUP_ELECTION_GRACE_MS=2500;
const PRE_ELECTION_PROBE_TIMEOUT_MS=250;
const APPEND_TIMEOUT_MS=1400;
const PEER_RETRY_COOLDOWN_MS=350;
const PEER_BACKOFF_FAIL_STREAK=3;
const peerBackoffUntil=new Map();
const peerFailureStreak=new Map();
const processStartedAt=Date.now();

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

function markLeaderContact() {
    lastLeaderContactAt = Date.now();
}

async function discoverExistingLeaderFromPeers(){
    const results=await Promise.allSettled(
        PEERS.map(peer=>
            axios.get(peer+"/status",{timeout:PRE_ELECTION_PROBE_TIMEOUT_MS})
                .then(res=>({peer,data:res.data}))
                .catch(()=>null)
        )
    );

    let found=null;
    for(const r of results){
        if(r.status==="fulfilled" && r.value && r.value.data && r.value.data.role==="leader"){
            const candidate={
                peer:r.value.peer,
                term:typeof r.value.data.term==="number" ? r.value.data.term : term,
            };
            if(!found || candidate.term>found.term){
                found=candidate;
            }
        }
    }
    if(!found) return false;

    if(found.term>term){
        becomeFollower(found.term);
    } else if(role!=="follower"){
        role="follower";
        votedFor=null;
        stopHeartbeatTimer();
    }
    markLeaderContact();
    resetElectionTimer();
    return true;
}

function schedulePendingRetry(){
    if(role!=="leader" || !pendingCommitEntries.length) return;
    setTimeout(()=>{
        retryPendingCommits();
    },300);
}

async function retryPendingCommits(){
    if(pendingRetryRunning || role!=="leader") return;
    pendingRetryRunning=true;
    try{
        while(role==="leader" && pendingCommitEntries.length){
            const nextEntry=pendingCommitEntries[0];
            const committed=await replicateEntry(nextEntry);
            if(committed){
                pendingCommitEntries.shift();
            } else{
                // Quorum still unavailable; keep buffered order and try later.
                break;
            }
        }
    } finally{
        pendingRetryRunning=false;
        if(role==="leader" && pendingCommitEntries.length){
            schedulePendingRetry();
        }
    }
}

//Roles
function becomeFollower(newTerm){
    rlog("-> FOLLOWER (new term "+newTerm+")");
    role="follower";
    term=newTerm;
    votedFor=null;
    stopHeartbeatTimer();
    resetElectionTimer(); 
}
function becomeLeader(){
    rlog("-> LEADER elected");
    role="leader";
    stopElectionTimer();
    sendHeartbeats();
    heartbeatTimer=setInterval(sendHeartbeats,HEARTBEAT_INTERVAL);
    schedulePendingRetry();
}

//election logic 
async function startElection(){
    if (role === "leader" || electionInProgress) {
        return;
    }
    const inStartupGrace=(Date.now()-processStartedAt)<STARTUP_ELECTION_GRACE_MS;
    if(inStartupGrace){
        const foundLeader=await discoverExistingLeaderFromPeers();
        if(foundLeader){
            return;
        }
        // Preserve stable leader during restart churn.
        resetElectionTimer();
        return;
    }
    // Even after grace, probe once before campaigning.
    const foundLeader=await discoverExistingLeaderFromPeers();
    if(foundLeader){
        return;
    }
    electionInProgress = true;
    role="candidate";
    term+=1;
    votedFor=REPLICA_ID;
    let votes=1;
    rlog("Starting election");
    try {
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
    } finally {
        electionInProgress = false;
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
                    leaderUrl:SELF_URL,
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
    if(role!=="leader"){
        return false;
    }
    // Trim any stale uncommitted tail from earlier quorum-loss windows.
    // Otherwise followers that rejoin later can reject new appends forever
    // due to prevLogIndex mismatch against these never-committed entries.
    while(strokeLog.length>0 && !strokeLog[strokeLog.length-1].committed){
        strokeLog.pop();
    }
    const index=strokeLog.length;
    strokeLog.push({entry,term,committed:false});
    rlog("Appended entry at index "+index+", replicating...");
    let acks=1;
    let quorumSatisfied=false;
    const quorumReached = await new Promise((resolve)=>{
        let resolved=false;
        let pending=PEERS.length;
        const done=(ok)=>{
            if(!resolved){
                resolved=true;
                if(ok){
                    quorumSatisfied=true;
                }
                resolve(ok);
            }
        };
        const markPeerFinished=()=>{
            pending--;
            if(!resolved && pending===0){
                done(acks>=2);
            }
        };

        PEERS.forEach(async(peer)=>{
            const backoffUntil=peerBackoffUntil.get(peer)||0;
            if(backoffUntil>Date.now()){
                markPeerFinished();
                return;
            }
            const appendPayload={
                term,
                leaderId:REPLICA_ID,
                entry,
                prevLogIndex:index-1,
                entryIndex:index,
                leaderCommitIndex: commitIndex,
            };
            const sendAppendWithRetry=async()=>{
                let lastErr=null;
                for(let attempt=1;attempt<=2;attempt++){
                    try{
                        return await axios.post(peer+"/append-entries",appendPayload,{timeout:APPEND_TIMEOUT_MS});
                    } catch(err){
                        lastErr=err;
                        if(attempt<2){
                            await new Promise(r=>setTimeout(r,120));
                        }
                    }
                }
                throw lastErr;
            };
            try{
                const res=await sendAppendWithRetry();
                if(res.data.term>term){
                    becomeFollower(res.data.term);
                    done(false);
                    return;
                }
                if(res.data.success){
                    peerFailureStreak.set(peer,0);
                    peerBackoffUntil.delete(peer);
                    acks++;
                    if(!quorumSatisfied){
                        rlog("ACK from",peer,"| total:",acks);
                    }
                    if(acks>=2){
                        done(true);
                    }
                } else if(res.data.logLength!==undefined){
                    if(quorumSatisfied){
                        return;
                    }
                    await syncFollower(peer, res.data.logLength);
                    const retry=await sendAppendWithRetry();
                    if(retry.data && retry.data.success){
                        peerFailureStreak.set(peer,0);
                        peerBackoffUntil.delete(peer);
                        acks++;
                        if(!quorumSatisfied){
                            rlog("ACK after sync from",peer,"| total:",acks);
                        }
                        if(acks>=2){
                            done(true);
                        }
                    }
                }
            } catch(e){
                const streak=(peerFailureStreak.get(peer)||0)+1;
                peerFailureStreak.set(peer,streak);
                if(streak>=PEER_BACKOFF_FAIL_STREAK){
                    peerBackoffUntil.set(peer,Date.now()+PEER_RETRY_COOLDOWN_MS);
                    peerFailureStreak.set(peer,0);
                }
                if(!quorumSatisfied){
                    rlog("Replication failed to "+peer);
                }
            } finally{
                markPeerFinished();
            }
        });
    });
    if(quorumReached){
        strokeLog[index].committed=true;
        commitIndex=index;
        rlog("Committed index "+index);
        axios.post(
            GATEWAY_URL+"/broadcast",
            {stroke:entry,index},
            {timeout:1000}
        ).catch(()=>{
            rlog("Gateway notify failed");
        });
        return true;
    } else{
        // Roll back the failed tail append so leader log only keeps committed
        // prefix; this keeps rejoining follower sync straightforward.
        if(strokeLog.length-1===index && strokeLog[index] && !strokeLog[index].committed){
            strokeLog.pop();
        }
        rlog("Commit failed - insufficient ACKs: ",acks);
        return false;
    }
}

function enqueueReplication(entry){
    replicationQueue = replicationQueue
        .then(async()=>{
            if(role!=="leader") return false;
            const committed=await replicateEntry(entry);
            if(!committed){
                pendingCommitEntries.push(entry);
                schedulePendingRetry();
            }
            return committed;
        })
        .catch((e)=>{
            rlog("Replication queue error:", e && e.message ? e.message : e);
            return false;
        });
    return replicationQueue;
}
async function syncFollower(peer, fromIndex) {
  const missing=[];
  for(let i=fromIndex;i<strokeLog.length;i++){
    const item=strokeLog[i];
    if(item && item.committed){
      missing.push({
        index:i,
        entry:item.entry,
        term:item.term,
      });
    }
  }

  try {
    await axios.post(peer + "/sync-log", {
      entries: missing,
      fromIndex,
    });
  } catch (e) {
    rlog("Sync failed to " + peer);
  }
}

async function catchUpFromLeader(leaderUrl) {
    if (!leaderUrl || catchUpInFlight) return;
    catchUpInFlight=true;
    try {
        // Recover from the last committed point, not raw log length.
        // On restarts/failovers, length may include stale uncommitted tail.
        const fromIndex = Math.max(0, commitIndex + 1);
        let res=null;
        let lastErr=null;
        for(let attempt=1;attempt<=2;attempt++){
            try{
                res = await axios.get(leaderUrl + "/sync-log", {
                    params: { from: fromIndex },
                    timeout: 1200,
                });
                break;
            } catch(err){
                lastErr=err;
                if(attempt<2){
                    await new Promise(r=>setTimeout(r,150));
                }
            }
        }
        if(!res){
            throw lastErr || new Error("sync-log unavailable");
        }
        const entries = (res.data && res.data.entries) || [];
        if (!entries.length) return;

        entries.forEach((item, i) => {
            const targetIndex=(typeof item.index==="number") ? item.index : (fromIndex + i);
            strokeLog[targetIndex] = {
                entry: item.entry,
                term: item.term,
                committed: true,
            };
        });
        for(let i=strokeLog.length-1;i>=0;i--){
            if(strokeLog[i] && strokeLog[i].committed){
                commitIndex=Math.max(commitIndex,i);
                break;
            }
        }
        rlog("Caught up", entries.length, "entries from leader");
    } catch (e) {
        rlog("Catch-up failed from leader");
    } finally{
        catchUpInFlight=false;
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
    const{term:leaderTerm, leaderUrl}=req.body;
    markLeaderContact();
    if(leaderTerm>term){
        becomeFollower(leaderTerm);
    } else if(leaderTerm===term){
        if(role!=="follower"){
            becomeFollower(leaderTerm);
        } else{
            resetElectionTimer(); //refresh
        }
    }
    if (leaderTerm >= term) {
        catchUpFromLeader(leaderUrl);
    }
    res.json({success:true,term});
});

app.post("/append-entries",(req,res)=>{
    const { term: leaderTerm, entry, prevLogIndex, entryIndex, leaderCommitIndex } = req.body;
    if(leaderTerm < term){
        return res.json({success:false, term});
    }
    if(leaderTerm > term) becomeFollower(leaderTerm);
    else resetElectionTimer();

    if(prevLogIndex >= 0 && strokeLog.length <= prevLogIndex){
        return res.json({success:false, logLength:strokeLog.length});
    }
    if(entry !== undefined && entryIndex !== undefined){
        strokeLog[entryIndex] = {entry, term:leaderTerm, committed:false};
    }
    if(leaderCommitIndex !== undefined && leaderCommitIndex >= 0){
        for(let i = 0; i <= leaderCommitIndex && i < strokeLog.length; i++){
            if(strokeLog[i]) strokeLog[i].committed = true;
        }
        commitIndex = Math.min(leaderCommitIndex, strokeLog.length - 1);
    }
    res.json({success:true, logLength:strokeLog.length, term});
});

app.post("/sync-log",(req,res)=>{
    const{entries,fromIndex}=req.body;
    entries.forEach((item,i)=>{
        const targetIndex=(typeof item.index==="number") ? item.index : (fromIndex+i);
        strokeLog[targetIndex]={
            entry:item.entry,
            term:item.term,
            committed:true,
        };
    });
    for(let i=strokeLog.length-1;i>=0;i--){
        if(strokeLog[i] && strokeLog[i].committed){
            commitIndex=i;
            break;
        }
    }
    res.json({ok:true});
});

app.get("/sync-log",(req,res)=>{
    const fromIndex=parseInt(req.query.from)||0;
    const missing=[];
    for(let i=fromIndex;i<strokeLog.length;i++){
        const item=strokeLog[i];
        if(item && item.committed){
            missing.push({
                index:i,
                entry:item.entry,
                term:item.term,
            });
        }
    }
    res.json({entries:missing});
});

app.post("/intake", async (req,res)=>{
    if (role!=="leader"){
        return res.status(400).json({error:"Not leader"});
    }
    enqueueReplication(req.body.stroke);
    res.json({ok:true,accepted:true,queued:true,commitIndex});
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