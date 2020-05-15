package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"

import "hash/fnv"



// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)


const (
    N_NIL = -999
)


// app constants. This can be changed
const (
    livenessBackoffMillis = 75
    learnerPingMillis = 350 // every learnerPingMillis milliseconds, discover any newly-decided values (useful for when
    // proposer doesn't send decided RPC (e.g. it crashes)
)




type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
    log        map[int]*lSlot // maps sequence number to the corresponding lslot object
    currentMinSeq int // minimum sequence number that hasn't been forgotten
    currentMaxSeq int // max sequence number this paxos peer knows so far.
    peersDone  map[int] int // maps a peers entry to its done level 
    meHash     uint32
    meDone     int // our latest Done() value.

    nMajority  int // how many votes needed for a majority. This stays constant
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}



// ------------ acceptor functions ------------------

func (px *Paxos) Ph1AcceptRPCHandler (args *Ph1AcceptArgs, reply *Ph1AcceptReply) error {
    DPrintf("px me %d: ph1 accept rpc handler for seq %d. begin\n",px.me, args.Seq)
    if args.Me != px.me {
        px.mu.Lock()
        DPrintf("px me %d: ph1 accept rpc handler for seq %d. acquired lock\n",px.me, args.Seq)
        px.peersDone[args.Me] = args.MeDone
        if args.MeDone > px.currentMaxSeq {
            px.currentMaxSeq = args.MeDone
        }
        px.cleanupDones()
        defer px.mu.Unlock()
    }
    slot, exists := px.log[args.Seq]
    if ! exists {
        px.log[args.Seq] = &lSlot{
            n_a: N_NIL,
            n_p: N_NIL,
            fate: Pending,
        }
        slot = px.log[args.Seq]
    }
    if args.Seq > px.currentMaxSeq {
        px.currentMaxSeq = args.Seq
    }
    if args.N > slot.n_p  || slot.n_p == N_NIL {
        // commit to proposal
        reply.OK = true
        px.log[args.Seq].n_p = args.N
        reply.N = args.N
        if slot.n_a == N_NIL {
            reply.AlreadyAccepted = false
        } else {
            reply.AlreadyAccepted = true
            reply.V_a = slot.v_a
            reply.N_a = slot.n_a
        }
    } else {
        // reject proposal. 
        reply.OK = false
        reply.MeCommitted = slot.meCommitted
        reply.N_p = slot.n_p
    }
    reply.SeqMax = px.currentMaxSeq
    return nil
}

func (px *Paxos) Ph2AcceptRPCHandler (args *Ph2AcceptArgs, reply *Ph2AcceptReply) error {
    if args.Me != px.me {
        px.mu.Lock()
        px.peersDone[args.Me] = args.MeDone
        if args.MeDone > px.currentMaxSeq {
            px.currentMaxSeq = args.MeDone
        }
        defer px.mu.Unlock()
    }
    slot, exists := px.log[args.Seq]
    if ! exists{
        px.log[args.Seq] = &lSlot{
            n_a: N_NIL,
            n_p: N_NIL,
            fate: Pending,
        }
        slot = px.log[args.Seq]
    }
    if !exists || args.N >= slot.n_p {
        px.log[args.Seq].n_p = args.N
        px.log[args.Seq].n_a = args.N
        px.log[args.Seq].v_a = args.V
        reply.OK = true
    } else {
        reply.OK = false
        reply.N_p = slot.n_p
    }
    reply.SeqMax = px.currentMaxSeq
    return nil
}


// This method tells learners that this value has been decided for a particular seq.
func (px *Paxos) Ph3DecidedRPCHandler (args *Ph3DecidedArgs, reply *Ph3DecidedReply) error {
    if args.Me != px.me {
        //DPrintf("px me %d: PH3 RPC handler: spinning for lock pos 1.\n",px.me)
        px.mu.Lock()
        //DPrintf("px me %d: PH3 RPC handler: lock acquired pos 1.\n",px.me)
        px.peersDone[args.Me] = args.MeDone
        if args.MeDone > px.currentMaxSeq {
            px.currentMaxSeq = args.MeDone
        }
        px.cleanupDones() // we can clean up here to see if lowest MeDone is higher than before
        defer px.mu.Unlock()
    }
    _, exists := px.log[args.Seq]
    if ! exists{
        DPrintf("px me %d: PH# px.log[%d] DOES NOT EXIST. currentMinSeq: %d, maxseq: %d\n",px.me, args.Seq, px.currentMinSeq, px.currentMaxSeq)
        px.log[args.Seq] = &lSlot{
            n_a: N_NIL,
            n_p: N_NIL,
            v: args.V,
            fate: Decided,
        }
    } else {
        if px.log[args.Seq].fate == Decided {
            // ignore!
            DPrintf("px me %d: PH3 px.log[%d] ALREADY EXISTS AND DECIDED ALR: orig v : %v, decihandler v: %v. Skipping..\n",px.me, args.Seq, px.log[args.Seq].v, args.V )
            return nil
        }
        px.log[args.Seq].v = args.V
        px.log[args.Seq].fate = Decided
    }
    reply.SeqMax = px.currentMaxSeq
    return nil
}


// utility function to get the majority from PH1 (prepare propose) replies
// returns (true, value) if there is a majority, otherwise false.
func (px *Paxos) getMajority(replies []Ph1AcceptReply, hasReplied []bool) (bool, interface{}) {
    naCounts := make(map[ProposalNum]int)
    for _, reply := range replies {
        if reply.AlreadyAccepted {
            // use N_a to uniquely identify acceptances.
            naCounts[reply.N_a] += 1
        }
    }
    var highestN_a ProposalNum = 0
    highestCount := 0
    for N_a, count := range naCounts {
        if count > highestCount {
            highestCount = count
            highestN_a = N_a
        }
    }
    if highestCount < px.nMajority {
        return false, nil
    }

    // get the corresponding value
    var v interface{}
    for _, reply := range replies {
        if reply.AlreadyAccepted && reply.N_a == highestN_a {
            v = reply.V_a
            break
        }
    }
    return true, v

}

// utility function
func (px* Paxos) printSlot( seq int, exists bool, slot *lSlot) {
    status := "(incorrect status)"
    if slot == nil || !exists {
        DPrintf("px me %d: printSlot(): seq %d slot NIL / doesn't exist\n",
        px.me, seq)
        return
    } else if slot.fate == Pending {
        status = "PENDING"
    } else if slot.fate == Decided {
        status = "DECIDED"
    } else if slot.fate == Forgotten {
        status = "FORGOTTEN"
    }
    DPrintf("px me %d: printSlot(): seq %d slot is %s, my isDone: %v, contents: v: %v, v_a: %v, n_a: %v, n_p: %v, highestNpTry: %d, peersDone: %v\n",
    px.me, seq, status, px.meDone, slot.v,slot.v_a,slot.n_a,slot.n_p, slot.highestNpTry, px.peersDone)
    return
}

// ------------------- Learner function ---------------------


func max (a,b ProposalNum) ProposalNum {
    if a <= b {
        return b
    }
    return a
}

func minInt(a,b int) int {
    if a > b {
        return b
    }
    return a
}
// long-running function where the paxos peer acts as a learner
// and discovers functions.
// this should be run in a separate goroutine


func (px *Paxos) runLearner () {
    for {
        px.mu.Lock()
        DPrintf("px me %d: runLearner() is scanning from %d to px.currentMaxSeq %d (inclusive) (Note: px.currentMinSeq: %d)\n",
            px.me, minInt(px.meDone + 1, px.currentMinSeq), px.currentMaxSeq, px.currentMinSeq)

        currentMaxSeq := px.currentMaxSeq
        for seq := minInt(px.meDone + 1, px.currentMinSeq); seq <= currentMaxSeq; seq++ {
            slot, exists := px.log[seq]
            px.printSlot(seq, exists, slot)
            if ! exists || slot.fate == Pending {
                if !exists {
                    //create one
                    px.log[seq] = &lSlot{
                        n_a: N_NIL,
                        n_p: N_NIL,
                        fate: Pending,
                        highestNpTry: 0,
                    }
                }
                DPrintf("px me %d: runLearner() is pursuing slot seq %d \n",px.me, seq)
                // then ping acceptors for this seq instance
                var N uint32 = 1
                if px.log[seq].n_p != N_NIL || px.log[seq].highestNpTry > 0 {
                    N, _ = px.parseProposalNumber(max(px.log[seq].n_p, px.log[seq].highestNpTry))
                }
                propNum := px.getProposalNumber(N + 1)

                ph1Args := Ph1AcceptArgs{
                    Seq: seq,
                    N: propNum,
                    Me: px.me,
                    MeDone: px.meDone,
                }
                var ph1Replies []Ph1AcceptReply = make([]Ph1AcceptReply, len(px.peers))
                var hasReplied []bool = make([]bool, len(px.peers))
                var highestN_p ProposalNum= 0
                for i, peer := range px.peers {
                    DPrintf("px me %d: runLearner() calling RPC Ph1AcceptRPCHandler to me %d\n",px.me, i)
                    if i == px.me {
                        px.Ph1AcceptRPCHandler(&ph1Args, &ph1Replies[i])
                        hasReplied[i] = true
                    } else {
                        px.mu.Unlock()
                        ok := call(peer, "Paxos.Ph1AcceptRPCHandler", ph1Args, &ph1Replies[i])
                        px.mu.Lock()
                        if ok {
                            hasReplied[i] = true
                            if highestN_p < ph1Replies[i].N_p {
                                highestN_p = ph1Replies[i].N_p
                            }
                            if ph1Replies[i].SeqMax > px.currentMaxSeq {
                                px.currentMaxSeq = ph1Replies[i].SeqMax
                            }
                        }
                    }
                }
                _, exists := px.log[seq]
                // check
                if ! exists {
                    px.mu.Unlock()
                    return
                }
                px.log[seq].highestNpTry = max(highestN_p, px.log[seq].highestNpTry)
                hasMajority, v := px.getMajority(ph1Replies, hasReplied)
                if hasMajority {
                    // if a majority is found, then call Ph3DecidedRPCHandler
                    ph3DecidedArgs := Ph3DecidedArgs{
                        Seq: seq,
                        Me: px.me,
                        V: v,
                        MeDone: px.meDone,
                    }

                    var ph3DecidedReply  Ph3DecidedReply

                    DPrintf("px me %d: runLearner() got a majority for seq %d and calling own PH3 handler. args.V: %v\n",
                        px.me, seq, ph3DecidedArgs.V )
                    px.Ph3DecidedRPCHandler(&ph3DecidedArgs, &ph3DecidedReply)

                }
            }
            //px.mu.Unlock()
            currentMaxSeq = px.currentMaxSeq
            //px.mu.Lock()
        }
        px.mu.Unlock()
        time.Sleep(learnerPingMillis * time.Millisecond)
    }
}


// proposal numbers
// format: [___N___ ___I___]
// where N is a 32 bit proposal identifier number, and I is a 32 bit hash of 'me'
// this is to ensure uniqueness in proposal numbers

func (px *Paxos) getProposalNumber(n uint32) ProposalNum {
    I := px.meHash
    var propNum ProposalNum = ProposalNum(I)
    propNum |= ProposalNum(n) << 32
    return propNum
}

func (px *Paxos) printProposalNumber(raw ProposalNum) {

    N,I := px.parseProposalNumber(raw)
    fmt.Printf("px instance me %d : print proposal num N : %d, I: %d\n", px.me, N,I)
}

func (px *Paxos) parseProposalNumber(raw ProposalNum) (uint32, uint32){
    N := uint32(raw>>32)
    I := uint32(raw)
    return N, I
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//


func (px *Paxos) Start(seq int, v interface{}) {
    DPrintf("px me %d: started for seq %d\n",px.me, seq)
	// Your code here.
    px.mu.Lock()
    if seq < px.currentMinSeq {
        px.mu.Unlock()
        return
    }

    cInst, exists := px.log[seq]
    if exists && (cInst.fate == Decided || (cInst.fate == Pending && cInst.v != nil)) {
        px.mu.Unlock()
        return
    }

    if !exists {
        // we make a new one here
        px.log[seq] = &lSlot{
            n_a: N_NIL,
            n_p: N_NIL,
            fate: Pending,
        }
        if seq > px.currentMaxSeq {
            px.currentMaxSeq = seq
        }
    }
    // we spawn a proposer thread here (on demand) to propose to all paxos peers.
    // but this thread needs to live until something is decided for this seq (doesn't have to be our v).
    // it is the application's responsibility to keep on retrying for its operation to be done,
    // not paxos.
    px.mu.Unlock()
    go func() {
        var currentN uint32 = 1
        var propNum ProposalNum = px.getProposalNumber(currentN)
            
        for {
            px.mu.Lock()
            if px.currentMinSeq > seq || px.log[seq].fate == Decided {
                px.mu.Unlock()
                break
            }
            meDone := px.meDone
            px.mu.Unlock()

            // ------------------- PHASE 1 --------------------
            ph1Args := Ph1AcceptArgs{
                Seq: seq,
                N: propNum,
                Me: px.me,
                MeDone: meDone,
            }
            var ph1Replies []Ph1AcceptReply = make([]Ph1AcceptReply, len(px.peers))
            var hasReplied []bool = make([]bool, len(px.peers))
            for i, peer := range px.peers {
                hasReplied[i] = true
                if i == px.me {
                    px.mu.Lock()
                    px.Ph1AcceptRPCHandler(&ph1Args, &ph1Replies[i])
                    px.mu.Unlock()
                } else {
                    ok := call(peer, "Paxos.Ph1AcceptRPCHandler", ph1Args, &ph1Replies[i])
                    if !ok {
                        hasReplied[i] = false
                    } else {
                        px.mu.Lock()
                        if ph1Replies[i].SeqMax > px.currentMaxSeq {
                            px.currentMaxSeq = ph1Replies[i].SeqMax
                        }
                        px.mu.Unlock()
                    }
                }
            }

            // count how many OKs
            nPromises := 0
            var maxNp ProposalNum = 0
            for _, reply := range ph1Replies {
                if reply.OK {
                    nPromises += 1
                } else {
                    if reply.N_p > maxNp {
                        maxNp = reply.N_p
                    }
                }
            }
            DPrintf("px me %d: proposer thread for seq %d: after phase 1 rpc calls. Obtained %d nPromises, %d maj needed, total: %d\n",
            px.me, seq, nPromises,px.nMajority, len(px.peers))
            if nPromises < px.nMajority {
                DPrintf("px me %d: proposer thread for seq %d, v: %v: restarting since more votes needed.\n",px.me, seq, v)
                nProp, _ := px.parseProposalNumber(maxNp)
                currentN = nProp + 1
                propNum = px.getProposalNumber(currentN)
                // TODO: implement back off period based on order w.r.t. ID of proposer
                // to whom acceptor committed.
                // for now, we implement a fixed amount
                time.Sleep(livenessBackoffMillis  * time.Millisecond)
                continue
            }

            //  -------------- PHASE 2 -------------------
            // Once it has received promises from majority, send PH2 accepts.

            // first, figure out what V to use.
            var highestNa ProposalNum = N_NIL
            vUse := v
            for IDX, reply := range ph1Replies {
                if !reply.OK {
                    continue
                }
                if reply.AlreadyAccepted {
                    DPrintf("px me %d: proposer thread for seq %d: PH2 looking at already Accepted reply: %v\n", px.me, seq, &reply)
                    if highestNa == N_NIL && reply.N_a != N_NIL || highestNa <reply.N_a {
                        highestNa = reply.N_a
                        vUse = reply.V_a
                        DPrintf("px me %d: proposer thread for seq %d: PH2 got v %v highestNa: %d from me: %d\n",
                        px.me, seq, vUse, highestNa,IDX )
                    }
                }
            }

            px.mu.Lock()
            // then, send the accepts.
            ph2Args := Ph2AcceptArgs{
                Seq: seq,
                N: propNum,
                V: vUse,
                Me: px.me,
                MeDone: px.meDone,
            }
            px.mu.Unlock()

            var ph2Replies []Ph2AcceptReply = make([]Ph2AcceptReply, len(px.peers))

            // send PH2 accepts to everyone.
            nOKs := 0
            maxNp = 0
            // why is v also v'

            DPrintf("px me %d: proposer thread for seq %d, propnum %d: PH2 calling accept RPC for majority v': %v with highestNa: %d,  own v': %v\n",px.me, seq, propNum, vUse, highestNa, v)
            for i, peer := range px.peers{
                ok := true
                if i == px.me {
                    px.mu.Lock()
                    px.Ph2AcceptRPCHandler(&ph2Args, &ph2Replies[i])
                    px.mu.Unlock()
                } else {
                    ok = call(peer, "Paxos.Ph2AcceptRPCHandler", ph2Args, &ph2Replies[i])
                }
                if ok {
                    if ph2Replies[i].OK {
                        nOKs += 1
                    } else if maxNp < ph2Replies[i].N_p {
                        maxNp = ph2Replies[i].N_p
                    }
                    px.mu.Lock()
                    if ph2Replies[i].SeqMax > px.currentMaxSeq {
                        px.currentMaxSeq = ph2Replies[i].SeqMax
                    }
                    px.mu.Unlock()
                }
            }

            if nOKs >= px.nMajority  {
                // ----------------------- PHASE 3 - send DECIDED RPC ------------------------------
                DPrintf("px me %d: proposer thread for seq %d: entering phase 3..\n",px.me, seq)

                px.mu.Lock()
                ph3DecidedArgs := Ph3DecidedArgs {
                    Seq: seq,
                    Me: px.me,
                    V: vUse,
                    MeDone: px.meDone,
                }
                px.mu.Unlock()
                ph3DecidedReplies := make([]Ph3DecidedReply, len(px.peers))
                for i, peer := range px.peers{
                    DPrintf("px me %d: proposer ting calling Ph3 handler of me %d for seq %d. vUse: %v\n",px.me, i, seq, vUse)
                    if i == px.me {
                        px.mu.Lock()
                        px.Ph3DecidedRPCHandler(&ph3DecidedArgs, &ph3DecidedReplies[i])
                        px.mu.Unlock()
                    } else {
                        ok := call(peer, "Paxos.Ph3DecidedRPCHandler", ph3DecidedArgs, &ph3DecidedReplies[i])
                        if ok {
                            px.mu.Lock()
                            if ph3DecidedReplies[i].SeqMax > px.currentMaxSeq {
                                px.currentMaxSeq = ph3DecidedReplies[i].SeqMax
                            }
                            px.mu.Unlock()

                        }
                    }
                }
            } else {
                // otherwise, retry again!
                nProp, _ := px.parseProposalNumber(maxNp)
                currentN = nProp + 1
                propNum = px.getProposalNumber(currentN)
                // TODO: implement back off period based on order w.r.t. ID of proposer
                // to whom acceptor committed.
                // for now, we implement a fixed amount
                //DPrintf("px me %d: proposer thread for seq %d: lock Released pos 2..\n",px.me, seq)
                time.Sleep(livenessBackoffMillis  * time.Millisecond)
                continue
            }
        }
    }()
    return
}


// utility function for cleanup of everything before smallest done.
func (px *Paxos) cleanupDones() {
    minDone := -1
    for idx, _ := range px.peers {
        d, exists := px.peersDone[idx]
        if !exists {
            // if one of them doesn't even have a done value, then we 
            // need to wait for it
            return
        }
        if minDone == -1 || d < minDone {
            minDone = d
        }
    }
    for i := px.currentMinSeq; i <= minDone; i++ {
        delete(px.log, i)
    }
    if minDone + 1 > px.currentMinSeq {
        // advance our currentMinSeq, since we've deleted all before it.
        px.currentMinSeq = minDone + 1
    }
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
    px.mu.Lock()
    defer px.mu.Unlock()
    if seq > px.meDone {
        px.peersDone[px.me] = seq
        px.meDone = seq
    }
    px.cleanupDones()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
    px.mu.Lock()
    v := px.currentMaxSeq
    px.mu.Unlock()
	return v
}


//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
    px.mu.Lock()
    min := px.peersDone[0]
    for _, d := range px.peersDone {
        if d < min {
            min = d
        }
    }
    px.mu.Unlock()
    return min + 1
}




//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//

func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
    px.mu.Lock()
    defer px.mu.Unlock()
    if seq < px.currentMinSeq {
        return Forgotten, nil
    }
    logSlot , exists  := px.log[seq]
    if !exists {
        DPrintf("px me %d: status for seq %d, NIL, log: %v\n",px.me, seq, px.log)
        // Should return pending here as this is a 'future' request. 
        // (Piazza @219_f2)
        return Pending, nil
    }
    DPrintf("px me %d: status for seq %d, Decided: %v, log: %v\n",px.me, seq, logSlot.fate == Decided,px.log)
    return logSlot.fate, logSlot.v
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.
    px.meDone = -1
    px.peersDone = make(map[int]int)
    for i, _ := range peers {
        px.peersDone[i] = -1
    }
    px.currentMinSeq = 0
    px.currentMaxSeq = -1
    px.log = make(map[int]*lSlot)
    h := fnv.New32a()
    h.Write([]byte(peers[me]))
    px.meHash = h.Sum32()
    px.nMajority = (len(px.peers) / 2) + 1 // account for both odd and even
    go px.runLearner()

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
