package paxos

import "os"
import "log"

func DPrintf(format string, a ...interface{}) (n int, err error) {
    isDebug := os.Getenv("LOGLEVEL") == "DEBUG"
	if isDebug {
		log.Printf(format, a...)
	}
	return
}

type ProposalNum int64

// struct to keep information about a slot in the log
type lSlot struct {
    v           interface{} // actual value (if decided)
    v_a         interface{} // accepted value of slot
    n_a         ProposalNum // accepted number
    n_p         ProposalNum // highest proposed number
    fate        Fate
    meCommitted int // ID of proposer to whom acceptor has committed
    highestNpTry ProposalNum // this is for the learner to keep track of which N it should try next.
}

type Ph1AcceptArgs struct {
    Seq    int
    N      ProposalNum
    Me     int
    MeDone int // piggybacking proposer's Done Value
}

type Ph1AcceptReply struct {
    OK              bool // true if it has received a promise
    N               ProposalNum
    AlreadyAccepted bool
    V_a             interface{}
    N_a             ProposalNum
    MeCommitted     int // Id of proposer to whom acceptor has committed. This is to aid liveness back off
    N_p             ProposalNum // if proposal fails, proposer uses this to change its propnum.
}


type Ph2AcceptArgs struct {
    Seq         int
    N           ProposalNum
    V           interface{}
    Me          int
    MeDone      int // piggybacking Done value 
}

type Ph2AcceptReply struct {
    OK          bool
    N_p         ProposalNum // if proposal fails, proposer uses this to change propnum
}

type Ph3DecidedArgs struct {
    Seq         int
    Me          int
    V           interface{}
    MeDone      int // piggybacking done value
}

type Ph3DecidedReply struct {
    OK          bool
}
