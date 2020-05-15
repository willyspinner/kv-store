package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"


const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Key					string
	Value				string
	Operation		string
	Curr_ID			int64
	Prev_ID			int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	// Your definitions here.
	keyVal			map[string]string			// key value pairs
	requests		map[int64]string
	seqCount		int
}

// Run paxos -- modified from code given in assignment spec
func Paxos(kv *KVPaxos, seq int, op Op) Op{
	kv.px.Start(seq, op)
	to := 10 * time.Millisecond
	for{
		status, val := kv.px.Status(seq)
		if status == paxos.Decided{
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second{
			to *= 2
		}
	}
}

func Update(kv *KVPaxos, op Op){
	log.Printf("KVPaxos: Updating...")
	res, ok := kv.keyVal[op.Key]
	command := op.Operation
	if command == "Get"{
		log.Printf("KVPaxos: Updating: GET")
		if ok{
			kv.requests[op.Curr_ID] = res
		} else{
			kv.requests[op.Curr_ID] = ErrNoKey
		}
	} else if command == "Put"{
		log.Printf("KVPaxos: Updating: PUT")
		// put -- set the result to keyVal
		kv.keyVal[op.Key] = op.Value
		kv.requests[op.Curr_ID] = OK
	} else if command == "Append"{
		log.Printf("KVPaxos: Updating: APPEND")
		// append -- add the result to existing value
		kv.keyVal[op.Key] = res + op.Value
		kv.requests[op.Curr_ID] = OK
	}
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	log.Printf("KVPaxos: GET")
	// Your code here.
	kv.mu.Lock()

	// check for duplicate Get req
	prevId, ok := kv.requests[args.Curr_ID]
	if ok && prevId == args.Key{
		reply.Value = kv.keyVal[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return nil
	}

	// if not duplicate, assign the next Paxos
	for {
		var res Op
		seq_num := kv.seqCount
		set_op := Op{Key:args.Key, Value:"", Operation:"Get", Curr_ID:args.Curr_ID, Prev_ID:args.Prev_ID}

		// add seq count by 1
		kv.seqCount += 1

		// get status of current Paxos
		status, val := kv.px.Status(seq_num)

		// check if decided, if not then run Paxos again
		log.Printf("KVPaxos: Checking STATUS")
		if status == paxos.Decided{
			res = val.(Op)
		} else{
			res = Paxos(kv, seq_num, set_op)
			log.Printf("KVPaxos: GET: Done Running Paxos")
		}

		// clean client requests that are done
		if args.Prev_ID != -1 {
			_, ok := kv.requests[args.Prev_ID]
			if ok{
				delete(kv.requests, args.Prev_ID)
			}
		}
		// update keyVal/ requests based on Op -> call Update function
		Update(kv, res)
		log.Printf("KVPaxos: Updating in GET")

		// finish processing -> call Done from Paxos
		kv.px.Done(seq_num)
		log.Printf("KVPaxos: Called Done from Paxos")

		// update results from the Get call
		if res.Curr_ID == args.Curr_ID{
			req := kv.requests[args.Curr_ID]
			if req != ErrNoKey{
				reply.Value = kv.requests[args.Curr_ID]
				reply.Err = OK
			} else{		// send no key error
				reply.Value = ""
				reply.Err = ErrNoKey
			}
			break
		}
	}
	kv.mu.Unlock()
	log.Printf("KVPaxos: GET Done")
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	log.Printf("KVPaxos: PUTAPPEND")
	kv.mu.Lock()

	// check for duplicates
	_, ok := kv.requests[args.Curr_ID]
	if ok{
		reply.Err = OK
		kv.mu.Unlock()
		return nil
	}

	// assign to next Paxos
	for{
		var res Op
		seq_num := kv.seqCount
		set_op := Op{Key:args.Key, Value:args.Value, Operation:args.Op, Curr_ID:args.Curr_ID, Prev_ID:args.Prev_ID}

		// add seq number by 1
		kv.seqCount += 1

		// check status of the Paxos
		log.Printf("KVPaxos: PUTAPPEND getting STATUS")
		status, val := kv.px.Status(seq_num)

		// check if decided, if not then run Paxos again
		if status == paxos.Decided{
			res = val.(Op)
		} else{
			res = Paxos(kv, seq_num, set_op)
			log.Printf("KVPaxos: PUTAPPEND: Done Running Paxos")
		}

		// clean client requests that are done
		if args.Prev_ID != -1 {
			_, ok := kv.requests[args.Prev_ID]
			if ok{
				delete(kv.requests, args.Prev_ID)
			}
		}

		// update keyVal/requests based on Op
		Update(kv, res)
		log.Printf("KVPaxos: Done Updating PUTAPPEND")

		// finished -> call Paxos done
		kv.px.Done(seq_num)
		log.Printf("KVPaxos: PUTAPPEND: Called Done from Paxos")

		if res.Curr_ID == args.Curr_ID{
			break
		}
	}
	reply.Err = OK
	kv.mu.Unlock()
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.keyVal = make(map[string]string)
	kv.requests = make(map[int64]string)
	kv.seqCount = 0

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
