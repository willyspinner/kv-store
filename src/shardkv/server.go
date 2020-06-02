package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "path"


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
        a := append([]interface{}{path.Base(kv.myAddr), kv.gid, kv.me, kv.config.Num}, a...)
        log.Printf("[%v %d_%d CFN %d]: " + format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
    Operation  string // "Put", "Append", "Get", "Reconfiguration"
    Key        string
    Value      string
    ID         int64
    Config     shardmaster.Config // for reconfiguration
}


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
    requests map[int64] bool // to check if a request ID has been served before.
    keyVal map[string] string

    config shardmaster.Config // current sharding configuration
    myAddr string
    paxosSeqCount int
}


// run paxos protocol on a sequence number
func (kv *ShardKV) Paxos(seq int, op Op) Op{
	kv.px.Start(seq, op)
	to := 10 * time.Millisecond
	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

// update KV state based on operation Op, or no-op if already exists.
// does not assume anything about whether we're responsible
// for the shard or not.
func (kv* ShardKV) updateState(op Op) {
	kv.DPrintf("updateState() Updating on op %v\n", op)
	command := op.Operation
	_, exists := kv.requests[op.ID]
	if exists && (command == "Put" || command == "Append" ) {
        return
    }
	if command == "Get"{
        kv.requests[op.ID] = true
	} else if command == "Put"{
		// put -- set the result to keyVal
		kv.keyVal[op.Key] = op.Value
		kv.requests[op.ID] = true
	} else if command == "Append"{
		// append -- add the result to existing value
        res, keyExists := kv.keyVal[op.Key]
        if keyExists {
            kv.keyVal[op.Key] = res + op.Value
        } else {
            kv.keyVal[op.Key] = op.Value
        }
		kv.requests[op.ID] = true
	}
}

func isHandlingKey(key string, config *shardmaster.Config, fromGid int64) bool {
    shard := key2shard(key)
    shardGid := config.Shards[shard]
    //NOTE: when shardGid is 0, then we are initializing
    return shardGid == 0 || shardGid == fromGid
}

func (kv *ShardKV) isHandlingKey(key string, config *shardmaster.Config) bool {
    return isHandlingKey(key, config, kv.gid)
}

func (kv *ShardKV) RetrieveKV(args *RetrieveKVArgs, reply *RetrieveKVReply) error {
    //TODO: Make sure to avoid deadlock when two groups get assigned each others shards.
    //TODO: this is wrong. Need to wait until our config number is the same or above?
    kv.DPrintf("RetrieveKV(): called with cfg num %v \n", args.NewConfigNum)
    kv.mu.Lock()
    kv.DPrintf("RetrieveKV(): unlocked..\n")
    defer kv.mu.Unlock()
    /*
    reply.KeyVal = kv.keyVal
    reply.Requests = kv.requests
    return nil
    */

    for kv.config.Num < args.NewConfigNum {
		var res Op
		seq_num := kv.paxosSeqCount
		// get status of current Paxos
		status, val := kv.px.Status(seq_num)

		// check if decided, if not then run Paxos again
		if status == paxos.Decided {
			res = val.(Op)
            if res.Operation == "Reconfiguration" {
                kv.handleReconfiguration(res)
            } else {
                if kv.isHandlingKey(res.Key, &kv.config) {
                    kv.updateState(res)
                }
            }

            // finish processing -> call Done from Paxos
            kv.px.Done(seq_num)
            kv.paxosSeqCount += 1
        }
    }

    reply.KeyVal = kv.keyVal
    reply.Requests = kv.requests
    return nil
}

func (kv *ShardKV) importKV(reply *RetrieveKVReply, fromGid int64) {
    newKeyVal := reply.KeyVal
    newRequests := reply.Requests
    for k, v := range newKeyVal {
        if !isHandlingKey(k, &kv.config, fromGid) {
            kv.DPrintf("importKV(): key \"%s\", v %s of newKeyVal skipped since old gid was not handling it before. ", k, v)
            continue
        }

        if oldVal, exists:= kv.keyVal[k];  exists {
            kv.DPrintf("importKV(): key \"%s\" of newKeyVal already exists (%v), but replacing with %s", k, oldVal, v)
        } else {
            kv.DPrintf("importKV(): importing new key \"%s\", value %s", k, v)
        }
        kv.keyVal[k] = v
    }
    for id, _ := range newRequests {
        kv.requests[id] = true
    }
}

func (kv *ShardKV) handleReconfiguration(op Op) {
    newConfig := op.Config
    // if we have no config before, then no need to retrieve from other shards.
    if kv.config.Num == 0 {
        kv.config = newConfig
        return
    }

    // check for new shards we have to deal with.
    for newShard, newGid := range newConfig.Shards {
        // get the group that was previously responsible for this.
        previousGid := kv.config.Shards[newShard]
        previousServers := kv.config.Groups[previousGid]
        if newGid == kv.gid && kv.gid != previousGid {
            args := RetrieveKVArgs{NewConfigNum: newConfig.Num}
            reply := RetrieveKVReply{}
            hasRetrieved := false
            for !hasRetrieved {
                // retrieve from the old people.
                for i, server := range previousServers {
                    kv.DPrintf("handleReconfiguration(): retrieving from server GID %d me %d, address %v..\n", previousGid, i, path.Base(server))
                    ok := call(server, "ShardKV.RetrieveKV", args, &reply)
                    if ok {
                        hasRetrieved = true
                        kv.importKV(&reply, previousGid)
                        break
                    }
                }
            }
        }
    }
    kv.config = newConfig
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

    kv.DPrintf("GET: key: %v, id: %v\n", args.Key, args.ID)
    kv.mu.Lock()
    kv.DPrintf("GET: got past Lock() \n")
    defer kv.mu.Unlock()

    // first, check if we are responsible for this key.
    if ! kv.isHandlingKey(args.Key, &kv.config) {
        kv.DPrintf("GET: not responsible for key %v\n", args.Key)
        reply.Err = ErrWrongGroup
        return nil
    }

	for {
		var res Op
		seq_num := kv.paxosSeqCount
		set_op := Op{Key:args.Key, Value:"", Operation:"Get", ID:args.ID}

		kv.paxosSeqCount += 1

		// get status of current Paxos
		status, val := kv.px.Status(seq_num)

		// check if decided, if not then run Paxos again
		if status == paxos.Decided {
			res = val.(Op)
		} else {
			res = kv.Paxos(seq_num, set_op)
		}

        if res.Operation == "Reconfiguration" {
            kv.handleReconfiguration(res)
        } else {
            if kv.isHandlingKey(res.Key, &kv.config) {
                kv.updateState(res)
            }
        }

		// finish processing -> call Done from Paxos
		kv.px.Done(seq_num)

        //  reply to client when we get our request.
		if res.ID == args.ID {
            if ! kv.isHandlingKey(args.Key, &kv.config) {
                reply.Err = ErrWrongGroup
            }
            val, exists := kv.keyVal[set_op.Key]
            if exists {
                reply.Value = val
                reply.Err = OK
                kv.DPrintf("GET: key: %v id: %v, replying  value %v \n", args.Key, args.ID, val)
            } else {
                reply.Value = ""
                reply.Err = ErrNoKey
                kv.DPrintf("GET: key: %v id: %v, replying ErrNoKey \n", args.Key, args.ID)
            }
			break
		}
	}

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
    kv.DPrintf("PutAppend() %v %v %v, id: %d\n", args.Op, args.Key, args.Value, args.ID)
    kv.mu.Lock()
    kv.DPrintf("PutAppend(): got past Lock() \n")
    defer kv.mu.Unlock()

    // first, check if we are responsible for this key.
    if ! kv.isHandlingKey(args.Key, &kv.config) {
        reply.Err = ErrWrongGroup
        kv.DPrintf("PutAppend(): wrong group %v \n", args)
        return nil
    }

    // then, check if request has been served before.
    if _, exists := kv.requests[args.ID]; exists {
        reply.Err = OK
        return nil
    }

    // finally, run paxos for this operation.
	for {
		var res Op
		seq_num := kv.paxosSeqCount
		set_op := Op{Key:args.Key, Value:args.Value, Operation:args.Op, ID:args.ID}

		kv.paxosSeqCount += 1

		// get status of current Paxos
		status, val := kv.px.Status(seq_num)

		// check if decided, if not then run Paxos again
		if status == paxos.Decided {
			res = val.(Op)
		} else {
			res = kv.Paxos(seq_num, set_op)
		}

        if res.Operation == "Reconfiguration" {
            kv.handleReconfiguration(res)
        } else {
            if kv.isHandlingKey(res.Key, &kv.config) {
                kv.updateState(res)
            }
        }

		// finish processing -> call Done from Paxos
		kv.px.Done(seq_num)

        //  reply to client when we get our request.
		if res.ID == args.ID {
            if ! kv.isHandlingKey(args.Key, &kv.config) {
                reply.Err = ErrWrongGroup
            } else {
                reply.Err =OK
            }
			break
		}
	}

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
    // TODO: have a feeling that we need to keep watch of the paxos log here.
    kv.mu.Lock()
    defer kv.mu.Unlock()
    upToDateConfig := kv.sm.Query(-1)
    if upToDateConfig.Num > kv.config.Num {
        kv.DPrintf("tick() - reconfiguring. %d -> %d", kv.config.Num, upToDateConfig.Num)
        ID := nrand()
        for {
            var res Op
            seq_num := kv.paxosSeqCount
            set_op := Op{Operation:"Reconfiguration", ID:ID, Config: upToDateConfig}

            // add seq count by 1
            kv.paxosSeqCount += 1

            // get status of current Paxos
            status, val := kv.px.Status(seq_num)

            // check if decided, if not then run Paxos again
            if status == paxos.Decided {
                kv.DPrintf("tick() - reconfiguration:. decided on %v\n", val.(Op))
                res = val.(Op)
            } else {
                res = kv.Paxos(seq_num, set_op)
            }

            isReconfigured := false
            if res.Operation == "Reconfiguration" && res.Config.Num >= upToDateConfig.Num {
                kv.DPrintf("tick() - handlingReconfiguration for new config %d... \n", res.Config.Num)
                kv.handleReconfiguration(res)
                isReconfigured = true
            } else {
                if kv.isHandlingKey(res.Key, &kv.config) {
                    kv.updateState(res)
                }
            }

            // finish processing -> call Done from Paxos
            kv.px.Done(seq_num)
            if isReconfigured {
                break
            }
        }

    }
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

    kv.requests = make(map[int64] bool)
    kv.keyVal = make(map[string] string)

    kv.config = shardmaster.Config{Num: 0}

    kv.DPrintf("starting server with init config %v..\n", kv.config)

    kv.paxosSeqCount = 0
    kv.myAddr = servers[me]


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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
