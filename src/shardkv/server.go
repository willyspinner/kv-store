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


const Debug = 0


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
    Operation  string // "Put", "Append", "Get", "Reconfiguration", or "Noop"
    Key        string
    Value      string
    ID         int64

    // for reconfiguration:
    Config     shardmaster.Config
    KeyVal map[string] string
    Requests map[int64] bool
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

    // paxos / networking
    myAddr string
    paxosSeqCount int

    // for dealing with configuration changes
    config shardmaster.Config 
    // keeps snapshots of keyval and requests for each config number.
    keyValHistory map[int] map[string]string
    requestsHistory map[int] map[int64] bool
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


/* Log our entire KeyVal and request for our current config number */
func (kv *ShardKV) logKVHistory() {
    kvHistory := make(map[string]string)
    for k,v := range kv.keyVal {
        if kv.isHandlingKey(k, &kv.config) {
            kvHistory[k] = v
        }
    }
    requestsHistory := make(map[int64] bool)
    for reqID, _ := range kv.requests {
        requestsHistory[reqID] = true
    }
    cfgNum := kv.config.Num
    kv.keyValHistory[cfgNum] = kvHistory
    kv.requestsHistory[cfgNum] = requestsHistory
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
		kv.keyVal[op.Key] = op.Value
		kv.requests[op.ID] = true
	} else if command == "Append"{
        res, keyExists := kv.keyVal[op.Key]
        if keyExists {
            kv.keyVal[op.Key] = res + op.Value
        } else {
            kv.keyVal[op.Key] = op.Value
        }
		kv.requests[op.ID] = true
	} else if command == "Reconfiguration" {
        if op.Config.Num < kv.config.Num {
            kv.DPrintf("CONFIGURING to an older cfg num: %d -> %d\n", kv.config.Num, op.Config.Num)
            log.Fatalf("ERROR\n")
        }
        kv.DPrintf("updateState() reconfiguration logging num %d \n", kv.config.Num)
        // Save a snapshot first. This will be used for RetrieveKV requests.
        kv.logKVHistory()

        // then update keyVal, requests and config.
        for k, v := range op.KeyVal {
            kv.keyVal[k] = v
        }
        for id, _ := range op.Requests {
            kv.requests[id] = true
        }
        kv.config = op.Config
    }
}

func isHandlingKey(key string, config *shardmaster.Config, fromGid int64) bool {
    shard := key2shard(key)
    if shard >= len(config.Shards) {
        return false
    }
    shardGid := config.Shards[shard]
    // NOTEDIFF: changed the below
    return (shardGid != 0) && shardGid == fromGid
}



func (kv *ShardKV) isHandlingKey(key string, config *shardmaster.Config) bool {
    return isHandlingKey(key, config, kv.gid)
}



func (kv *ShardKV) RetrieveKV(args *RetrieveKVArgs, reply *RetrieveKVReply) error {
    kv.mu.Lock()
    kv.DPrintf("RetrieveKV(): called with cfg num %v unlocked \n", args.NewConfigNum)
    defer kv.mu.Unlock()

    set_op := Op{Operation:"Noop", ID: nrand()}
    for kv.config.Num < args.NewConfigNum {
		var res Op
		seq_num := kv.paxosSeqCount

		status, val := kv.px.Status(seq_num)

		if status == paxos.Decided {
			res = val.(Op)
        } else {
			res = kv.Paxos(seq_num, set_op)
        }

        if res.Operation == "Reconfiguration" {
            kv.updateState(res)
        } else {
            if kv.isHandlingKey(res.Key, &kv.config) {
                kv.updateState(res)
            }
        }
        kv.DPrintf("RetriveKV() got res %v\n", res)

        // finish processing -> call Done from Paxos
        kv.px.Done(seq_num)
        kv.paxosSeqCount += 1

        if res.ID == set_op.ID {
            if kv.config.Num >= args.NewConfigNum {
                break
            } else {
                set_op =  Op{Operation:"Noop", ID: nrand()}
            }
        }
        // unlock so tick() can run if there's any config changes.
        kv.mu.Unlock()
        time.Sleep(50 * time.Millisecond)
        kv.mu.Lock()
    }

    oldKV, exists := kv.keyValHistory[args.NewConfigNum - 1]
    if ! exists {
        kv.DPrintf("ERROR: history %d doesn't exist.\n", args.NewConfigNum - 1)
        log.Fatalf("ERROR: history %d doesn't exist.\n", args.NewConfigNum - 1)
    }

    reply.KeyVal = oldKV
    reply.Requests = kv.requestsHistory[args.NewConfigNum - 1]
    return nil
}


// handles a new reconfiguration based on op. Assumes that we are inside a mutex Lock()
func (kv *ShardKV) getKVFromReconfig(previousConfig *shardmaster.Config, newConfig *shardmaster.Config) (map[string]string, map[int64] bool){
    newKeyVal := make(map[string] string)
    newRequests := make(map[int64] bool)

    // if we have no config before, then no need to retrieve from other shards.
    if previousConfig.Num == 0 {
        return newKeyVal, newRequests
    }

    // check for new shards we have to deal with.
    for newShard, newGid := range newConfig.Shards {
        // get the group that was previously responsible for this.
        previousGid := previousConfig.Shards[newShard]
        previousServers := previousConfig.Groups[previousGid]
        if newGid == kv.gid && kv.gid != previousGid {
            hasRetrieved := false
            for !hasRetrieved {
                for i, server := range previousServers {
                    kv.DPrintf("getKVFromReconfig(): retrieving %d -> %d from server GID %d me %d, address %v..\n", 
                                    previousConfig.Num, newConfig.Num, previousGid, i, path.Base(server))

                    args := RetrieveKVArgs{NewConfigNum: newConfig.Num}
                    reply := RetrieveKVReply{}
                    kv.mu.Unlock()

                    hasRetrieved = call(server, "ShardKV.RetrieveKV", args, &reply)

                    kv.mu.Lock()
                    if hasRetrieved {
                        for k, v := range reply.KeyVal{
                            if isHandlingKey(k, previousConfig, previousGid) {
                                newKeyVal[k] = v
                            } else {
                                kv.DPrintf("getKVFromReconfig(): key \"%s\", v %s of newKeyVal skipped since old gid was not handling it before. ", k, v)
                                continue
                            }
                        }
                        for id, _ := range reply.Requests {
                            newRequests[id] = true
                        }
                        break
                    }
                }
            }
        }
    }
    return newKeyVal, newRequests
}



func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.

    kv.mu.Lock()
    kv.DPrintf("GET %v, id: %v got past lock\n", args.Key, args.ID)
    defer kv.mu.Unlock()

    // first, check if we are responsible for this key.
    if ! kv.isHandlingKey(args.Key, &kv.config) {
        kv.DPrintf("GET not responsible for key %v\n", args.Key)
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
            kv.updateState(res)
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
                val, exists := kv.keyVal[set_op.Key]
                if exists {
                    reply.Value = val
                    reply.Err = OK
                    kv.DPrintf("GET %v id: %v, replying  value %v \n", args.Key, args.ID, val)
                } else {
                    reply.Value = ""
                    reply.Err = ErrNoKey
                    kv.DPrintf("GET %v id: %v, replying ErrNoKey \n", args.Key, args.ID)
                }
            }
			break
		}
	}

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
    kv.mu.Lock()
    kv.DPrintf("PutAppend() %v %v %v, id: %d past lock\n", args.Op, args.Key, args.Value, args.ID)
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
            kv.updateState(res)
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
    kv.mu.Lock()
    defer kv.mu.Unlock()
    upToDateConfig := kv.sm.Query(-1)
    for kv.config.Num < upToDateConfig.Num {
        nextCfgNum := kv.config.Num + 1
        nextConfig := kv.sm.Query(nextCfgNum)
        updateKeyVal, updateRequests := kv.getKVFromReconfig(&kv.config, &nextConfig)
        kv.DPrintf("tick() - reconfiguring. %d -> %d, nextConfig: %d\n", kv.config.Num, upToDateConfig.Num, nextConfig.Num)
        if nextCfgNum <= kv.config.Num {
            kv.DPrintf("tick(): continuing..\n")
            continue
        }

        set_op := Op {
            Operation:"Reconfiguration",
            ID:nrand(),
            Config: nextConfig,
            KeyVal: updateKeyVal,
            Requests:updateRequests,
        }
        for {
            var res Op
            seq_num := kv.paxosSeqCount

            kv.paxosSeqCount += 1

            // get status of current Paxos
            status, val := kv.px.Status(seq_num)

            // check if decided, if not then run Paxos again
            if status == paxos.Decided {
                kv.DPrintf("tick() -  decided on %v\n", val.(Op))
                res = val.(Op)
            } else {
                res = kv.Paxos(seq_num, set_op)
            }

            if res.Operation == "Reconfiguration" {
                kv.updateState(res)
            } else {
                if kv.isHandlingKey(res.Key, &kv.config) {
                    kv.updateState(res)
                }
            }

            // finish processing -> call Done from Paxos
            kv.px.Done(seq_num)
            // break if our config is up to date, either by us or others.
            if set_op.ID == res.ID || kv.config.Num == nextConfig.Num {
                break
            }
        }
        //nextCfgNum += 1
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
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

    kv.requests = make(map[int64] bool)
    kv.keyVal = make(map[string] string)
    kv.keyValHistory = make(map[int] map[string]string)
    kv.requestsHistory = make(map[int] map[int64]bool)

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
