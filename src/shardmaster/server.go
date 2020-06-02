package shardmaster

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

// from KVPaxos
import cr "crypto/rand"
import "math/big"

const MAX = int(^uint(0) >> 1)
const MIN = -MAX - 1

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs		[]Config // indexed by config num
	seqCount	int		// current sequence number- similar to kvpaxos
}


type Op struct {
	// Your data here.
	Operation			string		// Join, Leave, Move, Query
	Curr_ID				int64			// current ID
	Server				[]string	// array of server ports -> Join
	Group_ID			int64			// group ID -> Join, Leave, Move, Query
	NShard				int				// shard number -> Move
	NConfig				int				// configuration number -> Query
}


// The following 2 functions are taken from kvpaxos
// Run paxos
func (sm *ShardMaster) Paxos(seq int, op Op) Op{
	sm.px.Start(seq, op)
	to := 10 * time.Millisecond
	for{
		status, val := sm.px.Status(seq)
		if status == paxos.Decided{
			return val.(Op)
		}
		time.Sleep(to)
		if to < 10 * time.Second{
			to *= 2
		}
	}
}

// Random number generator to generate unique ID
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := cr.Int(cr.Reader, max)
	x := bigx.Int64()
	return x
}


// Create a new config by copying from current config
func (sm *ShardMaster) CreateNewConfig() *Config{
	// get current config (the last config in config list)
	sz := len(sm.configs)
	curr_config := &sm.configs[sz - 1]

	// create a new config
	new_config := Config{Num: curr_config.Num + 1}
	new_config.Groups = make(map[int64][]string)

	// copy the shards from curr_config
	for s, gid := range curr_config.Shards{
		new_config.Shards[s] = gid
	}

	// copy the group from curr_config
	for gid, server := range curr_config.Groups{
		new_config.Groups[gid] = server
	}
	return &new_config
}

func (sm *ShardMaster) CountReplicaShards(config *Config) map[int64]int{
	rep_count := make(map[int64]int)

	// initialize
	for gid, _ := range config.Groups{
		rep_count[gid] = 0
	}

	// count for every gid in Shards
	for _, gid := range config.Shards{
		_, exists := config.Groups[gid]
		if exists{
			rep_count[gid] += 1
		}
	}
	return rep_count
}

// Balancing the loads in shards after new assignment
func (sm *ShardMaster) BalanceShards(gid int64, config *Config){
	// assign min number of shards to shards not serving requests
	for shard, group := range config.Shards{
		if group == 0 || group == gid{
			count := sm.CountReplicaShards(config)
			var min_gid int64
			min_s := MAX
			for g, ct := range count{
				// get minimum number
				if ct < min_s{
					min_s = ct
					min_gid = g
				}
			}
			config.Shards[shard] = min_gid
		}
	}

	// balance the load
	balanced := false
	for !balanced{
		count := sm.CountReplicaShards(config)

		// get max and min number of shards
		min_s := MAX
		max_s := MIN
		for _, ct := range count{
			if ct < min_s{
				min_s = ct
			}
			if ct > max_s{
				max_s = ct
			}
		}

		// check if it is currently balanced
		if max_s - min_s <= 1{
			balanced = true
			break
		}

		// otherwise, rebalance shards until balanced
		for shard, g := range config.Shards{
			count := sm.CountReplicaShards(config)
			// get max and min group using the max/min shards
			min_s := MAX
			max_s := MIN
			var min_gid, max_gid int64
			for group, ct := range count{
				if ct < min_s{
					min_s = ct
					min_gid = group
				}
				if ct > max_s{
					max_s = ct
					max_gid = group
				}
			}

			// if it's max group, give shards to min replica group
			if g == max_gid{
				config.Shards[shard] = min_gid
			}
		}
	}
}

// Update according to the operations (Join, Leave, Move, Query)
func (sm *ShardMaster) Update(opt Op) Config{
	DPrintf("Entering Update")
	op := opt.Operation
	gid, server := opt.Group_ID, opt.Server
	if op == "Query"{
		config_num := opt.NConfig
		// Query -> replies with the configuration that has that number
		// if num is -1 or bigger than max config num, reply with latest config
		length := len(sm.configs)
		if config_num == -1 || config_num > length - 1{
			last_config := sm.configs[length - 1]
			return last_config
		}else{
			query := sm.configs[config_num]
			return query
		}
	}else{
		new_config := sm.CreateNewConfig()
		if op == "Join"{
			// Join -> create new config that includes the new replica group
			new_config.Groups[gid] = server
			// rebalance the shards
			sm.BalanceShards(gid, new_config)
			sm.configs = append(sm.configs, *new_config)
		}else if op == "Leave"{
			// Leave -> create new config that doesn't include the group
			new_config := sm.CreateNewConfig()
			// remove group
			delete(new_config.Groups, gid)
			// rebalance the shards
			sm.BalanceShards(gid, new_config)
			sm.configs = append(sm.configs, *new_config)
		}else if op == "Move"{
			new_config := sm.CreateNewConfig()
			// Move -> create new config in which the shard is assigned to the group
			shard_num := opt.NShard
			new_config.Shards[shard_num] = gid
			sm.configs = append(sm.configs, *new_config)
		}
		// sm.configs = append(sm.configs, *new_config)
		return Config{}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	DPrintf("JOIN: Entering Join")
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()
	set_op := Op{Operation: "Join", Curr_ID: nrand(), Group_ID: args.GID, Server: args.Servers}

	// run Paxos methods on set_op
	for{
		var res Op

		// add seq count by 1
		sm.seqCount += 1

		// get status of current Paxos
		status, val := sm.px.Status(sm.seqCount)

		// check if decided, if not then run Paxos again
		DPrintf("JOIN: Checking STATUS")
		if status == paxos.Decided{
			res = val.(Op)
		} else{
			res = sm.Paxos(sm.seqCount, set_op)
			DPrintf("JOIN: GET: Done Running Paxos")
		}

		// update configuration by applying Join
		sm.Update(res)
		DPrintf("JOIN: Updating Join")

		// finish processing -> call Done from Paxos
		sm.px.Done(sm.seqCount)
		DPrintf("JOIN: Called Done from Paxos")

		// Paxos chose the current operation ID -> done
		if res.Curr_ID == set_op.Curr_ID{
			break
		}
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	DPrintf("LEAVE: Entering Leave")
	sm.mu.Lock()
	defer sm.mu.Unlock()
	set_op := Op{Operation: "Leave", Curr_ID: nrand(), Group_ID: args.GID}

	// run Paxos methods on set_op
	for{
		var res Op

		// add seq count by 1
		sm.seqCount += 1

		// get status of current Paxos
		status, val := sm.px.Status(sm.seqCount)

		// check if decided, if not then run Paxos again
		DPrintf("LEAVE: Checking STATUS")
		if status == paxos.Decided{
			res = val.(Op)
		} else{
			res = sm.Paxos(sm.seqCount, set_op)
			DPrintf("LEAVE: GET: Done Running Paxos")
		}

		// update configuration by applying Join
		sm.Update(res)
		DPrintf("LEAVE: Updating Join")

		// finish processing -> call Done from Paxos
		sm.px.Done(sm.seqCount)
		DPrintf("LEAVE: Called Done from Paxos")

		// Paxos chose the current operation ID -> done
		if res.Curr_ID == set_op.Curr_ID{
			break
		}
	}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	DPrintf("MOVE: Entering Move")
	sm.mu.Lock()
	defer sm.mu.Unlock()
	set_op := Op{Operation: "Move", Curr_ID: nrand(), Group_ID: args.GID, NShard: args.Shard}

	// run Paxos methods on set_op
	for{
		var res Op

		// add seq count by 1
		sm.seqCount += 1

		// get status of current Paxos
		status, val := sm.px.Status(sm.seqCount)

		// check if decided, if not then run Paxos again
		DPrintf("MOVE: Checking STATUS")
		if status == paxos.Decided{
			res = val.(Op)
		} else{
			res = sm.Paxos(sm.seqCount, set_op)
			DPrintf("MOVE: GET: Done Running Paxos")
		}

		// update configuration by applying Join
		sm.Update(res)
		DPrintf("MOVE: Updating Join")

		// finish processing -> call Done from Paxos
		sm.px.Done(sm.seqCount)
		DPrintf("MOVE: Called Done from Paxos")

		// Paxos chose the current operation ID -> done
		if res.Curr_ID == set_op.Curr_ID{
			break
		}
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.
	DPrintf("QUERY: Entering Query")
	sm.mu.Lock()
	defer sm.mu.Unlock()
	set_op := Op{Operation: "Query", Curr_ID: nrand(), NConfig: args.Num}
	// run Paxos methods on set_op
	for{
		var res Op

		// add seq count by 1
		sm.seqCount += 1

		// get status of current Paxos
		status, val := sm.px.Status(sm.seqCount)

		// check if decided, if not then run Paxos again
		DPrintf("QUERY: Checking STATUS")
		if status == paxos.Decided{
			res = val.(Op)
		} else{
			res = sm.Paxos(sm.seqCount, set_op)
			DPrintf("QUERY: GET: Done Running Paxos")
		}

		// update configuration by applying Join
		set_config := sm.Update(res)
		DPrintf("QUERY: Updating Join")

		// finish processing -> call Done from Paxos
		sm.px.Done(sm.seqCount)
		DPrintf("QUERY: Called Done from Paxos")

		// Paxos chose the current operation ID -> done
		// Get config that is queried
		if res.Curr_ID == set_op.Curr_ID{
			reply.Config = set_config
			break
		}
	}
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	// intialization
	sm.seqCount = 0

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						DPrintf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
