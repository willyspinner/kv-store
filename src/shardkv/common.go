package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    ID   int64

}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
    ID int64
}

type GetReply struct {
	Err   Err
	Value string
}


// these are used to get key values of shards in join, move, leave, etc.
type RetrieveKVArgs struct {
    NewConfigNum int
}

type RetrieveKVReply struct {
    KeyVal map[string] string
    Requests map[int64] bool
}
