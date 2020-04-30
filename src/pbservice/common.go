package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type Request struct{
	Key			string
	Value 	string
	Command			string
}

// Put or Append
type PutAppendArgs struct {
	Key   	string
	Value 	string
	// You'll have to add definitions here.
	Command	string
	ID			int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ID	int64
}

type GetReply struct {
	Err   Err
	Value string
}

type TransferToBackupArgs struct{
	Kv				map[string]string
	Requests	map[int64]Request
}

type TransferToBackupReply struct{
	Err		Err
}

// Your RPC definitions here.
