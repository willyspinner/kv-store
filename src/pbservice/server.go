package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"



type PBServer struct {
	mu         	sync.Mutex
	l          	net.Listener
	dead       	int32 // for testing
	unreliable 	int32 // for testing
	me         	string
	vs         	*viewservice.Clerk
	// Your declarations here.
	currentView	viewservice.View
	keyVal		map[string]string
	inSync		bool			// tracks if primary and backup are in sync
	clientReq	map[int64]Request	// client requests: job ID & request type
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()

	// Duplicate RPC request
	req, ok := pb.clientReq[args.ID]
	if req.Key == args.Key && ok == true{
		reply.Err = OK
		reply.Value = pb.keyVal[args.Key]
		return nil
	}

	// if not primary -> send back
	if pb.me != pb.currentView.Primary{
		reply.Err = ErrWrongServer
		return nil
	}

	// get key/value from clientReq
	val, k := pb.keyVal[args.Key]
	if k == false{
		reply.Err = ErrNoKey
	} else{
		reply.Err = OK
		reply.Value = val
	}

	// update clientReq
	new_req := pb.clientReq[args.ID]
	new_req.Key = args.Key
	new_req.Value = ""
	new_req.Command = "Get"

	// forward to backup
	if pb.currentView.Backup != ""{
		// send RPC request
		send := call(pb.currentView.Backup, "PBServer.BackupGet", args, &reply)
		if send == false || reply.Err == ErrWrongServer || reply.Value != pb.keyVal[args.Key]{
			pb.inSync = true
		}
	}

	pb.mu.Unlock()
	return nil

}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()

	// Duplicate request
	req, ok := pb.clientReq[args.ID]
	if req.Key == args.Key && req.Command == args.Command && ok == true{
		reply.Err = OK
		return nil
	}

	// not primary -> send back
	if pb.me != pb.currentView.Primary{
		reply.Err = ErrWrongServer
		return nil
	}

	// process put and append commands
	if args.Command == "Put"{
		pb.keyVal[args.Key] = args.Value
	} else if args.Command == "Append"{
		pb.keyVal[args.Key] = args.Value + pb.keyVal[args.Key]
	}

	// update clientReq
	new_req := pb.clientReq[args.ID]
	new_req.Key = args.Key
	new_req.Value = args.Value
	new_req.Command = args.Command
	reply.Err = OK

	// forward to backup
	if pb.currentView.Backup != ""{
		// send RPC request
		send := call(pb.currentView.Backup, "PBServer.BackupGet", args, &reply)
		if send == false || reply.Err != OK || args.Value != pb.keyVal[args.Key]{
			pb.inSync = true
		}
	}
	pb.mu.Unlock()
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) TransferToBackup(args *TransferToBackupArgs, reply *TransferToBackupReply) error{
	// ping viewservice
	curr, ok := pb.vs.Ping(pb.currentView.Viewnum)
	if ok != nil{
		log.Printf("Ping failed: %v", curr)
	}

	// if not backup send back
	if pb.me != curr.Backup{
		reply.Err = ErrWrongServer
		return nil
	}

	// update keyVal for backup
	pb.keyVal = args.Kv
	pb.clientReq = args.Requests
	reply.Err = OK
	return nil
}

func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()

	// ping viewserver
	curr, ok := pb.vs.Ping(pb.currentView.Viewnum)

	if ok == nil{
		if pb.inSync == true{
			// sync primary and backup
			args := &TransferToBackupArgs{Kv: pb.keyVal, Requests: pb.clientReq}

			// send RPC request
			var reply TransferToBackupReply
			req := call(curr.Backup, "PBServer.TransferToBackup", args, &reply)

			if reply.Err != OK || req == false{
				pb.inSync = true
			}
			// set inSync to false after successful transfer
			pb.inSync = false
		}
		if pb.me == curr.Primary && pb.currentView.Backup != curr.Backup{
			if curr.Backup != ""{
				pb.inSync = true
			}
		}
	} else{
		log.Printf("Ping failed: %v", curr)
	}

	// update view
	pb.currentView = curr
	pb.mu.Unlock()

}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
