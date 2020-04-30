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

	// if not primary -> send back
	if pb.me != pb.currentView.Primary{
		reply.Err = ErrWrongServer
		return nil
	}

	// Duplicate RPC request
	// return old value (stale) for that request
	req, exists  := pb.clientReq[args.ID]
	if exists && req.Key == args.Key {
		reply.Err = OK
		reply.Value = pb.keyVal[args.Key]
		pb.mu.Unlock()
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

    // form the request obj and update clientReq
    pb.clientReq[args.ID] = Request{Key:args.Key, Value: "", Command: "Get"}

    // No need to forward a 'Get' to backup. We are done here
	pb.mu.Unlock()
	return nil

}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
    defer pb.mu.Unlock()

	// Check for Duplicate request
	req, exists := pb.clientReq[args.ID]
	fmt.Printf("%s: got PutAppend req: %v\n", pb.me, args)
	if exists && req.Key == args.Key && req.Command == args.Command {
		reply.Err = OK
		return nil
	}

	// not primary -> send back
	if pb.me != pb.currentView.Primary{
		reply.Err = ErrWrongServer
		return nil
	}


	// First, we update clientReq
    pb.clientReq[args.ID] = Request{Key: args.Key, Value: args.Value, Command: args.Command}
	reply.Err = OK

	// forward to backup, if there is one
	if pb.currentView.Backup != ""{
		// send RPC request
        var backupReply PutAppendReply 
		fmt.Printf("%s: forwarding to backup.. \n", pb.me)
		send := call(pb.currentView.Backup, "PBServer.ForwardPutAppend", args, &backupReply)
		fmt.Printf("%s: backup forwarded. \n", pb.me)
        if send == false {
            // error. Cannot put to backup. for now, respond to the request, but note that we need to sync with backup when
            // it goes online.
            pb.inSync = false // primary and backup are now out of sync. We need to sync at the next tick()
            reply.Err = OK
        } else if backupReply.Err == ErrWrongServer {
            // backup is now a primary server. We need to sync.
            reply.Err = ErrWrongServer  // this server not primary anymore
			// at this point, we don't actually write it.
			return nil
        } else if backupReply.Err == OK {
            reply.Err = OK
        }
	} else {
		fmt.Printf("%s: no backup in currentView. Skipping forwarding.... \n", pb.me)
	}
	// Then, write it here.
	if args.Command == "Put"{
		pb.keyVal[args.Key] = args.Value
	} else if args.Command == "Append" {
        _, exists := pb.keyVal[args.Key]
        if ! exists {
            pb.keyVal[args.Key] = ""
        }
		pb.keyVal[args.Key] += args.Value
	}


	return nil
}



/* RPC function to forward PUT/APPEND requests to backup server */
func (pb *PBServer) ForwardPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.me == pb.currentView.Primary{
        // I'm a primary! Don't forward the requests to me! 
		reply.Err = ErrWrongServer
		return nil
	}

	if args.Command == "Put" {
		pb.keyVal[args.Key] = args.Value
	} else if args.Command == "Append"{
        _, exists := pb.keyVal[args.Key]
        if ! exists {
            pb.keyVal[args.Key] = ""
        }
		pb.keyVal[args.Key] += args.Value
	}
	// update clientReq
    pb.clientReq[args.ID] = Request{Key: args.Key, Value: args.Value, Command: args.Command}
	reply.Err = OK
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

    pb.mu.Lock()
	// if not backup send back
	if pb.me != curr.Backup {
		reply.Err = ErrWrongServer
        pb.mu.Unlock()
		return nil
	}

	// update keyVal for backup
	pb.keyVal = args.Kv
	pb.clientReq = args.Requests
	reply.Err = OK
    pb.mu.Unlock()
	return nil
}

func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// ping viewserver
	curr, pingErr := pb.vs.Ping(pb.currentView.Viewnum)
	if pingErr == nil {
		//TODO: verify that this is correct
		if pb.me == curr.Primary {
			if (pb.inSync == false || pb.currentView.Backup != curr.Backup) && curr.Backup != "" {
				// since primary and backup not in sync, we
				// have to transfer entire KV to backup.
				args := &TransferToBackupArgs{Kv: pb.keyVal, Requests: pb.clientReq}
				var reply TransferToBackupReply
				ok := call(curr.Backup, "PBServer.TransferToBackup", args, &reply)
				pb.inSync = (ok && reply.Err == OK)
			}
		}
	} else {
		log.Printf("Ping failed: %v", curr)
	}
	// update view
	pb.currentView = curr

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
	pb.currentView.Viewnum = 0
	pb.currentView.Primary = ""
	pb.currentView.Backup = ""
	pb.inSync = false
	pb.keyVal = make(map[string]string)
	pb.clientReq = make(map[int64]Request)
	pb.mu = sync.Mutex{}


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
