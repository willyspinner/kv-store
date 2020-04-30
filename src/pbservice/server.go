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
import "path"



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
	clientReq	map[int64]Request	// client requests: job ID & request type
}


/* helper log unction */
func (pb *PBServer) logPrintf(format string, args ...interface{}) {
    /* prints the basename of pb.me and the other stuff. */
    serviceName := path.Base(pb.me)
    isDebug := os.Getenv("LOGLEVEL") == "DEBUG"
    if isDebug {
        fmt.Printf("%s: %s", serviceName, fmt.Sprintf(format, args...))
    }

}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
    defer pb.mu.Unlock()

	// if not primary -> send back
	if pb.me != pb.currentView.Primary{
		reply.Err = ErrWrongServer
        pb.logPrintf("GET req: %v, BUT wrong server \n", args)
		return nil
	}
    pb.logPrintf("GET req: %v, VAL: %s \n", args, pb.keyVal[args.Key] )

	// Duplicate RPC request
	// return old value (stale) for that request
	req, exists  := pb.clientReq[args.ID]
	if exists && req.Key == args.Key {
		reply.Err = OK
		reply.Value = pb.keyVal[args.Key]
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
	return nil

}



func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
    defer pb.mu.Unlock()

	// Check for Duplicate request
	req, exists := pb.clientReq[args.ID]
	pb.logPrintf("got PutAppend req: %v\n", args)
	if exists && req.Key == args.Key && req.Command == args.Command && req.Value == args.Value {
		reply.Err = OK
		return nil
	}

	// not primary -> send back
	if pb.me != pb.currentView.Primary{
		reply.Err = ErrWrongServer
		return nil
	}


	reply.Err = OK

	// forward to backup, if there is one
	if pb.currentView.Backup != "" {
		// send RPC request
        var backupReply PutAppendReply
        needBackup := false
        for {
            send := false
            if !needBackup {
                send = call(pb.currentView.Backup, "PBServer.ForwardPutAppend", args, &backupReply)
            }
            if send == true {
                break
            } else {
                // When call fails, we need to retry until it succeeds. Otherwise, consider scenario:
                // 1. X gets PUTted to primary. Primary stores 
                // 2. primary forwards to backup but fails. Primary will sync to backup next tick
                // 3. primary goes down before next tick
                // 4. backup becomes primary. Doesn't have new X.

                curr, _ := pb.vs.Ping(pb.currentView.Viewnum)
                if curr.Primary != pb.me {
                    reply.Err = ErrWrongServer
                    pb.currentView = curr
                    return nil
                }
                if curr.Backup == pb.currentView.Backup {
                    // we don't need to sync backup. We just need to forward this put again.
                    needBackup = false
                } else {
                    if curr.Backup == "" {
                        // current backup is down. We don't need to backup. 
                        pb.currentView = curr
                        needBackup = false
                        break
                    } else {
                        // deal with new backup. Need to sync HERE
                        backupSuccess := pb.syncToBackup(curr.Backup)
                        needBackup = backupSuccess == false
                    }
                }
                time.Sleep(viewservice.PingInterval)
                pb.currentView = curr
            }
        }
        if backupReply.Err == ErrWrongServer {
            reply.Err = ErrWrongServer  // this server not primary anymore
            return nil
        } else {
            reply.Err = OK
        }
	} else {
		pb.logPrintf("no backup in currentView. Skipping forwarding.... \n")
	}

	// After being sure that backup has our write, then we store it locally.
	if args.Command == "Put" {
		pb.keyVal[args.Key] = args.Value
	} else if args.Command == "Append" {
        _, exists := pb.keyVal[args.Key]
        if ! exists {
            pb.keyVal[args.Key] = ""
        }
		pb.keyVal[args.Key] += args.Value
	}
	// Lastly, we update clientReq , as we've finally 'committed' the request (backups have it)
    pb.clientReq[args.ID] = Request{Key: args.Key, Value: args.Value, Command: args.Command}


	return nil
}



/* RPC function to forward PUT/APPEND requests to backup server */
func (pb *PBServer) ForwardPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.me == pb.currentView.Primary{
        pb.logPrintf("IS A PRIMARY BUT got forwarded PutAppend req: %v\n", args)
        // I'm a primary! Don't forward the requests to me! 
		reply.Err = ErrWrongServer
		return nil
	}

    // First, need to check if request already done. This is to make sure that when this backup's ack to primary 
    // gets lost, when primary reacks again, this backup doesn't do anything.
	req, exists := pb.clientReq[args.ID]
	if exists && req.Key == args.Key && req.Command == args.Command && args.Value == req.Value {
		reply.Err = OK
        pb.logPrintf("Backup: got forwarded PutAppend req: %v, but request alr exists \n", args)
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
		pb.logPrintf("Ping failed in TransferToBackup(): %v\n", curr)
	}

    pb.mu.Lock()
    defer pb.mu.Unlock()
	// if not backup send back
	if pb.me != curr.Backup {
		reply.Err = ErrWrongServer
        pb.logPrintf("got TransfertoBackup request, but i'm a primary! \n")
		return nil
	}
    pb.logPrintf("Backup: got TransfertoBackup request: %v \n", args)

	// update keyVal for backup
	pb.keyVal = args.Kv
	pb.clientReq = args.Requests
	reply.Err = OK
	return nil
}

func (pb *PBServer) syncToBackup(backupSrv string) bool {
    args := &TransferToBackupArgs{Kv: pb.keyVal, Requests: pb.clientReq}
    var reply TransferToBackupReply
    ok := call(backupSrv, "PBServer.TransferToBackup", args, &reply)
    return ok && reply.Err == OK
}


func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// ping viewserver
    var curr viewservice.View
    var pingErr error
    for {
        curr, pingErr = pb.vs.Ping(pb.currentView.Viewnum)
        if pingErr == nil {
            if pb.me == curr.Primary {
                if (pb.currentView.Backup != curr.Backup) && curr.Backup != "" {
                    // since new backup is detected, we have to transfer entire KV to backup.
                    syncOK := pb.syncToBackup(curr.Backup)
                    if syncOK {
                        break
                    } else {
                        pb.logPrintf("FAILed to syncToBackup during tick(). Retrying.. \n")
                    }
                } else {
                    break
                }
            } else {
                break
            }
        } else {
            pb.logPrintf("Ping failed in tick() method: %v\n", curr)
        }
        time.Sleep(viewservice.PingInterval)
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
