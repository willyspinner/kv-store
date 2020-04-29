package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       		sync.Mutex
	l        		net.Listener
	dead     		int32 // for testing
	rpccount 		int32 // for testing
	me       		string


	// Your declarations here.
	currentView View
	pingTime		map[string]time.Time
	idleServer	string
	primaryACK	bool
	backupACK		bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()

	// update time
	currentServer := args.Me
	vs.pingTime[currentServer] = time.Now()

	// check primary server
	if currentServer == vs.currentView.Primary{
		// check if it ACKs currentView
		if args.Viewnum == vs.currentView.Viewnum{
			vs.primaryACK = true
		} else if args.Viewnum == 0 && vs.primaryACK == true{
			// primary crash -> backup takes over
			update(vs, vs.currentView.Backup, vs.idleServer)
		}
	} else if currentServer == vs.currentView.Backup{
		// check if it ACKs currentView
		if args.Viewnum == vs.currentView.Viewnum{
			vs.backupACK = true
		} else if args.Viewnum == 0 && vs.backupACK == true{
			// backup crash -> use idle server as backup
			update(vs, vs.currentView.Primary, vs.idleServer)
		}
	} else{
		// other pings
		if vs.currentView.Viewnum != 0{
			// new idle server
			vs.idleServer = currentServer
		} else{
			// make primary
			update(vs, currentServer, "")
		}
	}

	reply.View = vs.currentView
	log.Printf("Viewserver reply: %v\n", reply.View)
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.currentView
	vs.mu.Unlock()
	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick(){
	log.Printf("Entering tick")
	// Your code here.
	vs.mu.Lock()
	now := time.Now()
	timeWindow := DeadPings * PingInterval

	// check if we can get the primary server
	if now.Sub(vs.pingTime[vs.currentView.Primary]) >= timeWindow && vs.primaryACK == true{
		// primary already ACK currentView -> update view using backup
		log.Printf("Viewserver tick: received primary")
		update(vs, vs.currentView.Backup, vs.idleServer)
	}

	// check recent pings from backup server
	if now.Sub(vs.pingTime[vs.currentView.Backup]) >= timeWindow  && vs.backupACK == true{
		// check if it has an idle server
		log.Printf("Viewserver tick: received backup")
		// check if there's an idle server
		if vs.idleServer != ""{
			// use idle server as backup
			update(vs, vs.currentView.Primary, vs.idleServer)
		}
	}

	// check pings from idle server
	if now.Sub(vs.pingTime[vs.idleServer]) >= timeWindow{
		log.Printf("Viewserver tick: idle server")
		vs.idleServer = ""
	} else {
		if vs.primaryACK == true && vs.idleServer != "" && vs.currentView.Backup == ""{
			// no backup -> use idle server
			update(vs, vs.currentView.Primary, vs.idleServer)
		}
	}

	vs.mu.Unlock()
}

// function that updates the view according to the given primary/ backup servers
func update(vs *ViewServer, primary string, backup string){
	vs.currentView.Viewnum += 1
	vs.currentView.Primary = primary
	vs.currentView.Backup = backup
	vs.idleServer = ""
	vs.primaryACK = false
	vs.backupACK = false
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{}
	vs.currentView.Viewnum = 0
	vs.currentView.Primary = ""
	vs.currentView.Backup = ""
	vs.idleServer = ""
	vs.pingTime = make(map[string]time.Time)
	vs.primaryACK = false
	vs.backupACK = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
