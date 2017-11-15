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
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	recentPings map[string]time.Time
	currentView View
	isCurrentPrimaryAcked bool
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// LOCK BECAUSE OF CONCURRENT REQUESTS TO VIEW SERVER
	vs.mu.Lock()

	vs.recentPings[args.Me] = time.Now()

	if vs.currentView.Primary == "" {
		vs.addPrimary()
	}

	if vs.currentView.Backup == "" {
		vs.addBackup()
	}

	if args.Me == vs.currentView.Primary || args.Me == vs.currentView.Backup {
		if vs.currentView.Viewnum > args.Viewnum { //a crash!
			if args.Me == vs.currentView.Primary {
				vs.removePrimary()
			}

			if args.Me == vs.currentView.Backup {
				vs.removeBackup()
			}
		}
		if args.Me == vs.currentView.Primary {
			if args.Viewnum == vs.currentView.Viewnum {
				vs.isCurrentPrimaryAcked = true
			}
		}
	}

	reply.View = vs.currentView

	vs.mu.Unlock()
	// LOCK BECAUSE OF CONCURRENT REQUESTS TO VIEW SERVER
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// NOT ACTUALLY REQUIRED BUT LETS LOCK EVERYTHING DOWN ANYWAY
	// LOCK BECAUSE OF CONCURRENT REQUESTS TO VIEW SERVER
	vs.mu.Lock()
	reply.View = vs.currentView
	vs.mu.Unlock()
	// LOCK BECAUSE OF CONCURRENT REQUESTS TO VIEW SERVER

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	// PERIODICALLY CHECK IF THE CURRENT VIEW IS VALID AND TAKE NECESSARY ACTIONS
	// LIKE PROMOTING BACKUP TO PRIMARY OR PROMOTING IDLE SERVER TO BACKUP ETC.
	// LOCK BECAUSE OF CONCURRENT REQUESTS TO VIEW SERVER
	vs.mu.Lock()
	for server, recentPing := range vs.recentPings {
		if time.Now().Sub(recentPing).Seconds() * 10 >= DeadPings {
			if server == vs.currentView.Primary {
				vs.removePrimary()
			}
			if server == vs.currentView.Backup {
				vs.removeBackup()
			}
			delete(vs.recentPings, server)
		}
	}
	vs.mu.Unlock()
	// LOCK BECAUSE OF CONCURRENT REQUESTS TO VIEW SERVER
}

func (vs *ViewServer) addPrimary() {
	// SELECTING PRIMARY FOR THE FIRST TIME
	if vs.currentView.Primary == "" {
		vs.currentView.Primary = vs.findActiveServer()
		vs.updateView()
	}
}

func (vs *ViewServer) removePrimary() {
	// REMOVE PRIMARY AND PROMOTE BACKUP TO PRIMARY
	if vs.isCurrentPrimaryAcked {
		vs.currentView.Primary = vs.currentView.Backup
		vs.currentView.Backup = vs.findActiveServer()
		vs.updateView()
	}
}

func (vs *ViewServer) addBackup() {
	// FIND BACKUP FROM A LIST OF IDLE SERVERS
	availableBackup := vs.findActiveServer()

	if availableBackup != "" {
		vs.currentView.Backup = availableBackup
		vs.updateView()
	}
}

func (vs *ViewServer) removeBackup() {
	// REMOVE BACKUP AND PROBABLY PROMOTE AN IDLE SERVER 
	// OR LET IT BE BECAUSE BACKUP CAN BE NIL
	if vs.isCurrentPrimaryAcked {
		vs.currentView.Backup = ""
		vs.updateView()
	}
}

func (vs *ViewServer) updateView() {
	// UPDATE OR MOVE THE CURRENT VIEW LIKE THE SLIDING WINDOW
	// TO DISCARD OLD INVALID VIEW STATE 
	vs.currentView.Viewnum++
	vs.isCurrentPrimaryAcked = false
}

func (vs *ViewServer) findActiveServer() string {
	// FIND SERVER FROM THE LIST OF GIVEN SERVERS THAT ARE RECORDED
	// IN THE recentPings HASHMAP AND RETURN A SERVER THAT IS RESPONSIVE
	// WITHIN THE DEADINTERVALS TIME FRAME
	for server, recentPing := range vs.recentPings {
		liveServer := time.Now().Sub(recentPing).Seconds() * 10 < DeadPings
		if liveServer && server != vs.currentView.Primary && server != vs.currentView.Backup {
			return server
		}
	}
	return ""
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
	vs.recentPings = make(map[string]time.Time)
	vs.currentView = View{0, "", ""}
	vs.isCurrentPrimaryAcked = false

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
