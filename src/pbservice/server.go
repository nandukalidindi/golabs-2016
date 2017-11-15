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
import "strconv"



type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view *viewservice.View
	rpcRequestMap map[string]string
	store map[string]string
	valid bool
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// LOCK BECAUSE OF CONCURRENT REQUESTS TO PB SERVER

	// CHECK IF THE REQUEST IS ALREADY LOGGED AND PROCESS ACCORDINGLY
	// BY USING THE EXISTING VALUE FROM RECENT REQUESTS OR GET FROM STORE
	// AFTER TALKING TO BACKUP
	pb.mu.Lock()
	
	if pb.me != pb.view.Primary || !pb.valid {
		pb.mu.Unlock()
		return fmt.Errorf("INVALID PRIMARY!!")
	}
	
	key := args.Key
	requestID := args.Name + ":" + strconv.FormatInt(args.Number, 10)
	rValue, ok := pb.rpcRequestMap[requestID];
	if !ok {		
		reply.Value = pb.store[key]
		sValue, ok := pb.store[key]
		
		if ok {
			reply.Value = sValue
		} else {
			reply.Err = ErrNoKey
		}

		if pb.view.Backup != "" {
			checkArgs := SplitBrainCheckArgs{args.Key, "", args.Number, args.Name, ""}
			checkReply := SplitBrainCheckReply{}

			// TALK TO BACKUP TO PREVENT SPLIT BRAIN SYNDROME
			checkOK := call(pb.view.Backup, "PBServer.RPCToBackupBeforeGET", checkArgs, &checkReply)
			if !checkOK || reply.Err != "" {
				pb.mu.Unlock()
				return fmt.Errorf("Backup unable to respond during GET")
			}
		}

		pb.rpcRequestMap[requestID] = reply.Value
	} else{
		reply.Value = rValue
		pb.mu.Unlock()
		return nil
	}
 	pb.mu.Unlock()

	return nil
}


func (pb *PBServer) RPCToBackupBeforeGET(args *SplitBrainCheckArgs, reply *SplitBrainCheckReply) error {
	if pb.view.Backup != pb.me {
		reply.Err = Err(ErrWrongServer)
		return fmt.Errorf(ErrWrongServer)
	}
	
	requestID := args.Name + ":" + strconv.FormatInt(args.Number, 10)
	pb.rpcRequestMap[requestID] = ""
	
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	// LOCK BECAUSE OF CONCURRENT REQUESTS TO PB SERVER

	// CHECK IF THE REQUEST IS ALREDY IN THE RECENT REQUEST LIST
	// IF YES THEN EITHER YOU NEED TO APPEND TO EXISTING VALUE AND SEND RPC 
	// TO BACKUP BEFORE COMMITTING
	pb.mu.Lock()	
	if pb.me != pb.view.Primary || !pb.valid {
		pb.mu.Unlock()
		return fmt.Errorf("INVALID PRIMARY!!")
	}
	value := args.Value
	
	requestID := args.Name + ":" + strconv.FormatInt(args.Number, 10)
	rValue, ok := pb.rpcRequestMap[requestID];
	if !ok {
		if args.Append {
			existing := pb.store[args.Key]
			reply.OldAppend = existing
			value = existing + value
			args.Value = value
		}

		if pb.view.Backup != "" {
			checkArgs := SplitBrainCheckArgs{args.Key, args.Value, args.Number, args.Name, reply.OldAppend}
			checkReply := &SplitBrainCheckReply{}

			// TALK TO BACKUP TO PREVENT SPLIT BRAIN SYNDROME
			checkOK := call(pb.view.Backup, "PBServer.RPCToBackupBeforePUT", checkArgs, &checkReply)
			if !checkOK || reply.Err != "" {
				pb.mu.Unlock()
				return fmt.Errorf("Backup unable to respond during PUT")
			}
		}

		pb.store[args.Key] = value
		pb.rpcRequestMap[requestID] = reply.OldAppend
	} else {		
		reply.OldAppend = rValue
		pb.mu.Unlock()
		return nil
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) RPCToBackupBeforePUT(args *SplitBrainCheckArgs, reply *SplitBrainCheckReply) error {	
	if pb.view.Backup != pb.me {
		reply.Err = Err(ErrWrongServer)
		
		return fmt.Errorf(ErrWrongServer)
	}
	
	requestID := args.Name + ":" + strconv.FormatInt(args.Number, 10)
	pb.rpcRequestMap[requestID] = args.OldAppend
	pb.store[args.Key] = args.Value
	
	return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	// TICK EVERY INTERVAL TO CHECK VALIDITY OF THE CURRENT VIEW STATE AND TAKE
	// ACTIONS ACCORDINGLY
	pb.mu.Lock()
	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err == nil {
		old := pb.view.Viewnum
		var syncErr error

		if view.Primary == pb.me && old != view.Viewnum {
			if view.Backup == "" {
				syncErr = nil
			} else {
				args := &SyncArgs{pb.me, pb.store, pb.rpcRequestMap}
				var reply SyncReply
				ok := call(view.Backup, "PBServer.InvalidViewCorrection", args, &reply)
				if !ok {
					syncErr = fmt.Errorf("ERROR WHILE CORRECTING INVALID VIEW!!")
				}
			}

			if syncErr == nil && old == 0 {
				pb.valid = true
			}
		} 
		
		if view.Primary != pb.me && view.Backup != pb.me && pb.valid {
			pb.valid = false
		}
		if syncErr == nil {
			pb.view = &view
		}
	}
	pb.mu.Unlock()
}

func (pb *PBServer) InvalidViewCorrection(args *SyncArgs, reply *SyncReply) error {
	if pb.view.Backup != pb.me || args.Primary != pb.view.Primary {
		reply.Err = Err(ErrWrongServer)
		return fmt.Errorf(ErrWrongServer)
	}
	pb.rpcRequestMap = args.RpcRequestMap
	pb.store = args.Store
	pb.valid = true
	return nil
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
	pb.store = make(map[string]string)
	pb.rpcRequestMap = make(map[string]string)
	pb.view = &viewservice.View{}
	pb.valid = false

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
