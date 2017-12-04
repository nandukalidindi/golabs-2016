package kvpaxos

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


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
    // Your definitions here.
    // Field names must start with capital letters,
    // otherwise RPC will break.
    From string
    ClerkInstance int
    PaxosInstance int
  
    Type string
    Key string
    Value string
    OldAppend string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	log map[int]*Op
    responses map[string]*Op
    store map[string]string
    opChannelMap map[int]chan *Op
    highest int
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	instance, c := kv.instanceChannel()

	proposedVal := Op{args.From, args.Instance, instance, "Get", args.Key, "", ""}

	// START ELECTION
	kv.px.Start(instance, proposedVal)

	// BLOCK UNTIL VALUE
	acceptedVal := <- c
	
	if acceptedVal.From != args.From || acceptedVal.ClerkInstance != args.Instance {
		reply.Err = "ERROR DURING GET"
		kv.mu.Lock()
		delete(kv.opChannelMap, instance)
		c <- &Op{}
		kv.mu.Unlock()
		return nil
	}
	reply.Err = ""
	reply.Value = acceptedVal.Value

	kv.mu.Lock()
	delete(kv.opChannelMap, instance)
	c <- &Op{}
	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	instance, c := kv.instanceChannel()
	opType := "Put"
	if args.Op == "Append" {
		opType = "Append"
	}

	proposedVal := Op{args.From, args.Instance, instance, opType, args.Key, args.Value, ""}

	// START ELECTION
	kv.px.Start(instance, proposedVal)

	// BLOCK UNTIL VALUE
	acceptedVal := <- c
	
	if acceptedVal.From != args.From  || acceptedVal.ClerkInstance != args.Instance {
		reply.Err = "ERROR DURING PUT"
		
		kv.mu.Lock()
		delete(kv.opChannelMap, instance)
		c <- &Op{}
		kv.mu.Unlock()
		return nil
	}

	reply.Err = ""
	reply.OldAppend = acceptedVal.OldAppend
	
	kv.mu.Lock()
	delete(kv.opChannelMap, instance)
	c <- &Op{}
	kv.mu.Unlock()
	return nil
}

func (kv *KVPaxos) instanceChannel() (int, chan *Op) {
	kv.mu.Lock()
	instance := kv.px.Max() + 1
	for _,ok := kv.opChannelMap[instance]; ok || kv.highest >= instance; {
		instance++
		_,ok = kv.opChannelMap[instance]
	}
	channel := make(chan *Op)
	kv.opChannelMap[instance] = channel
	kv.mu.Unlock()
	return instance, channel
}

func (kv *KVPaxos) duplicateCheck(from string, ClerkInstance int) (bool, *Op) {
	if duplicate, ok := kv.responses[from]; ok {
		if duplicate.ClerkInstance == ClerkInstance {
			return true, duplicate
		} else if ClerkInstance < duplicate.ClerkInstance {
			return true, &Op{From:"None"}
		}
	}
	return false, &Op{}
}

func (kv *KVPaxos) getStatus() {
	instance := 0
	restarted := false

	to := 10 * time.Millisecond
	for kv.isdead() == false {
		decided, r := kv.px.Status(instance)
		if decided == paxos.Fate(1) {
			op, isOp := r.(Op)
			if isOp {
				kv.mu.Lock()
				if op.From != "None" {
					kv.commit(instance, &op)
				} else {
					kv.highest = instance
					kv.px.Done(instance)
				}
				if ch, ok := kv.opChannelMap[instance]; ok {
					ch <- &op
					kv.mu.Unlock()
					<- ch
				} else {
					kv.mu.Unlock()
				}
			}
			instance++
			restarted = false
		} else {
			// SCHEDULE SLEEP AS MENTIONED IN THE LAB PAGE 9
			time.Sleep(to)
			if to < 25 * time.Millisecond {
				to *= 2
			} else {
				if !restarted {
					to = 10 * time.Millisecond
					kv.px.Start(instance, Op{From: "None"})
					time.Sleep(time.Millisecond)
					restarted = true
				}
			}
		}
	}
}

func (kv *KVPaxos) commit(instance int, op *Op) {
	if dup, d := kv.duplicateCheck(op.From, op.ClerkInstance); !dup {
		switch op.Type {
		case "Put":
			op.OldAppend = kv.store[op.Key]
			kv.store[op.Key] = op.Value
		case "Append":
			prev := kv.store[op.Key]
			value := prev + op.Value
			op.OldAppend = prev
			kv.store[op.Key] = value
		case "Get":
			if v, ok := kv.store[op.Key]; ok {
				op.Value = v
			}
		}
		kv.responses[op.From] = op
	} else {
		if d.From != "None" {
			op.From = d.From
			op.ClerkInstance = d.ClerkInstance
			op.PaxosInstance = d.PaxosInstance
			op.Type = d.Type
			op.Key = d.Key
			op.Value = d.Value
			op.OldAppend = d.OldAppend
		}
	}
	kv.highest = instance
	kv.px.Done(instance)
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.store = make(map[string]string)
    kv.log = make(map[int]*Op)
    kv.responses = make(map[string]*Op)
    kv.opChannelMap = make(map[int]chan *Op)
    kv.highest = -1

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	go kv.getStatus()
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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
