package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"
// import "strconv"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Decided   Fate = iota + 1
	Pending        //2 not yet decided.
	Forgotten      //3 decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	instances map[int]*instance
	done      map[int]int
}

type instance struct {
	status  Fate
	n_p      int
	n_a      int
	v_a      interface{}
	n_p_consensus int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) Proposer(proposeArgs *ProposeArgs, proposeReply *ProposeReply) {	
	px.proposer(proposeArgs.Instance, proposeArgs.Value)
}

func (px *Paxos) proposer(instance int, value interface{}) {	
	status, _ := px.Status(instance)
	for status == Fate(3) && px.dead == 0 {
		
		validProposal := px.generateValidProposal(instance)
		
		responses := make([]*PrepareReply, len(px.peers))
		for i, _ := range responses {
			responses[i] = &PrepareReply{false, -1, -1, nil, -1, -1}
		}

		done := -1
		px.mu.Lock()
		if s, ok := px.done[px.me]; ok {
			done = s
		}
		px.mu.Unlock()

		// PREPARE
		px.sendPrepare(instance, validProposal, done, responses)
		// PREPARE

		unaccepted := 0
		for _, r := range responses {
			if !r.OK {
				unaccepted++
				
				// IF NOT HIGHEST RESTART
				if px.highestConsensus(instance, r.N_P) {
					px.Start(instance, value)
					return
				}
			}
		}

		if unaccepted >= MinMajority {
			time.Sleep(time.Duration(rand.Int()%30) * time.Millisecond)
			px.Start(instance, value)
			return
		}


		value = px.chooseHighest(responses, value)

		done = -1
		px.mu.Lock()
		if s, ok := px.done[px.me]; ok {
			done = s
		}
		px.mu.Unlock()
		
		// ACCEPTANCE (PROMISING) IF LESS THAN MAJORITY, RESTART
		if px.acceptanceCount(instance, value, validProposal, done) < MinMajority {
			px.Start(instance, value)
			return
		}
		// ACCEPTANCE (PROMISING) IF LESS THAN MAJORITY, RESTART

		// DECISION IF LESS THAN MAJORITY, RESTART
		if px.decisionCount(instance, value) < MinMajority {
			px.Start(instance, value)
			return
		}
		// DECISION IF LESS THAN MAJORITY, RESTART

		status, _ = px.Status(instance)
	}
}

// **************************************************
// ******************* PREPARATION *******************
// **************************************************


func (px *Paxos) sendPrepare(instance int, validProposal int, done int, responses []*PrepareReply) {	
	for i, p := range px.peers {
		// SELF PREPARE NO RPC REQUIRED
		if i == px.me {
			reply := &PrepareReply{}			
			if px.PrepareRPC(&PrepareArgs{instance, validProposal, px.me, done}, reply) == nil {
				responses[reply.From] = reply
				px.setDone(reply.From, reply.Done)
			}
		// PREPARATION FOR PEERS
		} else {
			if ok, reply := px.sendPrepareToPeers(instance, p, validProposal); ok {
				responses[reply.From] = reply
				px.setDone(reply.From, reply.Done)
			}
		}
	}
}

func (px *Paxos) sendPrepareToPeers(instance int, dest string, validProposal int) (bool, *PrepareReply) {
	var reply PrepareReply
	done := -1
	px.mu.Lock()
	if val, ok := px.done[px.me]; ok {
		done = val
	}
	px.mu.Unlock()
	args := &PrepareArgs{instance, validProposal, px.me, done}
	ok := call(dest, "Paxos.PrepareRPC", args, &reply)
	return ok, &reply
}

func (px *Paxos) PrepareRPC(args *PrepareArgs, reply *PrepareReply) error {
	reply.From = px.me
	px.setDone(args.From, args.Done)
	px.mu.Lock()
	if val, ok := px.done[px.me]; ok {
		reply.Done = val
	} else {
		reply.Done = 0
	}
	i := px.instances[args.Instance]
	if i == nil {
		px.instances[args.Instance] = &instance{Fate(3), -1, -1, nil, -1}
		i = px.instances[args.Instance]
	}

	if args.N_P > i.n_p {
		i.n_p = args.N_P
		reply.OK = true
	} else {
		reply.OK = false
	}

	reply.N_A = i.n_a
	reply.V_A = i.v_a
	reply.N_P = i.n_p
	px.mu.Unlock()
	return nil
}

// **************************************************
// ******************* PREPARATION *******************
// **************************************************


// **************************************************
// ******************* ACCEPTANCE *******************
// **************************************************

func (px *Paxos) sendAccept(instance int, dest string, n_a int, v_a interface{}) (bool, *AcceptReply) {
	var reply AcceptReply
	done := 0
	px.mu.Lock()
	if val, ok := px.done[px.me]; ok {
		done = val
	}
	px.mu.Unlock()
	args := &AcceptArgs{instance, n_a, v_a, px.me, done}
	ok := call(dest, "Paxos.AcceptRPC", args, &reply)
	return ok, &reply
}

func (px *Paxos) AcceptRPC(args *AcceptArgs, reply *AcceptReply) error {
	reply.From = px.me
	px.setDone(args.From, args.Done)
	px.mu.Lock()

	if val, ok := px.done[px.me]; ok {
		reply.Done = val
	} else {
		reply.Done = 0
	}

	i := px.instances[args.Instance]
	if i == nil {
		reply.OK = false
	} else {
		if args.N_A >= i.n_p {
			i.n_p = args.N_A
			i.n_a = args.N_A
			i.v_a = args.V_A
			reply.OK = true
			reply.N = i.n_a
			reply.V_A = i.v_a
		} else {
			reply.OK = false
		}
	}
	px.mu.Unlock()
	return nil
}


func (px * Paxos) acceptanceCount(instance int, value interface{}, validProposal int, done int) int {
	nAccepted := 0

	for i, p := range px.peers {
		// SELF ACCEPTANCE NO RPC REQUIRED
		if i == px.me {
			reply := &AcceptReply{}
			if px.AcceptRPC(&AcceptArgs{instance, validProposal, value, px.me, done}, reply) == nil {
				if reply.OK {
					px.mu.Lock()
					nAccepted++
					px.mu.Unlock()
					px.setDone(reply.From, reply.Done)
				}
			}
		// ACCEPTANCE FOR PEERS
		} else {
			if ok, r := px.sendAccept(instance, p, validProposal, value); ok {
				px.setDone(r.From, r.Done)
				if r.OK {
					px.mu.Lock()
					nAccepted++
					px.mu.Unlock()
				}
			}
		}
	}

	return nAccepted

}

// **************************************************
// ******************* ACCEPTANCE *******************
// **************************************************

// **************************************************
// ******************* DECISION *********************
// **************************************************

func (px *Paxos) sendDecide(instance int, dest string, val interface{}) (bool, *DecideReply) {
	var reply DecideReply
	ok := call(dest, "Paxos.DecideRPC", &DecideArgs{instance, val}, &reply)
	return ok, &reply
}

func (px *Paxos) DecideRPC(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	i := px.instances[args.Instance]
	if i != nil {
		i.v_a = args.Value
		i.status = Fate(1)
		reply.OK = true
	} else {
		reply.OK = false
	}
	px.mu.Unlock()
	return nil
}

func (px *Paxos) decisionCount(instance int, value interface{}) int {
	nDecided := 0
		
	for i, p := range px.peers {
		// SELF DECISION NO RPC REQUIRED
		if i == px.me {
			reply := &DecideReply{}
			if px.DecideRPC(&DecideArgs{instance, value}, reply) == nil && reply.OK {
				px.mu.Lock()
				nDecided++
				px.mu.Unlock()
			}
		// DECISION FOR PEERS	
		} else {
			ok, r := px.sendDecide(instance, p, value)
			if ok && r.OK {
				px.mu.Lock()
				nDecided++
				px.mu.Unlock()
			}
		}
	}

	return nDecided
}

// **************************************************
// ******************* DECISION *********************
// **************************************************

func (px *Paxos) generateValidProposal(instance int) int {
	px.mu.Lock()
	validProposal := instance + px.me
	px.mu.Unlock()

	for h := px.highestProposal(instance); validProposal <= h; {
		px.mu.Lock()
		instance++
		validProposal = instance + px.me
		px.mu.Unlock()
	}
	return validProposal
}

func (px *Paxos) chooseHighest(r []*PrepareReply, value interface{}) interface{} {
	max_id := -1
	var max_value interface{}

	for _, v := range r {
		if v.OK && v.N_A > max_id {
			max_id = v.N_A
			max_value = v.V_A
		}
	}
	
	if max_value == nil {
		max_value = value
	}
	return max_value
}

func (px *Paxos) highestProposal(instance int) int {
	result := -1
	px.mu.Lock()
	if px.instances[instance] != nil {
		if result = px.instances[instance].n_p; px.instances[instance].n_p > px.instances[instance].n_p_consensus {
			result = px.instances[instance].n_p_consensus
		}
	}
	px.mu.Unlock()
	return result
}

func (px *Paxos) highestConsensus(instance int, n_p int) bool {
	success := false
	px.mu.Lock()
	if px.instances[instance] != nil && n_p > px.instances[instance].n_p_consensus {
		px.instances[instance].n_p_consensus = n_p
		success = true
	}
	px.mu.Unlock()
	return success
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	if seq >= px.Min() {
		// call(strconv.Itoa(px.me), "Paxos.Proposer", &ProposeArgs{seq, v}, &ProposeReply{})
		go px.proposer(seq, v)
	}
}

// VARIATION OF DONE TO PREVENT FORGETTING
func (px *Paxos) setDone(peer int, instance int) {
	px.mu.Lock()
	px.done[peer] = instance
	px.mu.Unlock()
	min := px.Min()
	keys := px.getKeys(px.instances)
	for _, k := range keys {
		if k < min {
			px.mu.Lock()
			delete(px.instances, k)
			px.mu.Unlock()
		}
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
// ONLY USED FOR TESTS AND FROM KVPAXOS
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	px.done[px.me] = seq
	px.mu.Unlock()
}

// https://stackoverflow.com/questions/21362950/golang-getting-a-slice-of-keys-from-a-map
func (px *Paxos) getKeys(instances map[int]*instance) []int {
	px.mu.Lock()
	keys := make([]int, len(instances))
	for key, _ := range instances {
		keys = append(keys, key)
	}
	px.mu.Unlock()
	return keys
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	keys := px.getKeys(px.instances)
	
	max := -1
	for i := range keys {
		if keys[i] > max {
			max = keys[i]
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {		
	if len(px.done) < len(px.peers) {
		return 0
	}
	// TO PREVENT CONCURRENT MAP READS
	px.mu.Lock()
	min := px.done[0]
	for _, instance := range px.done {
		if instance < min {
			min = instance
		}
	}
	px.mu.Unlock()
	// TO PREVENT CONCURRENT MAP READS
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	status := Fate(3)
	var value interface{} = nil
	// TO PREVENT CONCURRENT MAP READS
	px.mu.Lock()
	if px.instances[seq] != nil {
		status = px.instances[seq].status
		value = px.instances[seq].v_a
	}
	px.mu.Unlock()
	// TO PREVENT CONCURRENT MAP READS
	return status, value
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	MinMajority = len(px.peers)/2 + 1

	// Your initialization code here.
	px.instances = make(map[int]*instance)
	px.done = make(map[int]int)	

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
