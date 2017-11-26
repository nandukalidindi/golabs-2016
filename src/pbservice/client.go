package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "crypto/rand"
import "math/big"
import "time"


type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	view  *viewservice.View
	me string
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	// TO TRACK THE NAME OF THE CURRENT SERVER THE CLIENT IS TRYING TO TALK TO
	ck.me = me
	return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (ck *Clerk) getPrimary(cache bool) string {
	// FOR PREVENTING REDUNDANT RPCs TO VIEW SERVER
	// IF THE CACHE FLAG IS FALSE THEN GET THE CURRENT PRIMARY
	if ck.view == nil || !cache {
		view, ok := ck.vs.Get()
		if ok {
			ck.view = &view
			return view.Primary
		} else { ck.view = nil }
	} else {
		if ck.view != nil {
			return ck.view.Primary
		}
	}

	return ""
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	// SEND RPC TO EXECUTE PBServer.Get on BEHALF OF PRIMARY
	var reply GetReply 
	args := &GetArgs{key, nrand(), ck.me}

	cache := true

	for {
		primary := ck.getPrimary(cache)
		ok := call(primary, "PBServer.Get", args, &reply)

		if !ok || reply.Err == ErrWrongServer {
			cache = false			
		} else {
			if reply.Err == ErrNoKey {
				return ""
			}
			return reply.Value			
		}

		time.Sleep(viewservice.PingInterval)
	}
	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	// SEND RPC TO EXECUTE PBServer.PutAppend on BEHALF OF PRIMARY 
	// (PUT OR APPEND IS DECIDED BY THE op PARAMETER)
	var reply PutAppendReply
	shouldAppend := op == "Append"
	args := &PutAppendArgs{key, value, shouldAppend, nrand(), ck.me}

	cache := true
	for {
		primary := ck.getPrimary(cache)
		ok := call(primary, "PBServer.PutAppend", args, &reply)

		if !ok || reply.Err != "" {	
			reply.Err = ""		
			cache = false
		} else {
			break
		}
		// EVERY PING INTERVAL TO PREVENT A BLAST OF RPCs
		time.Sleep(viewservice.PingInterval)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
