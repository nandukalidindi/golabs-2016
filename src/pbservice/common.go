package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Append bool
	Number int64
	Name string
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
	OldAppend string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Number int64
	Name string
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type SyncArgs struct {
	Primary string
	Store map[string]string
	RpcRequestMap map[string]string
}


type SyncReply struct {
	Err Err
}

type SplitBrainCheckArgs struct {
	Key string
	Value string
	Number int64
	Name string
	OldAppend string
}

type SplitBrainCheckReply struct {
	Err Err
}