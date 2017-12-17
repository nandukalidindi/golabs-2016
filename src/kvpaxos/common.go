package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	From string
    Instance int
}

type PutAppendReply struct {
	Err string
	OldAppend string 
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	From string
    Instance int
}

type GetReply struct {
	Err   string
	Value string
}
