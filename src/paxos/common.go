package paxos

var MinMajority int = 0

type PrepareArgs struct {
	Instance  int
	N_P  int
	From int
	Done int
}

type PrepareReply struct {
	OK   bool
	N_P  int
	N_A  int
	V_A  interface{}
	
	From int
	Done int
}

type ProposeArgs struct {
	Instance int
	Value interface{}
}

type ProposeReply struct {
	OK bool
}

type AcceptArgs struct {
	Instance  int
	N_A  int
	V_A  interface{}

	From int
	Done int
}

type AcceptReply struct {
	OK   bool
	N    int
	V_A  interface{}

	From int
	Done int
}

type DecideArgs struct {
	Instance int
	Value   interface{}
}

type DecideReply struct {
	OK bool
}