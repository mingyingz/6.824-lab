package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int
	SequenceNumber int
}

type PutAppendReply struct {
	Err Err
	LeaderId int
	Me int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	ClientId  int
	SequenceNumber int
}

type GetReply struct {
	Err   Err
	Value string
}
