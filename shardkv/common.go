package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrNotReady    = "ErrNotReady"
	ErrOld    = "ErrOld"
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

type PushShardArgs struct {
	ShardIds []int
	Shards   []Shard
	ConfigNum int
	// ClientSeq map[int64]int
	Gid int
	LeaderId int
}

type PushShardReply struct {
	Err Err
	LeaderId int
}

type GetShardArgs struct {
	ShardId int
	ConfigNum int
	Gid int
	LeaderId int
}

type GetShardReply struct {
	Err Err
	LeaderId int
}
