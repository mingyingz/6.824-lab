package shardctrler

import (
	"sort"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/sasha-s/go-deadlock"
)

// import "sync"

const timeout = 200 * time.Millisecond

type ShardCtrler struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs     []Config // indexed by config num
	NotifyCh    map[int]chan bool
	lastApplied int
	sessionMap  map[int]int
	dead        int32
	killCh      chan bool
}

type Op struct {
	// Your data here.
	Op             string // "Join", "Leave", "Move", "Query"
	ClientId       int
	SequenceNumber int
	// Join
	Servers map[int][]string
	// Leave
	GIDs []int
	// Move
	Shard int
	GID   int
	// Query
	Num int
}

func (sc *ShardCtrler) waitRaft(op Op) bool {
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return false
	}
	// fmt.Println("waitRaft: ", op)
	sc.mu.Lock()
	if sc.NotifyCh[index] == nil {
		sc.NotifyCh[index] = make(chan bool, 1)
	}
	ch := sc.NotifyCh[index]
	sc.mu.Unlock()
	select {
	case <-time.After(timeout):
		sc.mu.Lock()
		delete(sc.NotifyCh, op.ClientId)
		sc.mu.Unlock()
		return false
	case ok := <-ch:
		sc.mu.Lock()
		delete(sc.NotifyCh, op.ClientId)
		sc.mu.Unlock()
		return ok
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Op:             "Join",
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
		Servers:        args.Servers,
	}
	if !sc.waitRaft(op) {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	op := Op{
		Op:             "Leave",
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
		GIDs:           args.GIDs,
	}
	if !sc.waitRaft(op) {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	op := Op{
		Op:             "Move",
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
		Shard:          args.Shard,
		GID:            args.GID,
	}
	if !sc.waitRaft(op) {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	op := Op{
		Op:             "Query",
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
		Num:            args.Num,
	}
	if !sc.waitRaft(op) {
		reply.WrongLeader = true
		return
	} else {
		reply.WrongLeader = false
		reply.Err = OK
		sc.mu.Lock()
		if args.Num >= len(sc.configs) || args.Num == -1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		// fmt.Println("Query: ", reply.Config)
		sc.mu.Unlock()

		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
	sc.killCh <- true

}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) getMaxMinGid(gidNum map[int]int, gids []int) (int, int) {
	maxGid, minGid := 0, 0
	maxCount, minCount := 0, NShards
	for _, gid := range gids {
		count := gidNum[gid]
		if count >= maxCount {
			maxCount = count
			maxGid = gid
		}
		if count <= minCount {
			minCount = count
			minGid = gid
		}
	}
	return maxGid, minGid
}

func (sc *ShardCtrler) join(servers map[int][]string) {
	sc.configs = append(sc.configs, Config{})
	sc.configs[len(sc.configs)-1].Num = len(sc.configs) - 1
	sc.configs[len(sc.configs)-1].Groups = make(map[int][]string)
	gids := make([]int, 0)
	gidNum := make(map[int]int)

	// 更新新config的Groups
	for gid, server := range servers {
		sc.configs[len(sc.configs)-1].Groups[gid] = server
		gids = append(gids, gid)
		gidNum[gid] = 0
	}
	for gid, server := range sc.configs[len(sc.configs)-2].Groups {
		sc.configs[len(sc.configs)-1].Groups[gid] = server
		gids = append(gids, gid)
	}
	// group_num := len(sc.configs[len(sc.configs) - 1].Groups)
	sort.Ints(gids)
	// fmt.Println(gids)

	//初始化新config的Shards
	for i := 0; i < NShards; i++ {
		if sc.configs[len(sc.configs)-2].Shards[i] != 0 {
			sc.configs[len(sc.configs)-1].Shards[i] = sc.configs[len(sc.configs)-2].Shards[i]
			gidNum[sc.configs[len(sc.configs)-1].Shards[i]]++
		} else {
			sc.configs[len(sc.configs)-1].Shards[i] = gids[i%len(gids)]
			gidNum[gids[i%len(gids)]]++
			// if len(gids) != 1 {
			// 	fmt.Println("error", gids, )
			// }
		}
	}
	// fmt.Println(sc.configs[len(sc.configs)-1], gidNum, servers, gids)
	for {
		maxGid, minGid := sc.getMaxMinGid(gidNum, gids)
		if gidNum[maxGid]-gidNum[minGid] <= 1 {
			break
		}
		for i := 0; i < NShards; i++ {
			if sc.configs[len(sc.configs)-1].Shards[i] == maxGid {
				sc.configs[len(sc.configs)-1].Shards[i] = minGid
				gidNum[maxGid]--
				gidNum[minGid]++
				break
			}
		}
	}
	// fmt.Println(sc.configs[len(sc.configs)-1])
}

func (sc *ShardCtrler) leave(gids []int) {
	sc.configs = append(sc.configs, Config{})
	sc.configs[len(sc.configs)-1].Num = len(sc.configs) - 1
	sc.configs[len(sc.configs)-1].Groups = make(map[int][]string)
	gidsLeave := make(map[int]bool)
	for _, gid := range gids {
		gidsLeave[gid] = true
	}
	gidsKeep := make([]int, 0)
	for gid, server := range sc.configs[len(sc.configs)-2].Groups {
		if _, ok := gidsLeave[gid]; !ok {
			sc.configs[len(sc.configs)-1].Groups[gid] = server
			gidsKeep = append(gidsKeep, gid)
		}
	}
	// fmt.Println(gids_new)
	group_num := len(sc.configs[len(sc.configs)-1].Groups)
	sort.Ints(gidsKeep)
	for i := 0; i < NShards; i++ {
		if group_num == 0 {
			sc.configs[len(sc.configs)-1].Shards[i] = 0
			break
		} else if _, ok := gidsLeave[sc.configs[len(sc.configs)-1].Shards[i]]; !ok {
			sc.configs[len(sc.configs)-1].Shards[i] = gidsKeep[i%group_num]
		} else {
			sc.configs[len(sc.configs)-1].Shards[i] = 0
		}
	}
	gidNum := make(map[int]int)
	gidZero := make([]int, 0)
	for i := 0; i < NShards; i++ {
		if sc.configs[len(sc.configs)-1].Shards[i] != 0 {
			gidNum[sc.configs[len(sc.configs)-1].Shards[i]]++
		} else {
			gidZero = append(gidZero, i)
		}
	}
	for i := range gidZero {
		_, minGid := sc.getMaxMinGid(gidNum, gidsKeep)
		sc.configs[len(sc.configs)-1].Shards[gidZero[i]] = minGid
		gidNum[minGid]++
	}
}

func (sc *ShardCtrler) move(shard int, gid int) {

	sc.configs = append(sc.configs, Config{})
	sc.configs[len(sc.configs)-1].Num = len(sc.configs) - 1
	for i := 0; i < NShards; i++ {
		sc.configs[len(sc.configs)-1].Shards[i] = sc.configs[len(sc.configs)-2].Shards[i]
	}
	sc.configs[len(sc.configs)-1].Shards[shard] = gid
	sc.configs[len(sc.configs)-1].Groups = make(map[int][]string)
	for gid, server := range sc.configs[len(sc.configs)-2].Groups {
		sc.configs[len(sc.configs)-1].Groups[gid] = server
	}

}

func (sc *ShardCtrler) apply() {
	for !sc.killed() {
		select {
		case <-sc.killCh:
			return
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				// fmt.Printf("msg: %v\n", msg)

				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				op := msg.Command.(Op)
				if sc.sessionMap[op.ClientId] < op.SequenceNumber {
					if op.Op == "Join" {
						sc.join(op.Servers)
					} else if op.Op == "Leave" {
						sc.leave(op.GIDs)
					} else if op.Op == "Move" {
						sc.move(op.Shard, op.GID)
					} else if op.Op == "Query" {
						// do nothing
					}
					// fmt.Println(sc.db)
					sc.sessionMap[op.ClientId] = op.SequenceNumber
				}
				if ch, ok := sc.NotifyCh[msg.CommandIndex]; ok {
					select {
					case ch <- true:
					default:
					}
				}

				sc.lastApplied = msg.CommandIndex

				sc.mu.Unlock()

			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.NotifyCh = make(map[int]chan bool)
	sc.sessionMap = make(map[int]int)
	sc.lastApplied = 0
	sc.killCh = make(chan bool)

	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	go sc.apply()

	return sc
}
