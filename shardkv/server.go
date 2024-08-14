package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
	// "github.com/sasha-s/go-deadlock"
)

// import "fmt"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key            string
	Value          string
	Op             string // "Put" or "Append"
	ClientId       int
	SequenceNumber int
	ShardIds       []int
	ConfigNum      int
	Shards         []Shard
	Config         shardctrler.Config
	ShardAdd       []int
	ShardDelete    []int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	NotifyCh    map[int]chan bool
	lastApplied int
	shards      map[int]Shard
	persister   *raft.Persister
	dead        int32
	config      shardctrler.Config

	mck         *shardctrler.Clerk
	configTimer *time.Timer
	// configState  int
	waitCond    *sync.Cond
	addCh       chan int
	deleteCh    chan int
	nextConfig  shardctrler.Config
	lastLeaders map[int]int
	shardAdd    []int
	shardDelete []int
}

type Shard struct {
	Data       map[string]string
	SessionMap map[int]int
}

// const (
// 	latest   = 0
// 	updating = 1
// )

const timeout = 100 * time.Millisecond
const killTimeout = 1000 * time.Millisecond

// const configtimeout = 30 * time.Millisecond

func (kv *ShardKV) getGidsByShards(config shardctrler.Config, shards []int) map[int][]int {
	gids2shards := make(map[int][]int)
	for _, shardId := range shards {
		gids2shards[config.Shards[shardId]] = append(gids2shards[config.Shards[shardId]], shardId)
	}
	return gids2shards
}

func (kv *ShardKV) waitRaft(op Op) bool {
	kv.mu.Lock()
	DPrintf("gid: %v me: %v configNum: %v before waitRaft: %v", kv.gid, kv.me, kv.config.Num, op)
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)
	kv.mu.Lock()
	DPrintf("gid: %v me: %v isleader: %v configNum: %v after waitRaft: %v", kv.gid, kv.me, isLeader, kv.config.Num, op)
	kv.mu.Unlock()
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	DPrintf("gid: %v me: %v configNum: %v waitRaft: %v index: %v", kv.gid, kv.me, kv.config.Num, op, index)
	if kv.NotifyCh[index] == nil {
		kv.NotifyCh[index] = make(chan bool, 1)
	}
	ch := kv.NotifyCh[index]
	kv.mu.Unlock()
	select {
	case <-time.After(timeout):
		kv.mu.Lock()
		delete(kv.NotifyCh, op.ClientId)
		kv.mu.Unlock()
		return false
	case ok := <-ch:
		kv.mu.Lock()
		delete(kv.NotifyCh, op.ClientId)
		kv.mu.Unlock()
		return ok
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	// fmt.Println(kv.gid, kv.config)
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Key:            args.Key,
		Value:          "",
		Op:             "Get",
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
	}
	if kv.waitRaft(op) {
		kv.mu.Lock()
		// if !kv.checkShard(args.Key) {
		// 	reply.Err = ErrWrongGroup
		// 	kv.mu.Unlock()
		// 	return
		// }
		reply.Value = kv.shards[key2shard(args.Key)].Data[args.Key]
		DPrintf("gid %v me %v get: %v %v", kv.gid, kv.me, args.Key, reply.Value)
		kv.mu.Unlock()
		reply.Err = OK
	} else {
		kv.mu.Lock()
		if !kv.checkShard(args.Key) {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = ErrWrongLeader
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.checkShard(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{
		Key:            args.Key,
		Value:          args.Value,
		Op:             args.Op,
		ClientId:       args.ClientId,
		SequenceNumber: args.SequenceNumber,
	}
	if kv.waitRaft(op) {
		reply.Err = OK
		// fmt.Println(args.Op, args.Key, args.Value)
	} else {
		kv.mu.Lock()
		if !kv.checkShard(args.Key) {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = ErrWrongLeader
		}
		kv.mu.Unlock()
	}
}

// func (kv *ShardKV) pullShard(shard int, config shardctrler.Config) {
// 	args := PullShardArgs{
// 		ConfigNum: kv.config.Num,
// 	}
// 	reply := PullShardReply{}
// 	for i := 0; i < len(config.Groups[config.Shards[shard]]); i++ {
// 		srv := kv.make_end(config.Groups[config.Shards[shard]][i])
// 		ok := srv.Call("ShardKV.PullShard", &args, &reply)
// 		if ok && reply.Err == OK {
// 			break
// 		}
// 	}
// }

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum <= kv.config.Num {
		reply.Err = ErrOld
		DPrintf("gid %v get old shard %v config: %v", kv.gid, args.ShardId, kv.config)
	} else if kv.config.Shards[args.ShardId] != kv.gid {
		reply.Err = ErrWrongGroup
		DPrintf("gid %v get wrong shard %v config: %v", kv.gid, args.ShardId, kv.config)
	} else if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
	} else {
		kv.lastLeaders[args.Gid] = args.LeaderId
		reply.Err = OK
		if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
			kv.configTimer.Reset(0)
		}
	}
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()
	if args.ConfigNum <= kv.config.Num {
		reply.Err = OK
		// fmt.Println("too old")
		kv.mu.Unlock()
		return
	}
	if kv.killed() {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// } else if args.ConfigNum > kv.config.Num + 1 {
	// 	reply.Err = ErrNotReady
	// 	if false{
	// 		fmt.Println("too old")
	// 	}
	// 	DPrintf("gid %v too old: %v new %v", kv.gid, args.ConfigNum, kv.config.Num)
	// 	kv.configTimer.Reset(0)
	// 	kv.mu.Unlock()
	// 	return
	// }
	reply.Err = OK
	if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
		kv.configTimer.Reset(0)
	}
	for (len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0) || args.ConfigNum > kv.config.Num+1 {
		// fmt.Println("wait")
		kv.waitCond.Wait()
		// fmt.Println("wait end")
		// if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
		// fmt.Println(kv.configState, args.ConfigNum, kv.config.Num)
		// }
		// DPrintf("gid: %v me: %v wake up shardAdd: %v shardDelete: %v args.ConfigNum: %v kv.config.Num: %b", kv.gid, kv.me, kv.shardAdd, kv.shardDelete, args.ConfigNum, kv.config.Num+1)
		if kv.killed() {
			reply.Err = ErrWrongLeader
			kv.mu.Unlock()
			return
		}
	}
	isContain := false
	for _, shardId := range args.ShardIds {
		if Contains(kv.shardAdd, shardId) {
			isContain = true
			break
		}
	}
	if !isContain || args.ConfigNum <= kv.config.Num {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.lastLeaders[args.Gid] = args.LeaderId
	op := Op{
		Op:        "AddShard",
		ShardIds:  args.ShardIds,
		ConfigNum: args.ConfigNum,
		Shards:    args.Shards,
	}
	DPrintf("ConfigNum: %v %v get shard %v from %v data: %v", args.ConfigNum, kv.gid, args.ShardIds, args.Gid, args.Shards)
	kv.mu.Unlock()
	ok := kv.waitRaft(op)
	if ok {
		reply.Err = OK
		return
	}
	kv.mu.Lock()
	isContain = false
	for _, shardId := range args.ShardIds {
		if Contains(kv.shardAdd, shardId) {
			isContain = true
			break
		}
	}
	if !isContain || args.ConfigNum <= kv.config.Num {
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		select {
		case <-time.After(killTimeout):
			continue
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			_, isLeader := kv.rf.GetState()
			DPrintf("raw msg: gid %v me: %v isleader: %v msg %v ", kv.gid, kv.me, isLeader, msg)
			if msg.CommandValid {
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				if msg.CommandIndex > kv.lastApplied+1 {
					DPrintf("gid: %v me: %v index error: %v %v", kv.gid, kv.me, msg.CommandIndex, kv.lastApplied)
				}
				op := msg.Command.(Op)
				if op.Op == "Get" || op.Op == "Append" || op.Op == "Put" {
					shardId := key2shard(op.Key)
					if !kv.checkShard(op.Key) {
						kv.lastApplied = msg.CommandIndex
						kv.mu.Unlock()
						continue
					}
					if _, ok := kv.shards[shardId]; !ok {
						kv.shards[shardId] = Shard{
							Data:       make(map[string]string),
							SessionMap: make(map[int]int),
						}
					}
					if kv.shards[shardId].SessionMap[op.ClientId] < op.SequenceNumber {
						// fmt.Println("apply: ", op)
						DPrintf("msg: gid %v me: %v msg content %v %v", kv.gid, kv.me, msg, kv.shards)
						if op.Op == "Put" {
							kv.shards[shardId].Data[op.Key] = op.Value
						} else if op.Op == "Append" {
							kv.shards[shardId].Data[op.Key] += op.Value
						}
					}

					kv.shards[shardId].SessionMap[op.ClientId] = op.SequenceNumber
					// kv.checkSnapShot(msg.CommandIndex)
				} else if op.Op == "WaitShard" {
					if op.Config.Num == kv.config.Num+1 && len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
						if kv.nextConfig.Num == kv.config.Num+1 {
							DPrintf("nexttttttttttttttttt error")
						}
						kv.nextConfig = op.Config
						// if kv.configState != latest {
						// 	DPrintf("waittttttttttttttttt error")
						// }
						// kv.configState = waiting
						kv.shardAdd = CopySlice(op.ShardAdd)
						kv.shardDelete = CopySlice(op.ShardDelete)
						kv.waitCond.Broadcast()
						DPrintf("gid %v start waiting configNum: %v shardAdd: %v shardDelete: %v", kv.gid, kv.nextConfig.Num, kv.shardAdd, kv.shardDelete)
					}
				} else if op.Op == "AddShard" {
					if op.ConfigNum == kv.config.Num+1 {
						if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
							DPrintf("add error")
						}
						// DPrintf("before gid %v add shard %v data: %v", kv.gid, op.Shard, kv.db)
						for i, shardId := range op.ShardIds {
							if Contains(kv.shardAdd, shardId) {

								kv.shards[shardId] = Shard{
									Data:       CopyStringMap(op.Shards[i].Data),
									SessionMap: CopyIntMap(op.Shards[i].SessionMap),
								}

								kv.shardAdd = Delete(kv.shardAdd, shardId)
								// DPrintf("after gid %v add shard %v data: %v", kv.gid, op.Shard, op.Data)
								DPrintf("gid %v add shard %v data: %v remain: %v", kv.gid, op.ShardIds, op.Shards, kv.shardAdd)
								if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
									kv.config = kv.nextConfig
									DPrintf("gid %v new config: %v data: %v", kv.gid, kv.config, kv.shards)
									// kv.configState = latest
								} else {
									DPrintf("gid %v Already add: %v shardAdd: %v", kv.gid, op.ShardIds, kv.shardAdd)
								}
							}
						}
					} else {
						// fmt.Println("add error", op.Config.Num, kv.config.Num)
					}

				} else if op.Op == "DeleteShard" {
					if op.ConfigNum == kv.config.Num+1 {
						for _, shardId := range op.ShardIds {
							kv.shardDelete = Delete(kv.shardDelete, shardId)
							kv.shards[shardId] = Shard{}
						}
						DPrintf("gid %v delete shard %v remain: %v", kv.gid, op.ShardIds, kv.shardDelete)
						if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
							kv.config = kv.nextConfig
							DPrintf("gid %v new config: %v data: %v", kv.gid, kv.config, kv.shards)
							// kv.configState = latest
						}
					}

				} else if op.Op == "UpdateConfig" {
					if op.Config.Num == kv.config.Num+1 {
						// fmt.Println("update config")
						// fmt.Println("old config: ", kv.config)
						DPrintf("gid %v new config: %v data: %v", kv.gid, op.Config, kv.shards)
						kv.config = op.Config
						if op.Config.Num != kv.nextConfig.Num && len(kv.shardAdd) != 0 || len(kv.shardDelete) != 0 {
							DPrintf("config error")
						}
						if len(kv.shardAdd) != 0 || len(kv.shardDelete) != 0 {
							// fmt.Println("config state error")
						}
						// kv.configState = latest
					} else {
						// fmt.Println("update error")
					}
				}
				kv.lastApplied = msg.CommandIndex
				kv.checkSnapShot(msg.CommandIndex)
				if ch, ok := kv.NotifyCh[msg.CommandIndex]; ok {
					select {
					case ch <- true:
					default:
					}
				}

			} else if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.readSnapshot()
					if len(kv.shardAdd) > 0 || len(kv.shardDelete) > 0 {
						kv.waitCond.Broadcast()
					}
					DPrintf("gid %v me %v install snapshot: %v lastApplied: %v snapshotIndex: %v", kv.gid, kv.me, kv.shards, kv.lastApplied, msg.SnapshotIndex)
					kv.lastApplied = msg.SnapshotIndex
				}
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) getLatestConfig() {
	for !kv.killed() {
		select {
		case <-time.After(killTimeout):
			continue
		case <-kv.configTimer.C:
		}
		kv.configTimer.Reset(80 * time.Millisecond)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			DPrintf("gid %v me %v not leader", kv.gid, kv.me)
			continue
		}
		kv.mu.Lock()
		// if kv.configState == updating {
		// 	kv.mu.Unlock()
		// 	continue
		// }
		// fmt.Println(kv.gid, kv.configState)
		shardAdd := CopySlice(kv.shardAdd)
		shardDelete := CopySlice(kv.shardDelete)
		config := kv.nextConfig
		DPrintf("gid %v start get config: %v old config: %v shardAdd: %v shardDelete: %v", kv.gid, config, kv.config, shardAdd, shardDelete)
		if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
			// kv.mu.Unlock()
			// continue
			configNum := kv.config.Num + 1
			kv.mu.Unlock()
			for {
				config = kv.mck.Query(configNum)
				if kv.killed() {
					return
				}
				if config.Num != -1 {
					break
				}
			}
			DPrintf("gid %v get new config: %v", kv.gid, config)
			kv.mu.Lock()
			if config.Num != kv.config.Num+1 || len(kv.shardAdd) != 0 || len(kv.shardDelete) != 0 {
				kv.mu.Unlock()
				continue
			}
			// kv.configState = updating
			// shardAdd := make([]int, 0)
			// shardDelete := make([]int, 0)
			for i := 0; i < shardctrler.NShards; i++ {
				if config.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid && kv.config.Shards[i] != 0 {
					shardAdd = append(shardAdd, i)
				} else if config.Shards[i] != kv.gid && kv.config.Shards[i] == kv.gid && config.Shards[i] != 0 {
					shardDelete = append(shardDelete, i)
				}
			}
			if len(shardAdd) == 0 && len(shardDelete) == 0 {
				op := Op{
					Op:     "UpdateConfig",
					Config: config,
				}
				// fmt.Println("no change")
				kv.mu.Unlock()
				kv.waitRaft(op)
				continue
			}
			// kv.configState = updating
			DPrintf("configNum: %v gid %v add %v", kv.config.Num, kv.gid, shardAdd)
			DPrintf("configNum: %v gid %v delete %v", kv.config.Num, kv.gid, shardDelete)
			go func() {
				op := Op{
					Op:          "WaitShard",
					Config:      config,
					ShardAdd:    shardAdd,
					ShardDelete: shardDelete,
				}
				kv.waitRaft(op)
			}()
		}
		var wg sync.WaitGroup
		// gid2AddShardIds := kv.getGidsByShards(kv.config, shardAdd)
		// for gid, shardIds := range gid2AddShardIds {
		// 	wg.Add(1)
		// 	lastLeader := kv.lastLeaders[gid]
		// 	servers := kv.config.Groups[gid]
		// 	go func(shardId int) {
		// 		defer wg.Done()
		// 		if gid == kv.gid {
		// 			DPrintf("wwwwwwwwwwwwwwwrong gid")
		// 		}
		// 		for si := 0; si < len(servers); si++ {
		// 			server := servers[(si + lastLeader) % len(servers)]
		// 			srv := kv.make_end(server)
		// 			args := GetShardArgs{
		// 				ShardId: shardId,
		// 				ConfigNum: config.Num,
		// 				LeaderId: kv.me,
		// 				Gid: kv.gid,
		// 			}
		// 			reply := GetShardReply{}
		// 			ok := srv.Call("ShardKV.GetShard", &args, &reply)
		// 			if ok {
		// 				if reply.Err == OK {
		// 					return
		// 				}
		// 				if reply.Err == ErrWrongGroup {
		// 					DPrintf("wrong group")
		// 					return
		// 				}
		// 				if reply.Err == ErrOld {
		// 					DPrintf("Old")
		// 					return
		// 				}

		// 			}
		// 		}
		// 	}(shardIds[0])
		// }
		gid2DeleteShardIds := kv.getGidsByShards(config, shardDelete)
		// fmt.Println("delete: ", kv.gid, gid2DeleteShardIds, shardDelete, config)
		for gid, shardIds := range gid2DeleteShardIds {
			wg.Add(1)
			go func(gid int, shardIds []int) {
				defer wg.Done()
				servers := config.Groups[gid]
				kv.mu.Lock()
				if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
					kv.waitCond.Wait()
					if len(kv.shardAdd) == 0 && len(kv.shardDelete) == 0 {
						DPrintf("wait error")
					}
				}

				lastLeader := kv.lastLeaders[gid]
				shards := make([]Shard, len(shardIds))
				for i, shardId := range shardIds {
					shards[i] = Shard{
						Data:       CopyStringMap(kv.shards[shardId].Data),
						SessionMap: CopyIntMap(kv.shards[shardId].SessionMap),
					}
				}
				kv.mu.Unlock()
				for kv.killed() == false {
					if _, isLeader := kv.rf.GetState(); !isLeader {
						return
					}
					for si := 0; si < len(servers); si++ {
						server := servers[(si+lastLeader)%len(servers)]
						srv := kv.make_end(server)
						args := PushShardArgs{
							ShardIds:  shardIds,
							Shards:    shards,
							ConfigNum: config.Num,
							LeaderId:  kv.me,
							Gid:       kv.gid,
						}
						reply := PushShardReply{}
						ok := srv.Call("ShardKV.PushShard", &args, &reply)
						if ok {
							if reply.Err == OK {
								// fmt.Println("reply: ", reply, shardIds, kv.gid, gid, config.Num)
								op := Op{
									Op:        "DeleteShard",
									ShardIds:  shardIds,
									ConfigNum: config.Num,
								}
								ok := kv.waitRaft(op)
								DPrintf("gid: %v me: %v delete shard %v ok %v", kv.gid, kv.me, shardIds, ok)
								// kv.deleteCh <- shard
								return
							}
							if reply.Err == ErrWrongGroup {
								DPrintf("gid: %v me: %v delete wrong group", kv.gid, kv.me)
								return
							}
						}
						DPrintf("gid %v send gid %v server %v shard %v wrong leader", kv.gid, gid, si, shardIds)
					}
				}
			}(gid, shardIds)
		}
		kv.mu.Unlock()
		wg.Wait()
	}
	DPrintf("%v %v end", kv.gid, kv.me)
}

func (kv *ShardKV) checkShard(key string) bool {
	shardId := key2shard(key)
	// fmt.Println("shard: ", kv.gid, kv.config.Shards[shard])
	DPrintf("gid: %v me: %v key %v shard %v shardAdd: %v shardDelete: %v config: %v next: %v", kv.gid, kv.me, key, shardId, kv.shardAdd, kv.shardDelete, kv.config, kv.nextConfig)
	if len(kv.shardAdd) != 0 || len(kv.shardDelete) != 0 {
		if kv.nextConfig.Shards[shardId] == kv.gid && !Contains(kv.shardAdd, shardId) {
			return true
			// } else if kv.config.Shards[shard] == kv.gid && Contains(kv.shardDelete, shard) {
			// 	return true
		} else if kv.nextConfig.Shards[shardId] == kv.gid && kv.config.Shards[shardId] == kv.gid {
			return true
		} else {
			return false
		}
	}
	return kv.gid == kv.config.Shards[shardId]
}

func (kv *ShardKV) checkSnapShot(index int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate {
		// DPrintf("Server %v do snapshot db: %v", kv.me, kv.db)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.shards)
		e.Encode(kv.lastApplied)
		e.Encode(kv.config)
		e.Encode(kv.nextConfig)
		e.Encode(kv.shardAdd)
		e.Encode(kv.shardDelete)
		// e.Encode(kv.configState)
		// e.Encode(kv.nextConfig)
		if index != kv.lastApplied {
			DPrintf("gid: %v me: %v index apply error: %v %v", kv.gid, kv.me, index, kv.lastApplied)
		}
		DPrintf("gid: %v Server %v do snapshot db: %v index: %v lastApplied: %v", kv.gid, kv.me, kv.shards, index, kv.lastApplied)
		kv.rf.Snapshot(index, w.Bytes())
	}
}

func (kv *ShardKV) readSnapshot() {
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// DPrintf("Server %v before read db: %v\n", kv.me, kv.db)
	var shards map[int]Shard
	if d.Decode(&shards) != nil {
		DPrintf("error read db")
		return
	}
	kv.shards = shards
	d.Decode(&kv.lastApplied)
	d.Decode(&kv.config)
	d.Decode(&kv.nextConfig)
	d.Decode(&kv.shardAdd)
	d.Decode(&kv.shardDelete)
	// d.Decode(&kv.configState)
	// DPrintf("from snapchot: %v", kv.config)
	// d.Decode(&)
	// DPrintf("Server %v after read db: %v\n", kv.me, kv.db)

}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	kv.mu.Lock()
	kv.waitCond.Broadcast()
	kv.mu.Unlock()

}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//

var once sync.Once

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	// kv.maxraftstate = -1
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shards = make(map[int]Shard)
	kv.NotifyCh = make(map[int]chan bool)
	kv.persister = persister
	kv.lastApplied = 0

	// fmt.Println(kv.me)
	kv.configTimer = time.NewTimer(100 * time.Millisecond)

	// kv.configState = latest
	kv.waitCond = sync.NewCond(&kv.mu)
	kv.addCh = make(chan int)
	kv.deleteCh = make(chan int)
	kv.nextConfig = kv.config
	kv.lastLeaders = make(map[int]int)

	kv.readSnapshot()
	if kv.persister.RaftStateSize() == 0 {
		for {
			kv.config = kv.mck.Query(-1)
			if kv.config.Num != -1 && !kv.killed() {
				break
			}
		}
	}
	DPrintf("%v %v restart configNum: %v", kv.gid, kv.me, kv.config.Num)
	go kv.getLatestConfig()
	go kv.apply()
	once.Do(func() {
		go monitor()
	})

	return kv
}
