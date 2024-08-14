package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	// "sync"
	"sync/atomic"
	"github.com/sasha-s/go-deadlock"
	// "fmt"
	"time"
	"bytes"
)

const Debug = false
const timeout = 200*time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int
	SequenceNumber int



}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	db      map[string]string

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	// Your definitions here.
	sessionMap map[int]int
	NotifyCh map[int]chan bool
	persister *raft.Persister
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		Key:       args.Key,
		Value:     "",
		Op:        "Get",
		ClientId:  args.ClientId,
		SequenceNumber: args.SequenceNumber,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	} 
	// fmt.Println("get: ", args.Key, args.ClientId, args.SequenceNumber)
	kv.mu.Lock()
	if kv.NotifyCh[index] == nil {
		kv.NotifyCh[index] = make(chan bool, 1)
	}
	ch := kv.NotifyCh[index]
	kv.mu.Unlock()
	select {
	case <-time.After(timeout):
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		delete(kv.NotifyCh, args.ClientId)
		kv.mu.Unlock()
		return

	case ok := <-ch:
		// DPrintf("Get: %d %d %d %d", op.SequenceNumber, args.SequenceNumber, args.ClientId, op.ClientId)
		// fmt.Printf("Get: %s %d %d %d %d\n", op.Op, op.SequenceNumber, args.SequenceNumber, args.ClientId, op.ClientId)
		if currentTerm, isLeader := kv.rf.GetState(); ok && isLeader && currentTerm == term{
		// if ok && term == term {
			kv.mu.Lock()
			reply.Err = OK
			_, ok := kv.db[args.Key]; 
			if ok {
				reply.Value = kv.db[args.Key]
			} else {
				reply.Err = ErrNoKey
			}
			// fmt.Printf("%d Get key: %s value: %s op: %s seq: %d %d client: %d %d ", kv.me, op.Key, reply, op.Op, op.SequenceNumber, args.SequenceNumber, op.ClientId, args.ClientId)
			// fmt.Println(kv.db)
			delete(kv.NotifyCh, index)
			kv.mu.Unlock()
			// fmt.Println("Get value: ", reply.Value)
		} else{
			reply.Err = ErrWrongLeader
			reply.Value = ""
		}

	}
	kv.mu.Lock()
	delete(kv.NotifyCh, index)
	kv.mu.Unlock()

}


func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		ClientId:  args.ClientId,
		SequenceNumber: args.SequenceNumber,
	}
	reply.Me = kv.me
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Server %v start %v key: %v value %v seq: %v client: %v\n", kv.me, args.Op, args.Key, args.Value, args.SequenceNumber, args.ClientId)
	// fmt.Println("put: ", args.Key, args.ClientId, args.SequenceNumber)
	// fmt.Println("PA wait: ", op.Op, op.SequenceNumber, args.SequenceNumber, args.ClientId, op.ClientId)
	kv.mu.Lock()
	if kv.NotifyCh[index] == nil {
		kv.NotifyCh[index] = make(chan bool, 1)
	}
	ch := kv.NotifyCh[index]
	kv.mu.Unlock()
	select{
	case <-time.After(timeout):
		reply.Err = ErrWrongLeader
		kv.mu.Lock()
		delete(kv.NotifyCh, index)
		kv.mu.Unlock()
		return
	case ok := <-ch:
		// op := msg.Command.(Op)
		// fmt.Printf("PA key: %s value: %s op: %s seq: %d\n", op.Key, op.Value, op.Op, op.SequenceNumber)
		// fmt.Printf("PA: %d %d %d %d %d %d\n", op.SequenceNumber, args.SequenceNumber, args.ClientId, op.ClientId, kv.sessionMap[op.ClientId], op.ClientId)
		// fmt.Println("PA get: ", op.Op, op.SequenceNumber, args.SequenceNumber, args.ClientId, op.ClientId)
		if currentTerm, isLeader := kv.rf.GetState(); ok && isLeader && currentTerm == term{
		// if ok && term == term{
			DPrintf("Server %v %v key: %v value: %v seq: %v client: %v\n", kv.me, args.Op, op.Key, op.Value, op.SequenceNumber, op.ClientId)
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
		kv.mu.Lock()
		delete(kv.NotifyCh, index)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) apply() {
	for kv.killed() == false {
		select{
		case msg := <-kv.applyCh:
			if kv.killed() {
				return
			}
			if msg.CommandValid {
				kv.mu.Lock()
				// fmt.Printf("msg: %v\n", msg)

				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				op := msg.Command.(Op)
				if kv.sessionMap[op.ClientId] < op.SequenceNumber {
					if op.Op == "Put" {
						kv.db[op.Key] = op.Value
					} else if op.Op == "Append" {
						kv.db[op.Key] += op.Value
					}
					// fmt.Println(kv.db)
					kv.sessionMap[op.ClientId] = op.SequenceNumber
					kv.checkSnapShot(msg.CommandIndex)
				}
				if ch, ok := kv.NotifyCh[msg.CommandIndex]; ok {
					select{
					case ch <- true:
					default:
					}
				}

				kv.lastApplied = msg.CommandIndex

				kv.mu.Unlock()



			} else if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot){
					kv.readSnapshot()
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) checkSnapShot(index int){
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() > kv.maxraftstate {
		DPrintf("Server %v do snapshot db: %v", kv.me, kv.db)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.db)
		e.Encode(kv.sessionMap)
		kv.rf.Snapshot(index, w.Bytes())	
	}
}

func (kv *KVServer) readSnapshot(){
	data := kv.persister.ReadSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	DPrintf("Server %v before read db: %v\n", kv.me, kv.db)
	d.Decode(&kv.db)
	d.Decode(&kv.sessionMap)
	DPrintf("Server %v after read db: %v\n", kv.me, kv.db)

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.sessionMap = make(map[int]int)
	kv.NotifyCh = make(map[int]chan bool)
	kv.persister = persister

	kv.readSnapshot()
	// fmt.Println("start: ", me, kv.db)


	go kv.apply()

	// You may need initialization code here.

	return kv
}
