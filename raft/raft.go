package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
	// "github.com/sasha-s/go-deadlock"
)

const HEARTBEAT int = 50   // leader send heatbit per 150ms
const TIMEOUTLOW int = 150 // the timeout period randomize between 500ms - 1000ms
const TIMEOUTHIGH int = 300
const CHECKCOMMITPERIOED int = 10
const killTimeout = 3000 * time.Millisecond

// var count int32 = 0

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	CommandIndex int
	Command      interface{}
	CommandValid bool

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//

type Entry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       int
	currentTerm int
	vote        int
	commitIndex int
	lastApplied int
	voteCount   int
	log         []Entry
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
	replyRecord []bool
	leaderId    int
	applyCond   *sync.Cond
	blankNum    int

	electionTimer  *time.Timer
	heartBeatTimer *time.Timer

	snapshotlen int
}

func (rf *Raft) getRelativeIndex(index int) int {
	return index - rf.snapshotlen
}

func (rf *Raft) getAbsoluteIndex(index int) int {
	return index + rf.snapshotlen
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == 0 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log)
	// e.Encode(rf.me)
	// e.Encode(rf.peers)
	e.Encode(rf.currentTerm)
	// e.Encode(rf.commitIndex)
	// e.Encode(rf.lastApplied)
	e.Encode(rf.vote)
	e.Encode(rf.snapshotlen)
	// e.Encode(rf.state)
	// e.Encode(rf.applyCh)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	// Your code here.
	// Example:
	rf.persister.SaveRaftState(rf.encodeState())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.log)
	// // d.Decode(&rf.me)
	// // d.Decode(&rf.peers)
	// d.Decode(&rf.currentTerm)
	// // d.Decode(&rf.commitIndex)
	// // d.Decode(&rf.lastApplied)
	// d.Decode(&rf.vote)
	// // d.Decode(&rf.state)
	// // d.Decode(&rf.applyCh)
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	var currentTerm, vote, snapshotlen int
	var log []Entry
	if d.Decode(&log) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&vote) != nil ||
		d.Decode(&snapshotlen) != nil {
		DPrintf("%v fails to recover from persist", rf)
		return
	}

	rf.currentTerm = currentTerm
	rf.vote = vote
	rf.log = log
	rf.snapshotlen = snapshotlen
	rf.commitIndex = rf.snapshotlen
	rf.lastApplied = rf.snapshotlen
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()

	// fmt.Println("11111111111111111111")
	if lastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		return false
	}
	SnapshotIndex := rf.getRelativeIndex(lastIncludedIndex)
	if lastIncludedIndex > rf.getAbsoluteIndex(len(rf.log)-1) {
		rf.log = make([]Entry, 1)
	} else {
		rf.log = rf.log[SnapshotIndex:]
	}
	rf.snapshotlen = lastIncludedIndex
	rf.log[0].Command = nil
	rf.log[0].Term = lastIncludedTerm
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	rf.mu.Unlock()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// fmt.Println("3333333333333333333")
	rf.mu.Lock()
	if index > rf.getAbsoluteIndex(len(rf.log)-1) {
		DPrintf("error555555555555555555555")
	}
	if index <= rf.snapshotlen {
		rf.mu.Unlock()
		return
	}
	// fmt.Println("444444444444444444")
	// fmt.Println("Snapshot: ", rf.me, index, rf.commitIndex, rf.lastApplied, len(rf.log))
	snapshotIndex := rf.getRelativeIndex(index)
	// if(snapshotIndex < 0 || snapshotIndex >= len(rf.log)){
	// 	rf.mu.Unlock()
	// 	return
	// }
	rf.log = rf.log[snapshotIndex:]
	// rf.log[0].Command = nil
	rf.snapshotlen = index
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	// fmt.Println("Snapshot: ", rf.me, index, rf.commitIndex, len(rf.log))
	// rf.persist()
	rf.mu.Unlock()
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	ConflictIndex int
	ConflictTerm  int
	Success       bool
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	// fmt.Printf("me: %d, state: %d, term: %d, vote: %d, args.Term: %d, args.candidate: %d\n", rf.me, rf.state, rf.currentTerm, rf.vote, args.Term, args.CandidateId)
	// go fmt.Println(rf.currentTerm, args.Term, rf.me, args.CandidateId)
	if args.Term > rf.currentTerm {
		rf.state = 1
		rf.currentTerm = args.Term
		rf.vote = -1
		rf.leaderId = -1
		rf.persist()
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	restriction := false
	if rf.log[len(rf.log)-1].Term < args.LastLogTerm || (rf.log[len(rf.log)-1].Term == args.LastLogTerm && rf.getAbsoluteIndex(len(rf.log)-1) <= args.LastLogIndex) {
		restriction = true
	}
	// if rf.state != 1 && rf.vote == args.CandidateId {
	// 	fmt.Println(rf.state, rf.vote)
	// }
	if (rf.vote == -1 || rf.vote == args.CandidateId) && restriction {
		reply.VoteGranted = true
		rf.vote = args.CandidateId
		rf.electionTimer.Reset(time.Duration(rand.Intn(TIMEOUTHIGH-TIMEOUTLOW)+TIMEOUTLOW) * time.Millisecond)
		// fmt.Println(rf.me, "vote for ", args.CandidateId, args.LastLogTerm, rf.log, len(rf.log))
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
	// fmt.Println(rf.me, rf.state, rf.currentTerm, rf.vote, args.Term, args.CandidateId, reply.VoteGranted)
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs) bool {
	// time.Sleep(time.Millisecond * time.Duration(rand.Intn(300)))
	reply := RequestVoteReply{}
	reply.VoteGranted = false
	// if args.Term < rf.currentTerm{
	// 	return false
	// }
	if !rf.sendRequestVote(server, args, &reply) {
		return false
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = 1
		rf.vote = -1
		rf.leaderId = -1
		rf.persist()
	} else if reply.VoteGranted == true && args.Term == rf.currentTerm && rf.state == 2 {
		rf.voteCount += 1
		rf.replyRecord[server] = true
		rf.mu.Unlock()
		return true
	}
	rf.mu.Unlock()
	return false
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	term := rf.currentTerm
	var isLeader bool
	// fmt.Println(rf.state, rf.me)
	index = rf.getAbsoluteIndex(len(rf.log) - 1)
	if rf.state == 0 && !rf.killed() {
		entry := Entry{term, command}
		rf.log = append(rf.log, entry)
		rf.persist()
		index = rf.getAbsoluteIndex(len(rf.log) - 1)
		rf.matchIndex[rf.me] = rf.getAbsoluteIndex(len(rf.log) - 1)
		DPrintf("Leader %v start a new log entry %v, index %v, term %v", rf.me, command, index, term)
		// fmt.Println(rf.me, command, index, term, rf.currentTerm)
		// fmt.Println("--------------------------------------------------------- ", rf.me, term, command, index, rf.snapshotlen, len(rf.log), rf.log)
		rf.mu.Unlock()
		isLeader = true
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.SendAppendEntries(i, false)
		}

	} else {
		rf.mu.Unlock()
		isLeader = false
	}

	return index, term, isLeader
}

func (rf *Raft) GetLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == 0 {
		if rf.leaderId != rf.me {
			fmt.Println("error")
		}
		return rf.me
	}
	return rf.leaderId
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)))
	rf.mu.Lock()
	// fmt.Println("AppendEntries: ", rf.me, args.LeaderId, args.PrevLogIndex, len(rf.log), len(args.Entries), rf.vote)
	// defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// fmt.Println("AppendEntries: ", rf.me, args.LeaderId, args.Term, rf.currentTerm, len(rf.log), rf.vote)
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = 1
		rf.currentTerm = args.Term
		rf.vote = -1
		rf.leaderId = -1
		rf.persist()
	}

	rf.state = 1
	rf.electionTimer.Reset(time.Duration(rand.Intn(TIMEOUTHIGH-TIMEOUTLOW)+TIMEOUTLOW) * time.Millisecond)
	rf.leaderId = args.LeaderId
	// if rf.state == 2{
	// 	rf.state = 1
	// 	rf.vote = -1
	// }
	// time.Sleep(1)
	// if args.PrevLogIndex < len(rf.log){
	// 	fmt.Println("AppendEntries: ", rf.me, args.LeaderId, args.PrevLogIndex, len(rf.log), rf.vote, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term, rf.log[:min(len(rf.log) - 1, 3)])
	// }
	// fmt.Println("AppendEntries: ", args.PrevLogIndex, len(rf.log), rf.snapshotlen, len(args.Entries))
	prevLogIndex := rf.getRelativeIndex(args.PrevLogIndex)
	// fmt.Println("AppendEntries: ", rf.me, args.LeaderId, args.PrevLogIndex, prevLogIndex, len(rf.log), rf.snapshotlen, len(args.Entries), rf.log[:min(len(rf.log) - 1, 3)])
	if prevLogIndex < 0 {
		reply.ConflictIndex = rf.getAbsoluteIndex(0)
		reply.ConflictTerm = -1
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if prevLogIndex < len(rf.log) && rf.log[prevLogIndex].Term == args.PrevLogTerm {
		//fmt.Println(args.PrevLogIndex, len(rf.log))
		for i := 0; i < len(args.Entries); i++ {
			if prevLogIndex+1+i < len(rf.log) {
				if rf.log[prevLogIndex+1+i].Term != args.Entries[i].Term {
					// rf.log = rf.log[:args.PrevLogIndex + 1 + i]
					// rf.log = append(rf.log, args.Entries[i:]...)
					rf.log = append(rf.log[:prevLogIndex+1+i], args.Entries[i:]...)
					// fmt.Printf("me: %d, args.LeaderId: %d, args.PrevLogIndex: %d, args.Term: %d, rf.currentTerm: %d ", rf.me, args.LeaderId, args.PrevLogIndex, args.Term, rf.currentTerm)
					// fmt.Println(args.Entries)
					rf.persist()
					break
				}
			} else {
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		}
		// fmt.Println(rf.me, rf.log)
		reply.Success = true
		// if rf.commitIndex > args.LeaderCommit{
		// 	fmt.Println("error4")
		// }
		// if args.LeaderCommit > len(rf.log) - 1{
		// 	fmt.Println("error6")
		// }
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = min(args.LeaderCommit, rf.getAbsoluteIndex(len(rf.log)-1))
			// fmt.Println(rf.me, rf.commitIndex, args.LeaderId, args.LeaderCommit, len(args.Entries), args.PrevLogIndex)
			rf.applyCond.Signal()
		}
		// if(rf.commitIndex == len(rf.log) - 1){
		// 	fmt.Println(rf.me, rf.commitIndex, rf.log[len(rf.log) - 1])
		// }
		// rf.persist()
	} else {
		// if(args.PrevLogIndex == 0){
		// 	fmt.Println("error5")
		// }
		if prevLogIndex < len(rf.log) {
			reply.ConflictTerm = rf.log[prevLogIndex].Term
			for i := prevLogIndex; i >= 0; i-- {
				if rf.log[i].Term != reply.ConflictTerm {
					// fmt.Printf("me: %d, reply.ConflictTerm: %d, reply.ConflictIndex: %d, args.PrevLogIndex: %d, args.PrevLogTerm: %d ", rf.me, reply.ConflictTerm, i + 1, args.PrevLogIndex, args.PrevLogTerm)
					// fmt.Println(rf.log)
					reply.ConflictIndex = rf.getAbsoluteIndex(i + 1)
					break
				}
			}
			if reply.ConflictIndex == 0 {
				// fmt.Println("***************************************")
			}
		} else {
			// 	fmt.Println("+++++++++++++++++++++++++++++++++++")
			reply.ConflictIndex = rf.getAbsoluteIndex(len(rf.log))
			reply.ConflictTerm = -1
		}
		// reply.ConflictIndex = len(rf.log)
		reply.Success = false
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) SendAppendEntries(server int, isHeartBeat bool) {
	// for rf.killed() == false{
	rf.mu.Lock()
	if rf.state != 0 {
		rf.mu.Unlock()
		return
	}
	// isHeartBeat = false

	// fmt.Println(server, args.PrevLogIndex, rf.nextIndex[server], len(rf.log), len(args.Entries), rf.commitIndex)
	// if length - 1 < rf.commitIndex{
	// 	fmt.Println("error1")
	// }
	if rf.nextIndex[server] <= rf.snapshotlen {
		args := InstallSnapshotArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LastIncludedIndex = rf.snapshotlen
		args.LastIncludedTerm = rf.log[0].Term
		args.Data = rf.persister.ReadSnapshot()
		rf.mu.Unlock()
		if !rf.SendInstallSnapshot(server, &args) {
			return
		}
		// continue
		return
	}
	args := AppendEntriesArgs{}
	nextIndex := rf.nextIndex[server]
	args.Term = rf.currentTerm
	args.LeaderCommit = rf.commitIndex
	args.LeaderId = rf.me
	// fmt.Println(server, args.PrevLogIndex, rf.nextIndex[server], len(rf.log), rf.me, rf.commitIndex, rf.log[:min(len(rf.log) - 1, 3)])
	// fmt.Println(server, args.PrevLogIndex, rf.nextIndex[server], len(rf.log))
	args.PrevLogIndex = max(rf.nextIndex[server]-1, rf.snapshotlen)
	args.Entries = CopySlice(rf.log[max(rf.getRelativeIndex(rf.nextIndex[server]), 1):])
	args.PrevLogTerm = rf.log[rf.getRelativeIndex(args.PrevLogIndex)].Term
	reply := AppendEntriesReply{}
	rf.mu.Unlock()
	if !rf.sendAppendEntries(server, &args, &reply) {
		// fmt.Println(1, reply.Term, server)
		return
	}
	rf.mu.Lock()
	// fmt.Println("---- ", 2, reply.Term, reply.Success, rf.currentTerm, rf.state, rf.me)
	// defer rf.mu.Unlock()
	// if length - 1 < rf.commitIndex{
	// 	fmt.Println("error2")
	// 	// rf.mu.Unlock()
	// 	// return
	// }
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = 1
		rf.vote = -1
		rf.leaderId = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}
	if rf.state != 0 || nextIndex != rf.nextIndex[server] || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	// fmt.Println(server, args.PrevLogIndex, rf.nextIndex[server], len(rf.log), reply.Success, rf.commitIndex)
	rf.replyRecord[server] = true
	if reply.Success == true {
		// fmt.Println(isHeartBeat, rf.matchIndex[server], rf.nextIndex[server], args.PrevLogIndex, server, len(args.Entries))
		if rf.nextIndex[server] < args.PrevLogIndex+len(args.Entries)+1 {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		}
		if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
			// if isHeartBeat{
			// 	fmt.Println("error7", args.Entries, args.PrevLogIndex, len(rf.log), rf.nextIndex[server], rf.matchIndex[server])
			// }
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			if rf.matchIndex[server] > rf.getAbsoluteIndex(len(rf.log)-1) {
				fmt.Println("error22222222222")
			}
			// fmt.Println(server)
		}
		rf.mu.Unlock()
		return
	} else {
		// rf.nextIndex[server] = reply.ConflictIndex
		// nextIndex := min(args.PrevLogIndex, reply.ConflictIndex)
		nextIndex := reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			for i := rf.getRelativeIndex(args.PrevLogIndex); i > 0; i-- {
				if rf.log[i-1].Term == reply.ConflictTerm {
					nextIndex = rf.getAbsoluteIndex(i)
					break
				}
			}
			// fmt.Printf("me: %d, server: %d, nextIndex: %d, conflictIndex: %d, conflictTerm: %d\n", rf.me, server, nextIndex, reply.ConflictIndex, reply.ConflictTerm)
		}
		// if rf.nextIndex[server] > nextIndex{
		rf.nextIndex[server] = nextIndex
		// }else{
		// 	rf.mu.Unlock()
		// 	break
		// }
		// if nextIndex > args.PrevLogIndex + 1{
		// 	fmt.Println("error9")
		// }

		// fmt.Println(server, args.PrevLogIndex, rf.nextIndex[server], len(rf.log), rf.me, rf.commitIndex, rf.log[:min(len(rf.log) - 1, 3)])

		rf.mu.Unlock()
		// fmt.Println(server, args.PrevLogIndex, rf.nextIndex[server], len(rf.log), rf.me, rf.commitIndex, rf.log[:min(len(rf.log) - 1, 3)])
		// fmt.Println("nextIndex: ", rf.nextIndex[server])
	}
	// rf.mu.Unlock()
	// }
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// fmt.Println("2222222222222222222222")
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = 1
		rf.currentTerm = args.Term
		rf.vote = -1
		rf.leaderId = -1
		rf.persist()
	}
	rf.state = 1
	rf.electionTimer.Reset(time.Duration(rand.Intn(TIMEOUTHIGH-TIMEOUTLOW)+TIMEOUTLOW) * time.Millisecond)
	// fmt.Println("InstallSnapshot: ", rf.me, args.LeaderId, args.LastIncludedIndex, len(rf.log))
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	msg := ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
	select {
	case <-time.After(killTimeout):
		return
	case rf.applyCh <- msg:
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) SendInstallSnapshot(server int, args *InstallSnapshotArgs) bool {

	reply := InstallSnapshotReply{}
	if !rf.sendInstallSnapshot(server, args, &reply) {
		return false
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = 1
		rf.vote = -1
		rf.leaderId = -1
		rf.persist()
		rf.mu.Unlock()
		return false
	} else if (args.Term < rf.currentTerm) || rf.state != 0 {
		rf.mu.Unlock()
		return false
	}
	if rf.nextIndex[server] < args.LastIncludedIndex+1 {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		// fmt.Println("error999999999999999")
	}
	if rf.matchIndex[server] < args.LastIncludedIndex {
		rf.matchIndex[server] = args.LastIncludedIndex
		// fmt.Println("error999999999999999")
	}
	rf.replyRecord[server] = true
	rf.mu.Unlock()
	return true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.applyCond.Broadcast()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.vote = -1
	rf.leaderId = -1
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.replyRecord = make([]bool, len(rf.peers))
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = 1
	rf.dead = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.electionTimer = time.NewTimer(time.Duration(rand.Intn(TIMEOUTHIGH-TIMEOUTLOW)+TIMEOUTLOW) * time.Millisecond)
	rf.heartBeatTimer = time.NewTimer(0 * time.Millisecond)

	rf.readPersist(persister.ReadRaftState())

	// fmt.Println("bbbbbbbbbbbbbbb ", rf.me, rf.currentTerm, rf.state, rf.vote, len(rf.log))

	go rf.work()
	go rf.apply()

	return rf
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = 2
	rf.voteCount = 1
	rf.currentTerm += 1
	rf.vote = rf.me
	rf.leaderId = -1
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	args.LastLogIndex = rf.getAbsoluteIndex(len(rf.log) - 1)
	rf.persist()
	// fmt.Println("+++++++++++ ", rf.me, rf.state, rf.currentTerm, rf.log)
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			if !rf.SendRequestVote(peer, &args) {
				return
			}
			rf.mu.Lock()
			if rf.state == 2 && rf.voteCount >= len(rf.peers)/2+1 {
				rf.state = 0
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.getAbsoluteIndex(len(rf.log))
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = rf.getAbsoluteIndex(len(rf.log) - 1)
				// fmt.Println(rf.currentTerm, rf.me, rf.log[len(rf.log) - 1])
				rf.heartBeatTimer.Reset(0)
				rf.leaderId = rf.me
				rf.mu.Unlock()
				rf.Start(nil)
				rf.mu.Lock()
				go rf.heartBeat()
				go rf.checkCommit()
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) checkCommit() {
	//原子+1
	// atomic.AddInt32(&count, 1)
	// fmt.Println("check count: ", count)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != 0 {
			rf.mu.Unlock()
			return
		}
		for i := len(rf.log) - 1; i > rf.getRelativeIndex(rf.commitIndex); i-- {
			if rf.log[i].Term == rf.currentTerm {
				count := 0
				for j := 0; j < len(rf.peers); j++ {
					if rf.getRelativeIndex(rf.matchIndex[j]) >= i {
						// fmt.Println(rf.me, rf.matchIndex[j], i, j, rf.currentTerm, count)
						count++
					}
				}
				if count >= len(rf.peers)/2+1 {
					rf.commitIndex = rf.getAbsoluteIndex(i)
					rf.applyCond.Signal()
					break
				}
			}
		}
		// fmt.Printf("me: %v commitindex: %v matchindex: %v log: %v\n", rf.me, rf.commitIndex, rf.matchIndex, rf.log)
		rf.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	// atomic.AddInt32(&count, -1)
	// fmt.Println("check count: ", count)
}
func (rf *Raft) heartBeat() {
	tt := 0
	for !rf.killed() {
		select {
		case <-rf.heartBeatTimer.C:
		}
		rf.mu.Lock()
		// fmt.Println("+++++++++++ ", rf.me, rf.state, rf.currentTerm)
		if rf.state != 0 {
			fmt.Println(rf.me, "leader out")
			rf.mu.Unlock()
			break
		}
		// fmt.Println("llllllllll: ", rf.me, rf.state, rf.currentTerm, len(rf.log))
		// fmt.Println(rf.heartBeatTimer)
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.SendAppendEntries(i, true)
		}
		rf.mu.Lock()
		if rf.state != 0 {
			rf.mu.Unlock()
			break
		}
		rf.heartBeatTimer.Reset(time.Duration(HEARTBEAT) * time.Millisecond)
		// rf.persist()
		rf.mu.Unlock()
		tt++
	}
}

func (rf *Raft) work() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
		}
		rf.mu.Lock()
		// fmt.Println(rf.me, rf.state, rf.leaderId)

		if rf.state == 0 {
			replyCount := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.replyRecord[i] == true {
					replyCount += 1
				}
				rf.replyRecord[i] = false
			}
			if replyCount < len(rf.peers)/2+1 {
				rf.state = 1
				rf.vote = -1
				rf.leaderId = -1
				rf.persist()
			}
		}
		if rf.state != 0 {
			go rf.startElection()
		}
		rf.electionTimer.Reset(time.Duration(rand.Intn(TIMEOUTHIGH-TIMEOUTLOW)+TIMEOUTLOW) * time.Millisecond)
		rf.mu.Unlock()
	}
}

func (rf *Raft) apply() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}
		commitIndex := rf.getRelativeIndex(rf.commitIndex)
		lastApplied := rf.getRelativeIndex(rf.lastApplied)
		snapshotlen := rf.snapshotlen
		state := rf.state
		// if commitIndex <= lastApplied{
		// 	fmt.Println("error8", commitIndex, lastApplied, rf.state)
		// }
		log := make([]Entry, commitIndex-lastApplied)
		// fmt.Println("error8", commitIndex, lastApplied, len(rf.log), rf.me, rf.snapshotlen, rf.state)
		copy(log, rf.log[lastApplied+1:commitIndex+1])
		// fmt.Println(len(log))
		rf.mu.Unlock()
		for i := 0; i < len(log); i++ {
			msg := ApplyMsg{CommandValid: true, CommandIndex: snapshotlen + lastApplied + i + 1, Command: log[i].Command}
			// fmt.Println("------------ ", rf.me, msg, rf.state, rf.currentTerm, rf.log[len(rf.log) - 1], i)
			if log[i].Command == nil {
				continue
			}
			select {
			case <-time.After(killTimeout):
				if rf.killed() {
					return
				} else {
					continue
				}

			case rf.applyCh <- msg:
			}
			// fmt.Println("------------ ", rf.me, msg, rf.state, rf.currentTerm, log[i])
			DPrintf("Server %v (state: %v) apply %v, index %v, term %v", rf.me, state, log[i].Command, snapshotlen+lastApplied+i+1, log[i].Term)
		}
		rf.mu.Lock()
		rf.lastApplied = max(snapshotlen+commitIndex, rf.lastApplied)
		// fmt.Println("Commit: ", rf.me, rf.commitIndex, rf.lastApplied, len(rf.log))
		rf.mu.Unlock()
	}
}
