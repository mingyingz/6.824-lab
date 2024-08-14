package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
// import "fmt"
// import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId int
	sequenceNumber int
	lastLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	clientId := nrand()
	ck.clientId = int(clientId)
	ck.sequenceNumber = 0
	// fmt.Println("Client ID: ", ck.clientId, ck.sequenceNumber)

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.	
	ck.sequenceNumber++
	// fmt.Println("Get key: ", ck.clientId, ck.sequenceNumber)
	si := ck.lastLeader
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.SequenceNumber = ck.sequenceNumber
	reply := GetReply{}
	for {
		// try each known server.

		var ok bool
		
		ch := make(chan bool)
		go func(args *GetArgs, reply *GetReply){
			ok = ck.servers[si].Call("KVServer.Get", args, reply)
			ch <- ok
		}(&args, &reply)
		select{
		case ok = <-ch:
			break
		// case <-time.After(200 * time.Millisecond):
		// 	ok = false
		// 	break
		}
			
		if ok {
			if reply.Err == OK {
				ck.lastLeader = si
				// fmt.Println("--------------------------------", key, reply, ck.sequenceNumber, ck.clientId)
				// if reply.Value == "x 0 0 y"{
				// 	fmt.Println("++++++++++++++++++++++++++++++++++++++++++", key, reply.Value, ck.sequenceNumber)
				// }
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.lastLeader = si
				return ""
			} else{
				// fmt.Println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
			}
		}
		si = (si + 1) % len(ck.servers)
		// fmt.Println("get loop end")

	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sequenceNumber++
	// fmt.Println("PA key: ", ck.clientId, ck.sequenceNumber)
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ClientId = ck.clientId
	args.SequenceNumber = ck.sequenceNumber
	reply := PutAppendReply{}
	si := ck.lastLeader
	for {
		// try each known server.
		var ok bool
		ch := make(chan bool)
		// fmt.Println(si, ck.lastLeader)
		// fmt.Println("PA b: ", si, reply.LeaderId)
		go func(args *PutAppendArgs, reply *PutAppendReply){
			// fmt.Println("PA call: ", si)
			ok = ck.servers[si].Call("KVServer.PutAppend", args, reply)
			// fmt.Println("me: ", ok, reply.Me, si)
			ch <- ok
		}(&args, &reply)
		select{
		case ok = <-ch:
			break
		// case <-time.After(400 * time.Millisecond):
		// 	ok = false
		// 	break
		}
			
		if ok && reply.Err == OK {
			DPrintf("Client %d finish %v key: %v value %v seq: %v\n", ck.clientId, args.Op, args.Key, args.Value, args.SequenceNumber)
			ck.lastLeader = si
			return
		}
		si = (si + 1) % len(ck.servers)
		// fmt.Println("put loop end")
	}
}

// 	for {
// 		// try each known server.
// 		oldsi := si
// 		if si == -1{
// 			si = ck.lastLeader
// 		}
// 		var ok bool
// 		for{
// 			ch := make(chan bool)
// 			fmt.Println(si, ck.lastLeader)
// 			// fmt.Println("PA b: ", si, reply.LeaderId)
// 			go func(args *PutAppendArgs, reply *PutAppendReply){
// 				// fmt.Println("PA call: ", si)
// 				ok = ck.servers[si].Call("KVServer.PutAppend", args, reply)
// 				// fmt.Println("me: ", ok, reply.Me, si)
// 				ch <- ok
// 			}(&args, &reply)
// 			breakLoop := false
// 			select{
// 			case ok = <-ch:
// 				if (ok && reply.Err == OK) || reply.LeaderId == -1 || !ok{
// 					breakLoop = true
// 					break
// 				}
// 			case <-time.After(200 * time.Millisecond):
// 				ok = false
// 				breakLoop = true
// 				break
// 			}
// 			if breakLoop{
// 				break
// 			}
// 			fmt.Println("PA wait: ", si, reply.LeaderId, reply.Err)
// 			si = reply.LeaderId
// 		}
			
// 		if ok && reply.Err == OK {
// 			ck.lastLeader = si
// 			return
// 		}
// 		si = oldsi
// 		si = (si + 1) % len(ck.servers)
// 		// fmt.Println("put loop end")
// 	}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
