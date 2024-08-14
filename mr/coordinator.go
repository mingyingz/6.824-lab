package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
// import "fmt"


type Coordinator struct {

	// Your definitions here.
	nfile int
	nReduce int
	filenames []string
	maptasklog []int
	reducetasklog []int
	MapFinished int
	ReduceFinished int


}

var lock sync.Mutex
// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) ReceiveFinishedMap(args *Args, reply *Reply) error {
	lock.Lock()
	c.MapFinished++
	c.maptasklog[args.MapTaskNumber] = 2 // log the map task as finished
	lock.Unlock()
	return nil
}

func (c *Coordinator) ReceiveFinishedReduce(args *Args, reply *Reply) error {
	lock.Lock()
	c.ReduceFinished++
	c.reducetasklog[args.ReduceTaskNumber] = 2 // log the reduce task as finished
	lock.Unlock()
	return nil
}

func (c *Coordinator) MapReduce(args *Args, reply *Reply) error{
	lock.Lock()
	// fmt.Println(c)
	if c.MapFinished < c.nfile {
		// reply.IsMap = false
		allocate := -1
		for i := 0; i < c.nfile; i++{
			if c.maptasklog[i] == 0{
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.NotReady = true
			lock.Unlock()
		} else{
			c.maptasklog[allocate] = 1
			lock.Unlock()
			reply.IsMap = true
			reply.Filename = c.filenames[allocate]
			reply.Index = allocate
			reply.NReduce = c.nReduce
			reply.IsDone = false
			go func (){
				time.Sleep(time.Duration(10) * time.Second)
				lock.Lock()
				if c.maptasklog[allocate] == 1{
					c.maptasklog[allocate] = 0
				}
				lock.Unlock()
			} ()
		}
	} else if c.ReduceFinished < c.nReduce && c.MapFinished == len(c.filenames){
		// fmt.Println(c.MapFinished)
		allocate := -1
		for i := 0; i < c.nReduce; i++{
			if c.reducetasklog[i] == 0{
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.NotReady = true
			lock.Unlock()
		} else{
			c.reducetasklog[allocate] = 1
			lock.Unlock()
			reply.IsMap = false
			reply.Filenames = c.filenames
			reply.Index = allocate
			reply.IsDone = false
			reply.NotReady = false
			
			go func (){
				time.Sleep(time.Duration(10) * time.Second)
				lock.Lock()
				if c.reducetasklog[allocate] == 1{
					c.reducetasklog[allocate] = 0
				}
				lock.Unlock()
			} ()
		}
	} else if c.ReduceFinished == 10 {
		lock.Unlock()
		reply.IsDone = true
	} else{
		lock.Unlock()
		reply.NotReady = true
		// lock.Unlock()
		// time.Sleep(time.Second)
		// lock.Lock()
	}
	// fmt.Println(reply)
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	lock.Lock()
	if c.ReduceFinished == 10 {
		ret = true
	}
	lock.Unlock()


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	
	// Your code here.
	c.nfile = len(files)
	c.nReduce = nReduce
	c.filenames = files
	c.MapFinished = 0
	c.maptasklog = make([]int, c.nfile)
	c.reducetasklog = make([]int, c.nReduce)


	c.server()
	return &c
}
