package mr

import "fmt"
import "log"
import "net/rpc"
import "os"
import "io/ioutil"
import "strings"
import "strconv"
import "hash/fnv"
import "sort"
import "path/filepath"


type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.


	// call("Coordinator.MapReduce", &args, &reply)
	args := Args{}
	reply := Reply{}
	for true {
		args = Args{}
		reply = Reply{}
		call("Coordinator.MapReduce", &args, &reply)
		// fmt.Println(reply)
		if reply.IsDone{
			break
		}
		if reply.NotReady{
			continue
		}
		if reply.IsMap{
			Map(reply.Filename, reply.NReduce, mapf)
			args.MapTaskNumber = reply.Index
			call("Coordinator.ReceiveFinishedMap", &args, &reply)
		} else{
			Reduce(reply.Filenames, reply.Index, reducef)
			args.ReduceTaskNumber = reply.Index
			call("Coordinator.ReceiveFinishedReduce", &args, &reply)
		}
	}
		


	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func Map(filename string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// fmt.Println(string(content))
	kva := mapf(filename, string(content))
	ofiles := []*os.File{}
	for i := 0; i < nReduce; i++ {
		oname := "mr-" + filepath.Base(filename) + strconv.Itoa(i)
		// fmt.Println(oname)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		ofiles = append(ofiles, ofile)
	}
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % nReduce
		fmt.Fprintf(ofiles[reduceIndex], "%v %v\n", kv.Key, kv.Value)
	}


	for i := 0; i < nReduce; i++ {
		ofiles[i].Close()
	}
}


func Reduce(filenames []string, reduceIndex int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < len(filenames); i++ {
		file, _ := os.Open("mr-" + filepath.Base(filenames[i]) + strconv.Itoa(reduceIndex))
		content, _ := ioutil.ReadAll(file)
		// fmt.Println(string(content))
		file.Close()
		lines := strings.Split(string(content), "\n")
		// if reduceIndex == 9 {
		// 	fmt.Println(lines)
		// }
		
		for _, line := range lines {
			if line == "" {
				continue
			}
			kv := strings.Split(line, " ")
			intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
		}
	}

	// if reduceIndex == 9 {
	// 	fmt.Println(intermediate)
	// 	fmt.Println(len(intermediate))
	// }
	
	// fmt.Println(reduceIndex)
	sort.Sort(ByKey(intermediate))
	// if reduceIndex == 9 {
	// 	fmt.Println(intermediate)
	// 	fmt.Println(len(intermediate))
	// }
	oname := "mr-out-" + strconv.Itoa(reduceIndex)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		// fmt.Println(i)
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
	
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}


//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
