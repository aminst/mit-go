package mr

import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "sort"
import "math/rand"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func runPartition(taskId int, intermediate []KeyValue, nReduce int) {
	randomPrefix := rand.Int()

	sort.Sort(ByKey(intermediate))
	for _, kv := range intermediate {
		reduceNum := ihash(kv.Key) % nReduce
		fileName := fmt.Sprintf("temp-%d-%d-%d", randomPrefix, taskId, reduceNum)
		file, _ := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
	}

	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("temp-%d-%d-%d", randomPrefix, taskId, i)
		newFileName :=fmt.Sprintf("map-%d-%d", taskId, i)
		os.Rename(fileName, newFileName)
	}
}

func runMap(mapf func(string, string) []KeyValue, taskId int, fileName string, nReduce int) {
	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)
	runPartition(taskId, intermediate, nReduce)
	// call done
}

func runReduce(reducef func(string, []string) string, taskId int) {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	reply := CallForSendTask()
	if reply.TaskType == "map" {
		runMap(mapf, reply.TaskId, reply.FileName, reply.NReduce)
	} else if reply.TaskType == "reduce" {
		runReduce(reducef, reply.TaskId)
	} else {
		os.Exit(0)
	}
}

func CallForSendTask() SendTaskReply {
	args := SendTaskArgs{}
	reply := SendTaskReply{}
	call("Coordinator.SendTask", &args, &reply)
	return reply
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

	fmt.Println(err)
	return false
}
