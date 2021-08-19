package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	toBeAssignedMapTask int
	toBeAssignedReduceTask int
	files []string
	nReduce int
}

func (c *Coordinator) getTask() (string, int, string) {
	var taskType string
	var taskId int
	var fileName string

	if c.toBeAssignedMapTask < len(c.files) {
		taskType = "map"
		taskId = c.toBeAssignedMapTask
		fileName = c.files[c.toBeAssignedMapTask]
		c.toBeAssignedMapTask++
	} else if c.toBeAssignedReduceTask < c.nReduce {
		taskType = "reduce"
		taskId = c.toBeAssignedReduceTask
		c.toBeAssignedReduceTask++
	} else {
		taskType = "die"
	}
	return taskType, taskId, fileName
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) SendTask(args *SendTaskArgs, reply *SendTaskReply) error {
	reply.TaskType, reply.TaskId, reply.FileName = c.getTask()
	reply.NReduce = c.nReduce
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


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.toBeAssignedMapTask = 0
	c.toBeAssignedReduceTask = 0
	c.files = files
	c.nReduce = nReduce

	c.server()
	return &c
}
