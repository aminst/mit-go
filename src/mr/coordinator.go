package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	toBeAssignedMapTaskId int
	toBeAssignedReduceTaskId int
	files []string
	nReduce int
}

func (c *Coordinator) getTask() (string, int, string) {
	var taskType string
	var taskId int
	var fileName string

	if c.toBeAssignedMapTaskId < len(c.files) {
		taskType = "map"
		taskId = c.toBeAssignedMapTaskId
		fileName = c.files[c.toBeAssignedMapTaskId]
		c.toBeAssignedMapTaskId++
	} else if c.toBeAssignedReduceTaskId < c.nReduce {
		taskType = "reduce"
		taskId = c.toBeAssignedReduceTaskId
		c.toBeAssignedReduceTaskId++
	} else {
		taskType = "die"
	}
	return taskType, taskId, fileName
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) SendTask(args *SendTaskArgs, reply *SendTaskReply) error {
	reply.TaskType, reply.TaskId, reply.FileName = c.getTask()
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {

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

	c.toBeAssignedMapTaskId = 0
	c.toBeAssignedReduceTaskId = 0
	c.files = files
	c.nReduce = nReduce

	c.server()
	return &c
}
