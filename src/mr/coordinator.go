package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	InputFiles                []string
	MapIndex                  int
	NReduce                   int
	Tasks                     map[string]*WorkerTask
	CompletedMapTasksCount    int
	CompletedReduceTasksCount int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskRequest(taskRequest *TaskRequest, taskResponse *TaskResponse) error {
	if c.MapIndex == len(c.InputFiles) {
		taskResponse.TaskType = DoneTaskType
		return nil
	}

	taskResponse.TaskType = MapTaskType
	filePath := c.InputFiles[c.MapIndex]
	taskResponse.FilePath = filePath
	taskResponse.MapId = c.MapIndex
	taskResponse.NReduce = c.NReduce

	mapId := c.MapIndex
	c.Tasks[filePath] = &WorkerTask{
		TaskStatus: InProgressTask,
		MapTaskId:  mapId,
		StartTime:  time.Now(),
	}

	c.MapIndex += 1
	return nil
}

func (c *Coordinator) MapTaskCompleted(mapTaskCompletedRequest *MapTaskCompletedRequest, mapTaskCompletedResponse *MapTaskCompletedResponse) error {
	inputFilePath := mapTaskCompletedRequest.InputFilePath
	outputFilePath := mapTaskCompletedRequest.OutputFilePath

	c.Tasks[inputFilePath].TaskStatus = CompletedTask
	c.Tasks[inputFilePath].MapOutputFilePath = outputFilePath
	c.CompletedMapTasksCount += 1

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 2
	reply.TaskType = 2
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.CompletedReduceTasksCount == c.NReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Tasks = make(map[string]*WorkerTask)

	// we have N files and need to split them into nReduce buckets
	// device N/nReduce => total number of files
	c.InputFiles = files
	c.MapIndex = 0
	c.NReduce = nReduce

	c.server()
	return &c
}
