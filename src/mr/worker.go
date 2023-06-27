package mr

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		taskResponse, err := AskForTask(mapf, reducef)
		check(err)

		if taskResponse.TaskType == DoneTaskType {
			log.Println("Job is done")
			break
		} else if taskResponse.TaskType == MapTaskType {
			log.Println("Starting Map task")
			outputFilePath := doMap(mapf, taskResponse)

			mapTaskCompletedRequest := MapTaskCompletedRequest{
				InputFilePath:  taskResponse.FilePath,
				OutputFilePath: outputFilePath,
			}
			mapTaskCompletedResponse := MapTaskCompletedResponse{}

			log.Println("Sending Coordinator.MapTaskCompleted")
			ok := call("Coordinator.MapTaskCompleted", &mapTaskCompletedRequest, &mapTaskCompletedResponse)
			if ok {
				log.Println("Received successful response for Coordinator.MapTaskCompleted")
			} else {
				log.Println("Received error response for Coordinator.MapTaskCompleted!")
			}
		} else if taskResponse.TaskType == ReduceTaskType {
			log.Fatal("Reduce Task is not supported")
		}
	}

}

func AskForTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) (TaskResponse, error) {
	taskRequest := TaskRequest{}
	taskResponse := TaskResponse{}

	ok := call("Coordinator.TaskRequest", &taskRequest, &taskResponse)
	if ok {
		log.Printf("Task Type: %v\n", taskResponse.TaskType)

		return taskResponse, nil

	} else {
		log.Printf("call failed!\n")
		return TaskResponse{}, errors.New("Call failed")
	}
}

func doMap(mapf func(string, string) []KeyValue, taskResponse TaskResponse) string {
	filename := taskResponse.FilePath
	log.Printf("Starting Map process on file %v\n", filename)

	// calculate reduce job ID
	reduceId := ihash(filename) % taskResponse.NReduce

	content := readFileContent(filename)
	kva := mapf(filename, string(content))
	log.Printf("Finished Map step for file: %v with %d output files\n", filename, len(kva))

	// Now write map output to disk
	// Write output to a temp file first so that nobody observes partially written files in the presence of crashes
	tempFile, err := os.CreateTemp("", "sample")
	check(err)
	log.Printf("Created temp file %v\n", tempFile.Name())

	// write content to temp file
	for _, kv := range kva {
		_, err := fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
		check(err)
	}
	log.Printf("Finished writing to temp file %v\n", tempFile.Name())

	// after all write is finished, move temp file to actual file
	err = os.MkdirAll("map_files", os.ModePerm)
	outputFilePath := filepath.Join("map_files", fmt.Sprint("mr-", taskResponse.MapId, "-", reduceId))
	err = os.Rename(tempFile.Name(), outputFilePath)
	check(err)
	log.Printf("Finished renaming temp file to  %v\n", outputFilePath)

	return outputFilePath
}

func readFileContent(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	log.Println(err)
	return false
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
