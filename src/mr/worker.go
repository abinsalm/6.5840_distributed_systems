package mr

import (
	"fmt"
	"io/ioutil"
	"os"
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

	// uncomment to send the Example RPC to the coordinator.
	AskForTask(mapf, reducef)
	//CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
		fmt.Printf("reply.taskType is: %v\n", reply.TaskType)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func AskForTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	taskRequest := TaskRequest{}
	taskResponse := TaskResponse{}

	ok := call("Coordinator.TaskRequest", &taskRequest, &taskResponse)
	if ok {
		fmt.Printf("Task Type: %v\n", taskResponse.TaskType)

		doMap(mapf, taskResponse)

	} else {
		fmt.Printf("call failed!\n")
	}
}

func doMap(mapf func(string, string) []KeyValue, taskResponse TaskResponse) {
	filename := taskResponse.FilePath
	fmt.Printf("Starting Map process on file %v\n", filename)

	// calculate reduce job ID
	reduceId := ihash(filename) % taskResponse.NReduce

	content := readFileContent(filename)
	kva := mapf(filename, string(content))
	fmt.Printf("Finished Map step for file: %v with %d output files\n", filename, len(kva))

	// Now write map output to disk
	// Write output to a temp file first so that nobody observes partially written files in the presence of crashes
	tempFile, err := os.CreateTemp("", "sample")
	check(err)
	defer os.Remove(tempFile.Name())
	fmt.Printf("Created temp file %v\n", tempFile.Name())

	// write content to temp file
	for _, kv := range kva {
		fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
	}
	fmt.Printf("Finished writing to temp file %v\n", tempFile.Name())

	// after all write is finished, move temp file to actual file
	outputFilePath := fmt.Sprint("mr-", taskResponse.MapId, "-", reduceId)
	os.Rename(tempFile.Name(), outputFilePath)
	fmt.Printf("Finished renaming temp file to  %v\n", outputFilePath)
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

	fmt.Println(err)
	return false
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
