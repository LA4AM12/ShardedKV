package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		response := doHeatBeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapf, response)
		case ReduceJob:
			doReduceTask(reducef, response)
		case WaitJob:
			time.Sleep(time.Second)
		case ShutDownJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func doHeatBeat() HeartbeatResponse {
	request := HeartbeatRequest{}
	response := HeartbeatResponse{}
	ok := call("Coordinator.HeartBeat", &request, &response)
	if !ok {
		response.JobType = ShutDownJob
	}
	return response
}

func doReport(taskId int, jobType JobType) {
	request := ReportRequest{taskId, jobType}
	response := ReportResponse{}
	call("Coordinator.Report", &request, &response)
}

func doMapTask(mapf func(string, string) []KeyValue, response HeartbeatResponse) {
	fileName := response.FileName
	nReduce := response.NReduce
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

	// hash k-v
	bucket := make([][]KeyValue, nReduce)

	for _, kv := range kva {
		bucket[ihash(kv.Key)%nReduce] = append(bucket[ihash(kv.Key)%nReduce], kv)
	}

	for i := 0; i < nReduce; i++ {
		oname := "mr-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		enc.Encode(bucket[i])
		ofile.Close()
	}

	doReport(response.TaskId, MapJob)
}

func doReduceTask(reducef func(string, []string) string, response HeartbeatResponse) {
	files, _ := filepath.Glob(response.FileName)
	intermediate := readIntermediate(files)
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	i := 0
	for i < len(intermediate) {
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", response.TaskId)
	os.Rename(tempFile.Name(), oname)

	doReport(response.TaskId, ReduceJob)
}

func readIntermediate(files []string) (kva []KeyValue) {
	for _, f := range files {
		var temp []KeyValue
		file, _ := os.Open(f)
		dec := json.NewDecoder(file)
		dec.Decode(&temp)
		kva = append(kva, temp...)
		file.Close()
	}
	return kva
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
