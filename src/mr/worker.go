package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int {
	return len(a)
}

func (a ByKey) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a ByKey) Less(i, j int) bool {
	return a[i].Key < a[j].Key
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
		task := CallTask()
		switch task.State {
		case _map:
			workerMap(mapf, task)
		case _reduce:
			workerReduce(reducef, task)
		case _wait:
			time.Sleep(time.Second * 5)
		case _end:
			log.Println("work to the end")
			return
		default:
			log.Fatalln("no such task state!")
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	} else {
		fmt.Printf("call failed!\n")
	}
}

// get task from coordinator(master)
func CallTask() *TaskState {
	args := ExampleArgs{}
	reply := TaskState{}
	call("Coordinator.HandleTaskRequest", &args, &reply)
	return &reply
}

func CallTaskDone(taskInfo *TaskState) {
	reply := ExampleReply{}
	call("Coordinator.TaskDone", taskInfo, &reply)
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

func workerMap(mapf func(string, string) []KeyValue, taskInfo *TaskState) {
	log.Printf("Got assigned map task on %vth file %v\n", taskInfo.MapIndex, taskInfo.FileName)

	//read from target file as a kv slice
	interMediate := make([]KeyValue, 0)
	file, err := os.Open(taskInfo.FileName)
	if err != nil {
		log.Fatalf("workerMap: can not open %v\n", taskInfo.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("workerMap: can not read %v\n", taskInfo.FileName)
	}
	if err = file.Close(); err != nil {
		log.Fatalf("workerMap: file %v closed failed", taskInfo.FileName)
	}
	kva := mapf(taskInfo.FileName, string(content))
	interMediate = append(interMediate, kva...)

	// prepare output files and encoders
	nReduce := taskInfo.R
	outPrefix := "mr-tmp/mr-"
	outPrefix += strconv.Itoa(taskInfo.MapIndex)
	outPrefix += "-"
	outFiles := make([]*os.File, nReduce)
	fileEncoders := make([]*json.Encoder, nReduce)
	for outIndex := 0; outIndex < nReduce; outIndex++ {
		outFiles[outIndex], err = os.CreateTemp("mr-tmp", "mr-tmp-*")
		if err != nil {
			log.Fatalf("workerMap: create temp failed")
		}
		fileEncoders[outIndex] = json.NewEncoder(outFiles[outIndex])
	}

	for _, kv := range interMediate {
		outIndex := ihash(kv.Key) % nReduce
		file = outFiles[outIndex]
		encoder := fileEncoders[outIndex]
		if err = encoder.Encode(&kv); err != nil {
			log.Fatalf("workerMap: json encoder encode file %v failed", file.Name())
		}
	}

	for outIndex, File := range outFiles {
		outName := outPrefix + strconv.Itoa(outIndex)
		oldPath := filepath.Join(File.Name())
		if err = os.Rename(oldPath, outName); err != nil {
			log.Fatalf("workerMap: system rename failed")
		}
		if err = File.Close(); err != nil {
			log.Fatalf("workerMap: file %v closed failed", taskInfo.FileName)
		}
	}

	CallTaskDone(taskInfo)
}

func workerReduce(reducef func(string, []string) string, taskInfo *TaskState) {
	log.Printf("Got assigned reduce task on part %v\n", taskInfo.ReduceIndex)
	outName := "mr-out-" + strconv.Itoa(taskInfo.ReduceIndex)

	inNamePrefix := "mr-tmp/mr-"
	inNameSuffix := "-" + strconv.Itoa(taskInfo.ReduceIndex)

	//read from all files as a kv slice
	interMediate := make([]KeyValue, 0)
	for index := 0; index < taskInfo.M; index++ {
		inName := inNamePrefix + strconv.Itoa(index) + inNameSuffix
		file, err := os.Open(inName)
		if err != nil {
			log.Fatalf("workerReduce: open interMediate file %v failed: %v\n", inName, err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = decoder.Decode(&kv); err != nil {
				break
			}
			interMediate = append(interMediate, kv)
		}
	}

	sort.Sort(ByKey(interMediate))

	oFile, err := os.CreateTemp("mr-tmp", "mr-*")
	if err != nil {
		log.Fatalf("workerReduce: create output file %v failed: %v\n", outName, err)
	}
	i := 0
	for i < len(interMediate) {
		j := i + 1
		//聚合相同的Key的value
		for j < len(interMediate) && interMediate[j].Key == interMediate[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, interMediate[k].Value)
		}
		output := reducef(interMediate[i].Key, values)
		if _, err = fmt.Fprintf(oFile, "%v %v\n", interMediate[i].Key, output); err != nil {
			log.Fatalf("workerReduce: oFile write failed,%v", err)
		}

		i = j
	}
	if err = os.Rename(filepath.Join(oFile.Name()), outName); err != nil {
		log.Fatalf("workerReduce: system rename failed,%v", err)
	}
	if err = oFile.Close(); err != nil {
		log.Fatalf("workerReduce: oFile close failed,%v", err)
	}

	CallTaskDone(taskInfo)
}
