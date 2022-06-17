package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task := getTask()

		switch task.TaskState {
		case Map:
			mapTask(&task, mapf)

		case Reduce:
			reduceTask(&task, reducef)

		case Wait:
			time.Sleep(5 * time.Second)

		case Exit:
			return
		}
	}
}

func reduceTask(task *Task, reducef func(string, []string) string) {
	intermediate := *readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string

		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	outFilename := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), outFilename)
	task.Output = outFilename
	taskCompleted(task)
}

func mapTask(task *Task, mapf func(string, string) []KeyValue) {
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}
	intermediates := mapf(task.Input, string(content))
	buffer := make([][]KeyValue, task.NReduce)
	for _, kv := range intermediates {
		slot := ihash(kv.Key) % task.NReduce
		buffer[slot] = append(buffer[slot], kv)
	}

	mapOutPut := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		mapOutPut = append(mapOutPut, writeToLocalFile(task.TaskNumber, i, &buffer[i]))
	}

	task.Intermediates = mapOutPut
	taskCompleted(task)
}

func writeToLocalFile(x int, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("cannot open tempFile")
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func readFromLocalFile(files []string) *[]KeyValue {
	var kva []KeyValue
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal("Failed to open file "+filePath, err)
		}
		dec := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

func taskCompleted(task *Task) {
	reply := ExampleReply{}
	call("Coordinator.TaskCompleted", task, &reply)
}

func getTask() Task {
	args := ExampleArgs{}
	reply := Task{}
	call("Coordinator.AssignTask", &args, &reply)
	return reply
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
