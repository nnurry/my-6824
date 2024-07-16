package mr

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GetRpcName(taskName string) string {
	return fmt.Sprintf("Coordinator.%v", taskName)
}

func ReadFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("cannot read %v", path)
	}
	return content, nil
}

func WorkerSampleTask(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	rpcName := GetRpcName("RequestTask")
	args := &RequestTaskArgs{}
	reply := &RequestTaskReply{}
	call(rpcName, &args, &reply)

	content, err := ReadFile(reply.MapTask.Filename)
	if err != nil {
		log.Fatal("can't read file", reply.MapTask.Filename)
	}

	mapResult := mapf(reply.MapTask.Filename, string(content))
	sort.Sort(ByKey(mapResult))

	for _, kva := range mapResult {
		fmt.Println(kva.Key, "->", kva.Value)
	}

	log.Println("map:", len(mapResult), "key-value pair(s)")

	i := 0
	for i < len(mapResult) {
		j := i
		for j < len(mapResult) && mapResult[i].Key == mapResult[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, mapResult[k].Value)
		}
		reduceResult := reducef(mapResult[i].Key, values)
		log.Println("reduce:", mapResult[i].Key, reduceResult)
		i = j
	}
}

func RequestTask() *RequestTaskReply {
	rpcName := GetRpcName("RequestTask")
	args := &RequestTaskArgs{}
	reply := &RequestTaskReply{}
	rpcCallWrapper(rpcName, &args, &reply)

	return reply
}

func ExecMapTask(
	mapt *MapTask,
	mapf func(string, string) []KeyValue,
	ctx context.Context,
) error {
	content, err := ReadFile(mapt.Filename)
	if err != nil {
		return fmt.Errorf("can't read file %v: %v", mapt.Filename, err.Error())
	}
	mapResult := mapf(mapt.Filename, string(content))

	mapFiles := make(map[int]*struct {
		Path string
		File *os.File
	})

	for _, kva := range mapResult {
		key := kva.Key
		value := kva.Value
		mapKvaStr := fmt.Sprintf("%v %v", key, value)
		hashKey := ihash(key) % mapt.NReduce
		_, ok := mapFiles[hashKey]

		intermediateFilename := fmt.Sprintf("./%v_nreduce_%d", strings.ReplaceAll(mapt.Filename, "..", ""), hashKey)
		if !ok {
			file, err := os.Create(intermediateFilename)
			if err != nil {
				return err
			}
			fmt.Fprintln(file, mapKvaStr)
			mapFiles[hashKey] = &struct {
				Path string
				File *os.File
			}{intermediateFilename, file}
		} else {
			file := mapFiles[hashKey].File
			fmt.Fprintln(file, mapKvaStr)
		}
	}

	for _, fileWrapper := range mapFiles {
		fileWrapper.File.Close()
		// log.Printf("(index %d -> %s)\n", reduceIndex, fileWrapper.Path)
	}

	return nil
}

func ExecReduceTask(
	reducet *ReduceTask,
	reducef func(string, []string) string,
	ctx context.Context,
) error {
	mapResult := []KeyValue{}
	stringContent := ""
	for _, filename := range reducet.IntermediateFileNames {
		content, err := ReadFile(filename)
		if err != nil {
			return fmt.Errorf("can't read intermediate file %v: %v", filename, err.Error())
		}
		stringContent += string(content)
	}
	kvaStrings := strings.Split(stringContent, "\n")
	lastKvaStringsIndex := len(kvaStrings) - 1
	if kvaStrings[lastKvaStringsIndex] == "" {
		kvaStrings = kvaStrings[:lastKvaStringsIndex]
	}

	for i, kvaString := range kvaStrings {
		splitted := strings.Split(kvaString, " ")
		if len(splitted) != 2 {
			return fmt.Errorf("can't parse (line=%d, pair=%v)", i, splitted)
		}
		mapResult = append(
			mapResult,
			KeyValue{
				Key:   splitted[0],
				Value: splitted[1],
			},
		)
	}

	sort.Sort(ByKey(mapResult))

	reducePath := fmt.Sprintf("./mr-out-%d", reducet.ReduceIndex)

	reduceFile, err := os.Create(reducePath)

	if err != nil {
		return fmt.Errorf("can't write %v: %v", reducePath, err.Error())
	}

	defer reduceFile.Close()

	i := 0
	for i < len(mapResult) {
		j := i
		for j < len(mapResult) && mapResult[i].Key == mapResult[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, mapResult[k].Value)
		}
		// log.Printf("reducef(%s, %v)\n", mapResult[i].Key, values)
		reduceResult := reducef(mapResult[i].Key, values)
		fmt.Fprintf(reduceFile, "%v %v\n", mapResult[i].Key, reduceResult)
		i = j
	}
	return nil
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {

	// Your worker implementation here.
	for {
		func() {
			var err error
			reply := RequestTask()
			// log.Println("reply from RequestTask()", reply)
			ctx, cancel := context.WithTimeout(context.Background(), reply.MaxTime)

			errCh := make(chan error, 1)

			defer cancel()
			if reply.Done && reply.MapTask == nil && reply.ReduceTask == nil {
				log.Println("my worker job here is done, exitting . . . ")
				os.Exit(1)
			}
			if reply.MapTask != nil {
				go func() {
					err = ExecMapTask(
						reply.MapTask,
						mapf,
						ctx,
					)
					errCh <- err
				}()
				for {
					select {
					case <-ctx.Done():
						// context exceeded
						// cancel Map task and mark as failed
						log.Println("context out of time")
						args := &ReportTaskArgs{"map", reply, "context exceeded"}
						reply := &ReportTaskReply{}
						rpcCallWrapper(GetRpcName("CancelTask"), args, reply)
						return
					case err = <-errCh:
						// function completed
						// check for error and mark as cancelled if needed
						args := &ReportTaskArgs{"map", reply, "success"}
						reply := &ReportTaskReply{}
						var rpcName string
						if err != nil {
							rpcName = GetRpcName("CancelTask")
							args.Message = err.Error()
						} else {
							rpcName = GetRpcName("DoneTask")
						}
						rpcCallWrapper(rpcName, args, reply)
						return
					}
				}
			} else if reply.ReduceTask != nil {
				go func() {
					err = ExecReduceTask(
						reply.ReduceTask,
						reducef,
						ctx,
					)
					errCh <- err
				}()
				for {
					select {
					case <-ctx.Done():
						// context exceeded
						// cancel Reduce task and mark as failed
						log.Println("context out of time")
						args := &ReportTaskArgs{"reduce", reply, "context exceeded"}
						reply := &ReportTaskReply{}
						rpcCallWrapper(GetRpcName("CancelTask"), args, reply)
						return
					case err = <-errCh:
						// function completed
						// check for error and mark as cancelled if needed
						args := &ReportTaskArgs{"reduce", reply, "success"}
						reply := &ReportTaskReply{}
						var rpcName string
						if err != nil {
							rpcName = GetRpcName("CancelTask")
							args.Message = err.Error()
						} else {
							rpcName = GetRpcName("DoneTask")
						}
						rpcCallWrapper(rpcName, args, reply)
						return
					}
				}
			} else {
				time.Sleep(time.Second * 1)
				return
			}
		}()
	}
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
	// log.Println("call rpc", rpcname, args, reply)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}

func rpcCallWrapper(rpcname string, args interface{}, reply interface{}) {
	if ok := call(rpcname, args, reply); !ok {
		log.Println("can't comm with master, exitting...")
		os.Exit(1)
	}
}
