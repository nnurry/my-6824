package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Files             []string
	NReduce           int
	MapStatus         map[string]*JobProperties
	ReduceStatus      map[int]*JobProperties
	IntermediateFiles map[int][]string
	WorkerTicker      *time.Ticker
	Mutex             *sync.Mutex
	MapIsDone         bool
	ReduceIsDone      bool
}

type JobProperties struct {
	Status      int
	CreatedTime time.Time
	MaxTime     time.Duration
}

// Your code here -- RPC handlers for the worker to call.
type RequestTaskArgs struct {
}

type RequestTaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
	MaxTime    time.Duration
	Done       bool
}

type ReportTaskArgs struct {
	TaskType string
	TaskBody *RequestTaskReply
	Message  string
}

type ReportTaskReply struct {
	Done bool
}

type ReduceTask struct {
	ReduceIndex           int
	IntermediateFileNames []string
}

type MapTask struct {
	Filename string
	NReduce  int
}

func (c *Coordinator) GetMapTask() *MapTask {
	c.Mutex.Lock()
	for filename, jobProperties := range c.MapStatus {
		if jobProperties.Status == 0 {
			mapT := &MapTask{
				Filename: filename,
				NReduce:  c.NReduce,
			}

			c.MapStatus[filename].Status = 1
			c.MapStatus[filename].CreatedTime = time.Now().UTC()
			log.Println("allocate map task:", filename)
			c.Mutex.Unlock()

			return mapT
		}
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) GetReduceTask() *ReduceTask {
	c.Mutex.Lock()
	for reduceIndex, jobProperties := range c.ReduceStatus {
		if jobProperties.Status == 0 {
			reduceT := &ReduceTask{
				ReduceIndex:           reduceIndex,
				IntermediateFileNames: c.IntermediateFiles[reduceIndex],
			}

			c.ReduceStatus[reduceIndex].Status = 1
			c.ReduceStatus[reduceIndex].CreatedTime = time.Now().UTC()
			log.Println("allocate reduce task:", reduceIndex)
			c.Mutex.Unlock()

			return reduceT
		}
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) CheckMapCompletion() bool {
	if c.MapIsDone {
		return true
	}
	for _, jobProperties := range c.MapStatus {
		if jobProperties.Status != 2 {
			return false
		}
	}
	c.MapIsDone = true
	c.ShuffleIntermediateFiles()
	return true
}

func (c *Coordinator) CheckReduceCompletion() bool {
	if c.ReduceIsDone {
		return true
	}
	for _, jobProperties := range c.ReduceStatus {
		if jobProperties.Status != 2 {
			return false
		}
	}
	c.ReduceIsDone = true
	return true
}

func (c *Coordinator) ShuffleIntermediateFiles() bool {
	entries, _ := os.ReadDir(".")
	c.Mutex.Lock()
	for _, entry := range entries {
		entryName := entry.Name()
		splittedEntryName := strings.Split(entryName, "_nreduce_")
		if len(splittedEntryName) > 1 {
			// valid
			reduceIndex, _ := strconv.Atoi(splittedEntryName[1])
			_, ok := c.IntermediateFiles[reduceIndex]
			if !ok {
				// create new slice of filenames for certain reduce index
				c.IntermediateFiles[reduceIndex] = []string{entryName}
				c.ReduceStatus[reduceIndex] = &JobProperties{
					Status:  0,
					MaxTime: 45 * time.Second,
				}
			} else {
				// append
				c.IntermediateFiles[reduceIndex] = append(c.IntermediateFiles[reduceIndex], entryName)
			}
		}
	}
	c.Mutex.Unlock()
	return true
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	maxTime := 45 * time.Second
	reply.MaxTime = maxTime

	// get map task from the pool
	mapT := c.GetMapTask()
	if mapT != nil {
		reply.MapTask = mapT
		return nil
	}
	// check if map phase is complete (barrier synchronization)
	if !c.CheckMapCompletion() {
		reply.Done = false
		return nil
	}
	// get reduce task
	reduceT := c.GetReduceTask()
	if reduceT != nil {
		reply.ReduceTask = reduceT
		return nil
	}

	if !c.CheckReduceCompletion() {
		reply.Done = false
		return nil
	}
	reply.Done = true
	return nil
}

func (c *Coordinator) CancelTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	switch args.TaskType {
	case "map":
		// cancel map task
		mapT := args.TaskBody.MapTask
		log.Println("cancel map task:", mapT.Filename)
		c.Mutex.Lock()
		c.ModifyMapTask(mapT.Filename, 0)
		log.Println("unmap", mapT.Filename, c.MapStatus[mapT.Filename])
		c.Mutex.Unlock()
		reply.Done = true

	case "reduce":
		// cancel reduce task
		reduceT := args.TaskBody.ReduceTask
		log.Println("cancel map task:", reduceT.ReduceIndex)
		c.Mutex.Lock()
		c.ModifyReduceTask(reduceT.ReduceIndex, 0)
		log.Println("unreduce", reduceT.ReduceIndex, c.ReduceStatus[reduceT.ReduceIndex])
		c.Mutex.Unlock()
		reply.Done = true

	default:
		log.Fatalf("can't cancel task of type=%v\n", args.TaskType)
	}

	return nil
}

func (c *Coordinator) DoneTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	switch args.TaskType {
	case "map":
		// mark map task as complete
		mapT := args.TaskBody.MapTask
		c.Mutex.Lock()
		c.ModifyMapTask(mapT.Filename, 2)
		log.Println("done map task:", mapT.Filename)
		c.Mutex.Unlock()
		reply.Done = true

	case "reduce":
		// mark reduce task as complete
		reduceT := args.TaskBody.ReduceTask
		c.Mutex.Lock()
		c.ModifyReduceTask(reduceT.ReduceIndex, 2)
		log.Println("done map task:", reduceT.ReduceIndex)
		c.Mutex.Unlock()
		reply.Done = true

	default:
		log.Fatalf("can't cancel task of type=%v\n", args.TaskType)
	}

	return nil
}

func (c *Coordinator) ModifyMapTask(filename string, status int) {
	c.MapStatus[filename].Status = status
}

func (c *Coordinator) ModifyReduceTask(reduceIndex int, status int) {
	c.ReduceStatus[reduceIndex].Status = status
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
	httpAddr := l.Addr().Network() + ":/" + l.Addr().String()
	go func() {
		e = http.Serve(l, nil)
		if e != nil {
			log.Fatalln("Can't serve HTTP at " + httpAddr)
		}
	}()
	log.Println("serving HTTP at " + httpAddr)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	ret := c.MapIsDone && c.ReduceIsDone
	if ret {
		log.Println("Done() is actually done -> waiting to exit")
	}
	return ret
}

func (c *Coordinator) GetStatus() string {
	logline := "Status - "
	logline += fmt.Sprint(time.Now().Format(time.UnixDate))
	logline += "\n\n"
	logline += "Map status\n"
	statusFormat := "\t- %v: %v\n"

	mapCompleted := true
	for filename, jobProperties := range c.MapStatus {
		var strStatus string
		switch jobProperties.Status {
		case 2:
			strStatus = "completed"
		case 1:
			strStatus = "running"
		case 0:
			strStatus = "pending"
		default:
			strStatus = "unknown"
		}
		if strStatus != "completed" {
			mapCompleted = false
		}
		logline += fmt.Sprintf(statusFormat, filename, strStatus)
	}
	logline += fmt.Sprintf("\nCompleted map? %t\n\n", mapCompleted)

	logline += "Reduce status\n"
	reduceCompleted := true
	for reduceIndex, jobProperties := range c.ReduceStatus {
		var strStatus string
		switch jobProperties.Status {
		case 2:
			strStatus = "completed"
		case 1:
			strStatus = "running"
		case 0:
			strStatus = "pending"
		default:
			strStatus = "unknown"
		}
		if strStatus != "completed" {
			reduceCompleted = false
		}
		logline += fmt.Sprintf(statusFormat, fmt.Sprintf("reduce-%d", reduceIndex), strStatus)
	}
	logline += fmt.Sprintf("\nCompleted reduce? %t\n\n", reduceCompleted)
	logline += fmt.Sprintf("\nMapIsDone - ReduceIsDone = %t - %t\n", c.MapIsDone, c.ReduceIsDone)
	return logline
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		Files:             files,
		NReduce:           nReduce,
		MapStatus:         make(map[string]*JobProperties),
		ReduceStatus:      make(map[int]*JobProperties),
		IntermediateFiles: make(map[int][]string),
		Mutex:             &sync.Mutex{},
		WorkerTicker:      time.NewTicker(time.Second * 5),
	}

	for _, filename := range files {
		// init map status as "PENDING"
		c.MapStatus[filename] = &JobProperties{
			Status:  0,
			MaxTime: 45 * time.Second,
		}
	}

	go func() {
		for {
			file, _ := os.Create("coordinator-status.log")
			fmt.Fprintln(file, c.GetStatus())
			time.Sleep(1000 * time.Millisecond)
			file.Close()
		}
	}()

	go func() {
		for range c.WorkerTicker.C {
			c.Mutex.Lock()
			currentTime := time.Now().UTC()
			for filename, jobProperties := range c.MapStatus {
				if currentTime.Sub(jobProperties.CreatedTime) > jobProperties.MaxTime {
					// reset the job and have other workers do it
					log.Println("kill map job", filename)
					c.ModifyMapTask(filename, 0)
					c.MapIsDone = false
				}
			}
			for reduceIndex, jobProperties := range c.ReduceStatus {
				if currentTime.Sub(jobProperties.CreatedTime) > jobProperties.MaxTime {
					// reset the job and have other workers do it
					log.Println("kill reduce job", reduceIndex)
					c.ModifyReduceTask(reduceIndex, 0)
					c.ReduceIsDone = false
				}
			}
			c.Mutex.Unlock()
		}
	}()

	c.server()
	return &c
}
