package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	AllFilesName     map[string]int
	MapTaskNumCount  int
	NReduce          int        // n reduce task
	InterFilename    [][]string // intermediate file
	MapFinished      bool
	ReduceTaskStatus map[int]int // about reduce tasks' status
	ReduceFinished   bool        // Finish the reduce task
	RWLock           *sync.RWMutex
}

// tasks' status
// UnMapped    ---->     UnMaped
// Mapped      ---->     be maped to a worker
// Finished       ---->     worker finish the map task
const (
	UnMapped = iota
	Mapped
	Finished
)

var mapChannel chan string // chan for map task
var reducetasks chan int   // chan for reduce task

// MyCallHandler func
// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) MyCallHandler(args *MyArgs, reply *MyReply) error {
	msgType := args.MessageType
	switch msgType {
	case MsgForTask:
		select {
		case filename := <-mapChannel:
			// allocate map task
			reply.Filename = filename
			reply.MapNumMapped = c.MapTaskNumCount
			reply.NReduce = c.NReduce
			reply.TaskType = "map"

			c.RWLock.Lock()
			c.AllFilesName[filename] = Mapped
			c.MapTaskNumCount++
			c.RWLock.Unlock()
			go c.timerForWorker("map", filename)
			return nil

		case reduceNum := <-reducetasks:
			// allocate reduce task
			reply.TaskType = "reduce"
			reply.ReduceFileList = c.InterFilename[reduceNum]
			reply.NReduce = c.NReduce
			reply.ReduceNumMapped = reduceNum

			c.RWLock.Lock()
			c.ReduceTaskStatus[reduceNum] = Mapped
			c.RWLock.Unlock()
			go c.timerForWorker("reduce", strconv.Itoa(reduceNum))
			return nil
		}
	case MsgForFinishMap:
		c.RWLock.Lock()
		defer c.RWLock.Unlock()
		c.AllFilesName[args.MessageCnt] = Finished
	case MsgForFinishReduce:
		index, _ := strconv.Atoi(args.MessageCnt)
		c.RWLock.Lock()
		defer c.RWLock.Unlock()
		c.ReduceTaskStatus[index] = Finished
	}
	return nil
}

// MyInnerFileHandler : intermediate files' handler
func (c *Coordinator) MyInnerFileHandler(args *MyIntermediateFile, reply *MyReply) error {
	nReduceNum := args.NReduceType
	filename := args.MessageCnt

	c.InterFilename[nReduceNum] = append(c.InterFilename[nReduceNum], filename)
	return nil
}

// timerForWorker : monitor the worker
func (c *Coordinator) timerForWorker(taskType, identify string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if taskType == "map" {
				c.RWLock.Lock()
				c.AllFilesName[identify] = UnMapped
				c.RWLock.Unlock()
				mapChannel <- identify
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				c.RWLock.Lock()
				c.ReduceTaskStatus[index] = UnMapped
				c.RWLock.Unlock()
				reducetasks <- index
			}
			return
		default:
			if taskType == "map" {
				c.RWLock.RLock()
				if c.AllFilesName[identify] == Finished {
					c.RWLock.RUnlock()
					return
				} else {
					c.RWLock.RUnlock()
				}
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(identify)
				c.RWLock.RLock()
				if c.ReduceTaskStatus[index] == Finished {
					c.RWLock.RUnlock()
					return
				} else {
					c.RWLock.RUnlock()
				}
			}
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	mapChannel = make(chan string, 5)
	reducetasks = make(chan int, 5)
	rpc.Register(c)
	rpc.HandleHTTP()
	go c.generateTask()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// GenerateTask : create tasks
func (c *Coordinator) generateTask() {
	for k, v := range c.AllFilesName {
		if v == UnMapped {
			mapChannel <- k
		}
	}
	ok := false
	for !ok {
		ok = checkAllMapTask(c)
	}

	c.MapFinished = true

	for k, v := range c.ReduceTaskStatus {
		if v == UnMapped {
			reducetasks <- k
		}
	}

	ok = false
	for !ok {
		ok = checkAllReduceTask(c)
	}
	c.ReduceFinished = true
}

func checkAllMapTask(c *Coordinator) bool {
	c.RWLock.RLock()
	defer c.RWLock.RUnlock()
	for _, v := range c.AllFilesName {
		if v != Finished {
			return false
		}
	}
	return true
}

func checkAllReduceTask(c *Coordinator) bool {
	c.RWLock.RLock()
	defer c.RWLock.RUnlock()
	for _, v := range c.ReduceTaskStatus {
		if v != Finished {
			return false
		}
	}
	return true
}

//func (c *Coordinator) requestJob() MapJob {
//	for fileName, status := range c.mapStatus {
//		if status == 0 {
//			return MapJob{fileName, c.mapTaskId, c.nReducer}
//		} else if status == 1 {
//			return MapJob{}
//		}
//	}
//}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.ReduceFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.AllFilesName = make(map[string]int)
	c.MapTaskNumCount = 0
	c.NReduce = nReduce
	c.MapFinished = false
	c.ReduceFinished = false
	c.ReduceTaskStatus = make(map[int]int)
	c.InterFilename = make([][]string, c.NReduce)
	c.RWLock = new(sync.RWMutex)
	for _, v := range files {
		c.AllFilesName[v] = UnMapped
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTaskStatus[i] = UnMapped
	}

	c.server()
	return &c
}
