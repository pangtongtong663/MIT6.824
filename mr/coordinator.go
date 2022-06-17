package mr

import (
	"log"
	"net/http"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"

type CoordinatorTaskStatus int

const (
	Idle CoordinatorTaskStatus = iota
	InProgress
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	// Your definitions here.
	TaskQueue        chan *Task
	TaskMeta         map[int]*CoordinatorTask
	CoordinatorPhase State
	NReduce          int
	InputFiles       []string
	Intermediates    [][]string
}

type CoordinatorTask struct {
	TaskStatus    CoordinatorTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

var mu sync.Mutex

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

func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()

	ret := c.CoordinatorPhase == Exit
	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:        make(chan *Task, max(len(files), nReduce)),
		TaskMeta:         make(map[int]*CoordinatorTask),
		CoordinatorPhase: Map,
		NReduce:          nReduce,
		InputFiles:       files,
		Intermediates:    make([][]string, nReduce),
	}

	for idx, filename := range files {
		taskMeta := Task{
			Input:      filename,
			TaskState:  Map,
			TaskNumber: idx,
			NReduce:    nReduce,
		}

		c.TaskQueue <- &taskMeta
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskReference: &taskMeta,
			TaskStatus:    Idle,
		}
	}

	c.server()

	go c.checkTimeOut()
	return &c
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	if len(c.TaskQueue) > 0 {
		*reply = *<-c.TaskQueue
		c.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		c.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if c.CoordinatorPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		*reply = Task{TaskState: Wait}
	}

	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()

	if task.TaskState != c.CoordinatorPhase || c.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		return nil
	}

	c.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go c.handleTaskResult(task)
	return nil
}

func (c *Coordinator) handleTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		for reduceNum, filePath := range task.Intermediates {
			c.Intermediates[reduceNum] = append(c.Intermediates[reduceNum], filePath)
		}

		if c.allTasksDone() {
			c.TaskMeta = make(map[int]*CoordinatorTask)
			for idx, files := range c.Intermediates {
				taskMeta := Task{
					TaskState:     Reduce,
					TaskNumber:    idx,
					NReduce:       c.NReduce,
					Intermediates: files,
				}
				c.TaskQueue <- &taskMeta
				c.TaskMeta[idx] = &CoordinatorTask{
					TaskStatus:    Idle,
					TaskReference: &taskMeta,
				}
			}
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.allTasksDone() {
			c.CoordinatorPhase = Exit
		}
	}
}

func (c *Coordinator) checkTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()

		if c.CoordinatorPhase == Exit {
			mu.Unlock()
			return
		}

		for _, coordinatorTask := range c.TaskMeta {
			if coordinatorTask.TaskStatus == InProgress && time.Now().Sub(coordinatorTask.StartTime) > 10*time.Second {
				coordinatorTask.TaskStatus = Idle
				c.TaskQueue <- coordinatorTask.TaskReference
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) allTasksDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
