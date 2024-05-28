package mr

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	beginTime   time.Time
	mapIndex    int
	reduceIndex int
	FileName    string
	R           int
	M           int
}

func (t *Task) GenerateTaskInfo(s int) TaskState {
	state := TaskState{}
	state.M = t.M
	state.R = t.R
	state.MapIndex = t.mapIndex
	state.ReduceIndex = t.reduceIndex
	state.FileName = t.FileName
	switch s {
	case _map:
		state.State = _map
	case _reduce:
		state.State = _reduce
	default:
		panic("unhandled default case")
	}
	return state
}
func (t *Task) TimeOut() bool {
	return time.Now().Sub(t.beginTime) > time.Second*10
}

func (t *Task) SetTime() {
	t.beginTime = time.Now()
}

type TaskQueue struct {
	taskArray []Task
	mutex     sync.Mutex
}

func (t *TaskQueue) Len() int {
	return len(t.taskArray)
}

func (t *TaskQueue) Pop() (Task, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	taskSize := len(t.taskArray)
	if taskSize == 0 {
		return Task{}, errors.New("task queue is empty")
	}
	popTask := t.taskArray[taskSize-1]
	t.taskArray = t.taskArray[:taskSize-1]
	return popTask, nil
}

func (t *TaskQueue) Push(task Task) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if reflect.DeepEqual(task, Task{}) {
		return
	}
	t.taskArray = append(t.taskArray, task)
}

func (t *TaskQueue) TimeOut() []Task {
	timeoutArray := make([]Task, 0)
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for i := 0; i < len(t.taskArray); {
		task := t.taskArray[i]
		if task.TimeOut() {
			timeoutArray = append(timeoutArray, task)
			t.taskArray = append(t.taskArray[:i], t.taskArray[i+1:]...)
		} else {
			i++
		}
	}
	return timeoutArray
}

func (t *TaskQueue) RemoveTask(fileIndex, partIndex int) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	for i := 0; i < len(t.taskArray); {
		task := t.taskArray[i]
		if fileIndex == task.mapIndex && partIndex == task.reduceIndex {
			t.taskArray = append(t.taskArray[:i], t.taskArray[i+1:]...)
			break
		} else {
			i++
		}
	}
}

type Coordinator struct {
	// Your definitions here.
	fileName []string

	//map and reduce task state
	idleMapTask       TaskQueue //waiting task queue
	inProgressMapTask TaskQueue //Allocated unfinished working task queue
	// if len(inProgressMapTask) == 0 {map task is over }
	idleReduceTask       TaskQueue
	inProgressReduceTask TaskQueue

	isDone bool
	R      int
}

// Your code here -- RPC handlers for the worker to call.

// HandleTaskRequest coordinator do task request
func (c *Coordinator) HandleTaskRequest(args *ExampleArgs, reply *TaskState) error {
	if c.isDone {
		return nil
	}

	reduceTask, err := c.idleReduceTask.Pop()
	if err == nil {
		reduceTask.SetTime()
		c.inProgressReduceTask.Push(reduceTask)
		*reply = reduceTask.GenerateTaskInfo(_reduce)
		log.Println("allocate a task to reduce to do")
		return nil
	}

	mapTask, err := c.idleMapTask.Pop()
	if err == nil {
		mapTask.SetTime()
		c.inProgressMapTask.Push(mapTask)
		*reply = mapTask.GenerateTaskInfo(_map)
		log.Println("allocate a task to map to do")
		return nil
	}

	if c.inProgressMapTask.Len() == 0 || c.inProgressReduceTask.Len() == 0 {
		reply.State = _wait
		return nil
	}
	reply.State = _end
	c.isDone = true
	return nil
}

func (c *Coordinator) TaskDone(args *TaskState, reply *ExampleReply) error {
	switch args.State {
	case _map:
		log.Printf("Map task %Vth file %v complete", args.MapIndex, args.FileName)
		c.inProgressMapTask.RemoveTask(args.MapIndex, args.ReduceIndex)
		if c.inProgressMapTask.Len() == 0 && c.idleMapTask.Len() == 0 {
			//如若已经没有maptask，分配reduce
			c.distributeReduceTask()
		}
	case _reduce:
		log.Printf("Reduce task on %vth part complete\n", args.ReduceIndex)
		c.inProgressReduceTask.RemoveTask(args.MapIndex, args.ReduceIndex)
	default:
		panic("unhandled default case")
	}
	return nil
}

func (c *Coordinator) distributeReduceTask() {
	for i := 0; i < c.R; i++ {
		task := Task{
			mapIndex:    0,
			reduceIndex: i,
			FileName:    "",
			R:           c.R,
			M:           len(c.fileName),
		}

		c.idleReduceTask.Push(task)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	// Your code here.

	return c.isDone
}

func (c *Coordinator) findTaskTimeOut() {
	for {
		time.Sleep(time.Second * 5)
		mapTask := c.inProgressMapTask.TimeOut()
		if len(mapTask) > 0 {
			for _, v := range mapTask {
				c.idleMapTask.Push(v)
			}
			mapTask = nil
		}

		reduceTask := c.inProgressReduceTask.TimeOut()
		if len(reduceTask) > 0 {
			for _, v := range reduceTask {
				c.idleReduceTask.Push(v)
			}
			reduceTask = nil
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	mapArray := make([]Task, 0)
	for i, fileName := range files {
		mapTask := Task{
			mapIndex:    i,
			reduceIndex: 0,
			FileName:    fileName,
			R:           nReduce,
			M:           len(files),
		}
		mapArray = append(mapArray, mapTask)
	}

	if _, err := os.Stat("mr-tmp"); os.IsNotExist(err) {
		err = os.Mkdir("mr-tmp", os.ModePerm)
		if err != nil {
			fmt.Printf("Create tmp directory failed... Error: %v\n", err)
			panic("Create tmp directory failed...")
		}
	}
	c := Coordinator{
		fileName:    files,
		idleMapTask: TaskQueue{taskArray: mapArray},
		isDone:      false,
		R:           nReduce,
	}
	go c.findTaskTimeOut()
	c.server()
	return &c
}
