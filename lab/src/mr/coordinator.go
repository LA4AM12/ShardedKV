package mr

import (
	"log"
	"strconv"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Task struct {
	id        int
	fileName  string
	startTime time.Time
	status    TaskStatus
}

type TaskStatus int

const (
	Idle = iota
	Processing
	Completed
)

type Coordinator struct {
	// Your definitions here.
	files       []string
	nMap        int
	nReduce     int
	phase       SchedulePhase
	tasks       []Task
	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

type SchedulePhase int

const (
	MapPhase = iota
	ReducePhase
)

type JobType int

const (
	MapJob = iota
	ReduceJob
	WaitJob
	ShutDownJob
)

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HeartBeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok //wait until response is ready
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok //
	return nil
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	select {
	case <-c.doneCh:
		ret = true
	default:
	}
	return ret
}

func (c *Coordinator) initMapPhase(files []string, nReduce int) {
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = MapPhase
	c.heartbeatCh = make(chan heartbeatMsg)
	c.reportCh = make(chan reportMsg)
	c.doneCh = make(chan struct{})

	c.tasks = make([]Task, c.nMap)
	for i := range c.tasks {
		c.tasks[i] = Task{i, files[i], time.Now(), Idle}
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase

	c.tasks = make([]Task, c.nReduce)
	for i := range c.tasks {
		c.tasks[i] = Task{i, "mr-*-" + strconv.Itoa(i), time.Now(), Idle}
	}
}

func (c *Coordinator) schedule() {
	for {
		select {
		case msg := <-c.heartbeatCh:
			response := msg.response
			switch c.phase {
			case MapPhase:
				if c.tryAssignTask(response) {
					log.Printf("map task %d was assigned\n", response.TaskId)
					response.JobType = MapJob
					response.NReduce = c.nReduce
				} else {
					log.Printf("waiting for map task done\n")
					response.JobType = WaitJob
				}
			case ReducePhase:
				if c.tryAssignTask(response) {
					log.Printf("reduce task %d was assigned\n", response.TaskId)
					response.JobType = ReduceJob
				} else {
					log.Printf("waiting for reduce task done\n")
					response.JobType = WaitJob
				}
			}
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			req := msg.request
			switch c.phase {
			case MapPhase:
				if req.JobType == MapJob && !c.isTimeout(c.tasks[req.TaskId]) {
					log.Printf("map task %d completed \n", req.TaskId)
					c.tasks[req.TaskId].status = Completed
					c.nMap--
					if c.nMap == 0 {
						log.Printf("map job was done! \n")
						c.initReducePhase()
					}
				}
			case ReducePhase:
				if req.JobType == ReduceJob && !c.isTimeout(c.tasks[req.TaskId]) {
					log.Printf("reduce task %d completed \n", req.TaskId)
					c.tasks[req.TaskId].status = Completed
					c.nReduce--
					if c.nReduce == 0 {
						log.Printf("reduce job was done! \n")
						c.doneCh <- struct{}{}
					}
				}
			}
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) tryAssignTask(res *HeartbeatResponse) bool {

	for i, _ := range c.tasks {
		if c.isTimeout(c.tasks[i]) {
			c.tasks[i].status = Idle
		}
		if c.tasks[i].status == Idle {
			c.tasks[i].status = Processing
			c.tasks[i].startTime = time.Now()
			res.FileName = c.tasks[i].fileName
			res.TaskId = c.tasks[i].id
			return true
		}
	}
	return false
}

func (c *Coordinator) isTimeout(task Task) bool {
	// lazy check
	if task.status == Processing && task.startTime.Add(10*time.Second).Before(time.Now()) {
		log.Printf("task %d timeout\n", task.id)
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.initMapPhase(files, nReduce)
	go c.schedule()
	c.server()
	return &c
}
