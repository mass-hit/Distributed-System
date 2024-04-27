package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const MaxTaskRuntime = time.Second * 10

type Task struct {
	FileName  string
	Id        int
	StartTime time.Time
	Status    TaskStatus
}

type Coordinator struct {
	Files   []string
	NReduce int
	NMap    int
	Phase   SchedulePhase
	Tasks   []Task

	HeartbeatCh chan HeartbeatMsg
	ReportCh    chan ReportMsg
	DoneCh      chan struct{}
}

type HeartbeatMsg struct {
	Response *HeartbeatResponse
	Done     chan struct{}
}

type ReportMsg struct {
	Request *ReportRequest
	Done    chan struct{}
}

func (c *Coordinator) Heartbeat(request *HeartbeatResponse, response *HeartbeatResponse) error {
	msg := HeartbeatMsg{response, make(chan struct{})}
	c.HeartbeatCh <- msg
	<-msg.Done
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := ReportMsg{request, make(chan struct{})}
	c.ReportCh <- msg
	<-msg.Done
	return nil
}

func (c *Coordinator) schedule() {
	c.initMapPhase()
	for {
		select {
		case msg := <-c.HeartbeatCh:
			if c.Phase == CompletePhase {
				msg.Response.TaskType = CompleteTask
			} else if c.selectTask(msg.Response) {
				switch c.Phase {
				case MapPhase:
					c.initReducePhase()
					c.selectTask(msg.Response)
				case ReducePhase:
					c.initCompletePhase()
					msg.Response.TaskType = CompleteTask
				default:
					panic("unhandled default case")
				}
			}
			msg.Done <- struct{}{}
		case msg := <-c.ReportCh:
			if msg.Request.Phase == c.Phase {
				c.Tasks[msg.Request.Id].Status = Finished
			}
			msg.Done <- struct{}{}
		}
	}
}

func (c *Coordinator) selectTask(response *HeartbeatResponse) bool {
	allFinished, hasNewTask := true, false
	for id, task := range c.Tasks {
		switch task.Status {
		case Idle:
			allFinished, hasNewTask = false, true
			c.Tasks[id].Status, c.Tasks[id].StartTime = Working, time.Now()
			response.NReduce, response.Id = c.NReduce, id
			if c.Phase == MapPhase {
				response.TaskType, response.FileName = MapTask, c.Files[id]
			} else {
				response.TaskType, response.NMap = ReduceTask, c.NMap
			}
		case Working:
			allFinished = false
			if time.Now().Sub(task.StartTime) > MaxTaskRuntime {
				hasNewTask = true
				c.Tasks[id].StartTime = time.Now()
				response.NReduce, response.Id = c.NReduce, id
				if c.Phase == MapPhase {
					response.TaskType, response.FileName = MapTask, c.Files[id]
				} else {
					response.TaskType, response.NMap = ReduceTask, c.NMap
				}
			}
		}
		if hasNewTask {
			break
		}
	}
	if !hasNewTask {
		response.TaskType = WaitTask
	}
	return allFinished
}

func (c *Coordinator) initMapPhase() {
	c.Phase = MapPhase
	c.Tasks = make([]Task, len(c.Files))
	for index, file := range c.Files {
		c.Tasks[index] = Task{
			FileName: file,
			Id:       index,
			Status:   Idle,
		}
	}
}

func (c *Coordinator) initReducePhase() {
	c.Phase = ReducePhase
	c.Tasks = make([]Task, c.NReduce)
	for i := 0; i < c.NReduce; i++ {
		c.Tasks[i] = Task{
			Id:     i,
			Status: Idle,
		}
	}
}

func (c *Coordinator) initCompletePhase() {
	c.Phase = CompletePhase
	c.DoneCh <- struct{}{}
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
	<-c.DoneCh
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:       files,
		NReduce:     nReduce,
		NMap:        len(files),
		HeartbeatCh: make(chan HeartbeatMsg),
		ReportCh:    make(chan ReportMsg),
		DoneCh:      make(chan struct{}),
	}
	c.server()
	go c.schedule()
	return &c
}
