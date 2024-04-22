package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y        int
	taskType TaskType
}

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type HeartbeatResponse struct {
}

type ReportRequest struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
