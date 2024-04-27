package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

type HeartbeatRequest struct {
}

type HeartbeatResponse struct {
	FileName string
	TaskType TaskType
	Id       int
	NReduce  int
	NMap     int
}

type ReportRequest struct {
	Id    int
	Phase SchedulePhase
}

type ReportResponse struct{}

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
