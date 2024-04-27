package mr

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	CompleteTask
)

type SchedulePhase int

const (
	MapPhase SchedulePhase = iota
	ReducePhase
	CompletePhase
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	Working
	Finished
)
