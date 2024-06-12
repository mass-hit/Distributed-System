package kvraft

import "time"

type Err uint8

const ExecuteTimeout = 500 * time.Millisecond

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeOut
)

type LastOp struct {
	MaxAppliedId int64
	LastResponse *OpResponse
}

type Command struct {
	*OpRequest
}

type OpType uint8

const (
	OpGet OpType = iota
	OpPut
	OpAppend
)

type OpRequest struct {
	Type      OpType
	Key       string
	Value     string
	ClientId  int64
	CommandId int64
}

type OpResponse struct {
	Err   Err
	Value string
}
