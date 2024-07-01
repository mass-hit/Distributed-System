package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	ExecuteTimeout = 500 * time.Millisecond
	ConfigTimeout  = 100 * time.Millisecond
	ShardsTimeOut  = 50 * time.Millisecond
)

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongGroup
	ErrWrongLeader
	ErrTimeOut
	ErrOutDated
	ErrNotReady
)

type ShardStatus uint8

const (
	Serving ShardStatus = iota
	Pulling
	BePulling
)

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

type LastOp struct {
	MaxAppliedId int64
	LastResponse *OpResponse
}

type OpType uint8

const (
	OpPut OpType = iota
	OpAppend
	OpGet
)

type CommandType uint8

const (
	Operation CommandType = iota
	Configuration
	GetShards
)

type Command struct {
	Type CommandType
	Data interface{}
}

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

type ShardRequest struct {
	ConfigNum int
	ShardIds  []int
}

type ShardResponse struct {
	Err            Err
	ConfigNum      int
	Shards         map[int]map[string]string
	LastOperations map[int64]LastOp
}
