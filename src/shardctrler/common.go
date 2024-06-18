package shardctrler

import "time"

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const ExecuteTimeout = 500 * time.Millisecond

type Err uint8

const (
	OK Err = iota
	ErrNoKey
	ErrWrongLeader
	ErrTimeOut
)

type OpType uint8

const (
	OpJoin OpType = iota
	OpLeave
	OpMove
	OpQuery
)

type OpRequest struct {
	Type      OpType
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
	ClientId  int64
	CommandId int64
}

type OpResponse struct {
	Err    Err
	Config Config
}

type LastOp struct {
	MaxAppliedId int64
	LastResponse *OpResponse
}
