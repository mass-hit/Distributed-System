package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type RequestVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotRequest struct {
	Term         int
	LeaderId     int
	LastLogIndex int
	LastLogTerm  int
	Data         []byte
}

type InstallSnapshotResponse struct {
	Term int
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

type lockRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockRand{rand: rand.New(rand.NewSource(time.Now().UnixNano()))}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	HeartBeatTimeout = 125
	ElectionTimeout  = 1000
)

func StableHeartBeatTimeout() time.Duration {
	return time.Duration(HeartBeatTimeout) * time.Millisecond
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}
