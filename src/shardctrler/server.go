package shardctrler

import (
	"6.824/raft"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	lastOperations map[int64]LastOp
	notifyChans    map[int]chan *OpResponse

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *OpResponse {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *OpResponse, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) checkOutDateRequest(clientId int64, commandId int64) bool {
	opLog, ok := sc.lastOperations[clientId]
	return ok && commandId <= opLog.MaxAppliedId
}

func (sc *ShardCtrler) Join(args *OpRequest, reply *OpResponse) {
	sc.mu.RLock()
	if sc.checkOutDateRequest(args.ClientId, args.CommandId) {
		lastResponse := sc.lastOperations[args.ClientId].LastResponse
		reply.Err, reply.Config = lastResponse.Err, lastResponse.Config
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	sc.Query(args, reply)
}

func (sc *ShardCtrler) Leave(args *OpRequest, reply *OpResponse) {
	sc.mu.RLock()
	if sc.checkOutDateRequest(args.ClientId, args.CommandId) {
		lastResponse := sc.lastOperations[args.ClientId].LastResponse
		reply.Err, reply.Config = lastResponse.Err, lastResponse.Config
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	sc.Query(args, reply)
}

func (sc *ShardCtrler) Move(args *OpRequest, reply *OpResponse) {
	sc.mu.RLock()
	if sc.checkOutDateRequest(args.ClientId, args.CommandId) {
		lastResponse := sc.lastOperations[args.ClientId].LastResponse
		reply.Err, reply.Config = lastResponse.Err, lastResponse.Config
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	sc.Query(args, reply)
}

func (sc *ShardCtrler) Query(args *OpRequest, reply *OpResponse) {
	index, _, isLeader := sc.rf.Start(*args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()
	select {
	case result := <-ch:
		reply.Err, reply.Config = result.Err, result.Config
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	go func() {
		sc.mu.Lock()
		delete(sc.notifyChans, index)
		defer sc.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func deepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}

func (sc *ShardCtrler) applier() {
	for {
		message := <-sc.applyCh
		reply := new(OpResponse)
		args := message.Command.(OpRequest)
		sc.mu.Lock()
		if args.Type != OpQuery && sc.checkOutDateRequest(args.ClientId, args.CommandId) {
			reply = sc.lastOperations[args.ClientId].LastResponse
		} else {
			switch args.Type {
			case OpJoin:
				lastConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
				for gid, servers := range args.Servers {
					if _, ok := newConfig.Groups[gid]; !ok {
						newServers := make([]string, len(servers))
						copy(newServers, servers)
						newConfig.Groups[gid] = newServers
					}
				}
				for {
					mx, posx := -1, -1
					mi, posi := NShards+1, -1
					cnt := make(map[int]int)
					for _, gid := range newConfig.Shards {
						cnt[gid]++
					}
					for gid := range newConfig.Groups {
						value, ok := cnt[gid]
						if !ok {
							if mi > 0 || posi < gid {
								posi = gid
							}
							mi = 0
						} else {
							if value >= mx {
								if mx < value || posx < gid {
									posx = gid
								}
								mx = value
							}
							if value <= mi {
								if mi > value || posi < gid {
									posi = gid
								}
								mi = value
							}
						}
					}
					if _, ok := cnt[0]; ok {
						posx = 0
					}
					if posx != 0 && mx-mi <= 1 {
						break
					}
					for index, gid := range newConfig.Shards {
						if gid == posx {
							newConfig.Shards[index] = posi
							break
						}
					}
				}
				sc.configs = append(sc.configs, newConfig)
				reply.Err = OK
			case OpLeave:
				lastConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
				tmpShards := make([]int, 0)
				for _, gid := range args.GIDs {
					if _, ok := newConfig.Groups[gid]; ok {
						delete(newConfig.Groups, gid)
					}
					for index, tgid := range newConfig.Shards {
						if tgid == gid {
							tmpShards = append(tmpShards, index)
							newConfig.Shards[index] = 0
						}
					}
				}
				if len(newConfig.Groups) != 0 {
					for _, shard := range tmpShards {
						cnt := make(map[int]int)
						mi, posi := NShards+1, -1
						for _, gid := range newConfig.Shards {
							cnt[gid]++
						}
						for gid := range newConfig.Groups {
							value := cnt[gid]
							if value <= mi {
								if mi > value || posi < gid {
									posi = gid
								}
								mi = value
							}
						}
						newConfig.Shards[shard] = posi
					}
				}
				sc.configs = append(sc.configs, newConfig)
				reply.Err = OK
			case OpMove:
				lastConfig := sc.configs[len(sc.configs)-1]
				newConfig := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}
				newConfig.Shards[args.Shard] = args.GID
				sc.configs = append(sc.configs, newConfig)
				reply.Err = OK
			case OpQuery:
				if args.Num < 0 || args.Num >= len(sc.configs) {
					reply.Config, reply.Err = sc.configs[len(sc.configs)-1], OK
				} else {
					reply.Config, reply.Err = sc.configs[args.Num], OK
				}
			}
			if args.Type != OpQuery {
				sc.lastOperations[args.ClientId] = LastOp{args.CommandId, reply}
			}
		}
		if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
			ch := sc.getNotifyChan(message.CommandIndex)
			ch <- reply
		}
		sc.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	labgob.Register(OpRequest{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.notifyChans = make(map[int]chan *OpResponse)
	sc.lastOperations = make(map[int64]LastOp)
	go sc.applier()
	return sc
}
