package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	sc           *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	lastApplied    int
	currentConfig  shardctrler.Config
	lastConfig     shardctrler.Config
	lastOperations map[int64]LastOp
	shards         map[int]*Shard
	notifyChans    map[int]chan *OpResponse
}

func (kv *ShardKV) checkOutDateRequest(clientId int64, commandId int64) bool {
	opLog, ok := kv.lastOperations[clientId]
	return ok && commandId <= opLog.MaxAppliedId
}

func (kv *ShardKV) canServe(shardId int) bool {
	return kv.currentConfig.Shards[shardId] == kv.gid && kv.shards[shardId].Status == Serving
}

func (kv *ShardKV) getNotifyChan(index int) chan *OpResponse {
	if _, ok := kv.notifyChans[index]; !ok {
		kv.notifyChans[index] = make(chan *OpResponse, 1)
	}
	return kv.notifyChans[index]
}

func (kv *ShardKV) getShardIdsByStatus(status ShardStatus) map[int][]int {
	gidShardIdsMap := make(map[int][]int)
	for i, shard := range kv.shards {
		if shard.Status == status {
			gid := kv.lastConfig.Shards[i]
			if gid != 0 {
				if _, ok := gidShardIdsMap[gid]; !ok {
					gidShardIdsMap[gid] = make([]int, 0)
				}
				gidShardIdsMap[gid] = append(gidShardIdsMap[gid], i)
			}
		}
	}
	return gidShardIdsMap
}

func (kv *ShardKV) Get(args *OpRequest, reply *OpResponse) {
	kv.mu.RLock()
	if !kv.canServe(key2shard(args.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(Command{Operation, *args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeOut
	}
	go func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		defer kv.mu.Unlock()
	}()
}

func (kv *ShardKV) PutAppend(args *OpRequest, reply *OpResponse) {
	kv.mu.RLock()
	if kv.checkOutDateRequest(args.ClientId, args.CommandId) {
		lastResponse := kv.lastOperations[args.ClientId].LastResponse
		reply.Value, reply.Err = lastResponse.Value, lastResponse.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.Get(args, reply)
}

func (kv *ShardKV) GetShards(args *ShardRequest, reply *ShardResponse) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.currentConfig.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}
	reply.Shards = make(map[int]map[string]string)
	for _, shardId := range args.ShardIds {
		reply.Shards[shardId] = make(map[string]string)
		for k, v := range kv.shards[shardId].KV {
			reply.Shards[shardId][k] = v
		}
	}
	reply.LastOperations = make(map[int64]LastOp)
	for clientId, operation := range kv.lastOperations {
		reply.LastOperations[clientId] = LastOp{operation.MaxAppliedId, &OpResponse{operation.LastResponse.Err, operation.LastResponse.Value}}
	}
	reply.ConfigNum, reply.Err = args.ConfigNum, OK
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		for i := 0; i < shardctrler.NShards; i++ {
			if _, ok := kv.shards[i]; !ok {
				kv.shards[i] = &Shard{make(map[string]string), Serving}
			}
		}
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shards map[int]*Shard
	var lastOperation map[int64]LastOp
	var currentConfig shardctrler.Config
	var lastConfig shardctrler.Config
	d.Decode(&shards)
	d.Decode(&lastOperation)
	d.Decode(&currentConfig)
	d.Decode(&lastConfig)
	kv.shards, kv.lastOperations, kv.currentConfig, kv.lastConfig = shards, lastOperation, currentConfig, lastConfig
}

func (kv *ShardKV) applier() {
	for {
		message := <-kv.applyCh
		if message.CommandValid {
			kv.mu.Lock()
			if message.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = message.CommandIndex
			reply := new(OpResponse)
			args := message.Command.(Command)
			switch args.Type {
			case Operation:
				opRequest := args.Data.(OpRequest)
				shardId := key2shard(opRequest.Key)
				if kv.canServe(shardId) {
					if opRequest.Type != OpGet && kv.checkOutDateRequest(opRequest.ClientId, opRequest.CommandId) {
						reply = kv.lastOperations[opRequest.ClientId].LastResponse
					} else {
						switch opRequest.Type {
						case OpGet:
							if value, ok := kv.shards[shardId].KV[opRequest.Key]; ok {
								reply.Value, reply.Err = value, OK
							} else {
								reply.Value, reply.Err = "", ErrNoKey
							}
						case OpPut:
							kv.shards[shardId].KV[opRequest.Key] = opRequest.Value
							reply.Value, reply.Err = "", OK
						case OpAppend:
							kv.shards[shardId].KV[opRequest.Key] += opRequest.Value
							reply.Value, reply.Err = "", OK
						}
					}
					if opRequest.Type != OpGet {
						kv.lastOperations[opRequest.ClientId] = LastOp{opRequest.CommandId, reply}
					}
				} else {
					reply.Err = ErrWrongGroup
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- reply
				}
			case Configuration:
				newConfig := args.Data.(shardctrler.Config)
				if newConfig.Num == kv.currentConfig.Num+1 {
					for i := 0; i < shardctrler.NShards; i++ {
						if kv.currentConfig.Shards[i] != kv.gid && newConfig.Shards[i] == kv.gid {
							gid := kv.currentConfig.Shards[i]
							if gid != 0 {
								kv.shards[i].Status = Pulling
							}
						}
						if kv.currentConfig.Shards[i] == kv.gid && newConfig.Shards[i] != kv.gid {
							gid := newConfig.Shards[i]
							if gid != 0 {
								kv.shards[i].Status = BePulling
							}
						}
					}
					kv.lastConfig = kv.currentConfig
					kv.currentConfig = newConfig
				}
			case GetShards:
				shardsInfo := args.Data.(ShardResponse)
				if shardsInfo.ConfigNum == kv.currentConfig.Num {
					for shardId, shard := range shardsInfo.Shards {
						if kv.shards[shardId].Status == Pulling {
							for key, value := range shard {
								kv.shards[shardId].KV[key] = value
							}
							kv.shards[shardId].Status = Serving
						}
					}
					for clientId, operation := range shardsInfo.LastOperations {
						if lastOp, ok := kv.lastOperations[clientId]; !ok || lastOp.MaxAppliedId < operation.MaxAppliedId {
							kv.lastOperations[clientId] = operation
						}
					}
				}
			}
			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() >= kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.shards)
				e.Encode(kv.lastOperations)
				e.Encode(kv.currentConfig)
				e.Encode(kv.lastConfig)
				kv.rf.Snapshot(message.CommandIndex, w.Bytes())
			}
			kv.mu.Unlock()
		} else if message.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
				kv.readPersist(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) ticker() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			flag := true
			for _, shard := range kv.shards {
				if shard.Status == Pulling {
					flag = false
					break
				}
			}
			currentConfigNum := kv.currentConfig.Num
			kv.mu.RUnlock()
			if flag {
				newConfig := kv.sc.Query(currentConfigNum + 1)
				if newConfig.Num == currentConfigNum+1 {
					kv.rf.Start(Command{Configuration, newConfig})
				}
			}
		}
		time.Sleep(ConfigTimeout)
	}
}

func (kv *ShardKV) monitor() {
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.mu.RLock()
			gidShardIdsMap := kv.getShardIdsByStatus(Pulling)
			var wg sync.WaitGroup
			for gid, shardIds := range gidShardIdsMap {
				wg.Add(1)
				go func(servers []string, configNum int, shardIds []int) {
					defer wg.Done()
					args := &ShardRequest{configNum, shardIds}
					for _, server := range servers {
						reply := new(ShardResponse)
						if kv.make_end(server).Call("ShardKV.GetShards", args, reply) && reply.Err == OK {
							kv.rf.Start(Command{GetShards, *reply})
						}
					}
				}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
			}
			kv.mu.RUnlock()
			wg.Wait()
		}
		time.Sleep(ShardsTimeOut)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OpRequest{})
	labgob.Register(Command{})
	labgob.Register(ShardRequest{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardResponse{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.lastApplied = 0

	kv.sc = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.shards = make(map[int]*Shard)
	kv.lastOperations = make(map[int64]LastOp)
	kv.notifyChans = make(map[int]chan *OpResponse)
	kv.currentConfig = shardctrler.Config{Groups: make(map[int][]string)}
	kv.lastConfig = shardctrler.Config{Groups: make(map[int][]string)}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readPersist(persister.ReadSnapshot())
	go kv.applier()
	go kv.ticker()
	go kv.monitor()
	return kv
}
