package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu      sync.RWMutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int

	// Your definitions here.
	memoryKV       map[string]string
	notifyChan     map[int]chan *OpResponse
	lastOperations map[int64]LastOp
}

func (kv *KVServer) checkOutDateRequest(clientId int64, commandId int64) bool {
	opLog, ok := kv.lastOperations[clientId]
	return ok && commandId <= opLog.MaxAppliedId
}

func (kv *KVServer) getNotifyChan(index int) chan *OpResponse {
	if _, ok := kv.notifyChan[index]; !ok {
		kv.notifyChan[index] = make(chan *OpResponse, 1)
	}
	return kv.notifyChan[index]
}

func (kv *KVServer) Get(args *OpRequest, reply *OpResponse) {
	index, _, isLeader := kv.rf.Start(*args)
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
		delete(kv.notifyChan, index)
		defer kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *OpRequest, reply *OpResponse) {
	kv.mu.RLock()
	if kv.checkOutDateRequest(args.ClientId, args.CommandId) {
		lastResponse := kv.lastOperations[args.ClientId].LastResponse
		reply.Err, reply.Value = lastResponse.Err, lastResponse.Value
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.Get(args, reply)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		message := <-kv.applyCh
		kv.mu.Lock()
		if message.CommandIndex <= kv.lastApplied {
			kv.mu.Unlock()
			continue
		}
		kv.lastApplied = message.CommandIndex
		reply := new(OpResponse)
		args := message.Command.(OpRequest)
		if args.Type != OpGet && kv.checkOutDateRequest(args.ClientId, args.CommandId) {
			reply = kv.lastOperations[args.ClientId].LastResponse
		} else {
			switch args.Type {
			case OpGet:
				if value, ok := kv.memoryKV[args.Key]; ok {
					reply.Value, reply.Err = value, OK
				} else {
					reply.Value, reply.Err = "", ErrNoKey
				}
			case OpPut:
				kv.memoryKV[args.Key] = args.Value
				reply.Value, reply.Err = "", OK
			case OpAppend:
				kv.memoryKV[args.Key] += args.Value
				reply.Value, reply.Err = "", OK
			}
			if args.Type != OpGet {
				kv.lastOperations[args.ClientId] = LastOp{args.CommandId, reply}
			}
		}
		if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
			ch := kv.getNotifyChan(message.CommandIndex)
			ch <- reply
		}
		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OpRequest{})
	kv := new(KVServer)
	kv.maxraftstate = maxraftstate
	kv.lastApplied = 0
	kv.dead = 0
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.memoryKV = make(map[string]string)
	kv.notifyChan = make(map[int]chan *OpResponse)
	kv.lastOperations = make(map[int64]LastOp)
	go kv.applier()
	return kv
}
