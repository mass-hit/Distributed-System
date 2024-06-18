package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int64
	clientId  int64
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.commandId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &OpRequest{
		Type:      OpQuery,
		Num:       num,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	for {
		reply := new(OpResponse)
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &OpRequest{
		Type:      OpJoin,
		Servers:   servers,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	for {
		reply := new(OpResponse)
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &OpRequest{
		Type:      OpLeave,
		GIDs:      gids,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	for {
		reply := new(OpResponse)
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &OpRequest{
		Type:      OpMove,
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
	}
	for {
		reply := new(OpResponse)
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeOut {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++
		return
	}
}
