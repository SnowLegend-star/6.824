package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId     int64 //每个Client的编号
	commandIndex int   //记录这个op的index
	lastLeader   int   //记录上一次RPC的Leader
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
	// Your code here.
	ck.clientId = nrand()
	ck.commandIndex = 0
	ck.lastLeader = 0 //Leader初始化为0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	args.CommandIndex = ck.commandIndex
	var reply QueryReply
	ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Query", args, &reply)
	Debug(dClerk, "Clerk %v向Server %v发送args: %v", ck.clientId, ck.lastLeader, *args)
	if ok && reply.WrongLeader == false && reply.Err == OK {
		Debug(dClerk, "Clerk %v成功收到来自Server %v args: %v的处理结果", ck.clientId, ck.lastLeader, *args)
		ck.commandIndex++ //成功了就依次增加命令的编号
		return reply.Config
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			Debug(dClerk, "Clerk %v向Server %v发送args: %v", ck.clientId, i, *args)
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				Debug(dClerk, "Clerk %v成功收到来自Server %v args: %v的处理结果", ck.clientId, i, *args)
				ck.commandIndex++ //成功了就依次增加命令的编号
				ck.lastLeader = i
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	args.CommandIndex = ck.commandIndex
	var reply JoinReply
	ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Join", args, &reply)
	Debug(dClerk, "Clerk %v向Server %v发送args: %v", ck.clientId, ck.lastLeader, args)
	if ok && reply.WrongLeader == false && reply.Err == OK {
		Debug(dClerk, "Clerk %v成功收到来自Server %v对args: %v的处理结果", ck.clientId, ck.lastLeader, args)
		ck.commandIndex++ //成功了就依次增加命令的编号
		return
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			Debug(dClerk, "Clerk %v向Server %v发送args: %v", ck.clientId, i, *args)
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				Debug(dClerk, "Clerk %v成功收到来自Server %v args: %v的处理结果", ck.clientId, i, *args)
				ck.commandIndex++
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	args.CommandIndex = ck.commandIndex
	var reply LeaveReply
	ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Leave", args, &reply)
	Debug(dClerk, "Clerk %v向Server %v发送args: %v", ck.clientId, ck.lastLeader, args)
	if ok && reply.WrongLeader == false && reply.Err == OK {
		Debug(dClerk, "Clerk %v成功收到来自Server %v args: %v的处理结果", ck.clientId, ck.lastLeader, args)
		ck.commandIndex++ //成功了就依次增加命令的编号
		return
	}

	for {
		// try each known server.
		for i, srv := range ck.servers {
			Debug(dClerk, "Clerk %v向Server %v发送args: %v", ck.clientId, i, *args)
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				Debug(dClerk, "Clerk %v成功收到来自Server %v args: %v的处理结果", ck.clientId, i, *args)
				ck.commandIndex++
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	args.CommandIndex = ck.commandIndex
	var reply MoveReply
	Debug(dClerk, "Clerk %v向Server %v发送args: %v", ck.clientId, ck.lastLeader, args)
	ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Move", args, &reply)
	if ok && reply.WrongLeader == false && reply.Err == OK {
		Debug(dClerk, "Clerk %v成功收到来自Server %v args: %v的处理结果", ck.clientId, ck.lastLeader, args)
		ck.commandIndex++ //成功了就依次增加命令的编号
		return
	}
	for {
		// try each known server.
		for i, srv := range ck.servers {
			Debug(dClerk, "Clerk %v向Server %v发送args: %v", ck.clientId, i, *args)
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				Debug(dClerk, "Clerk %v成功收到来自Server %v args: %v的处理结果", ck.clientId, i, *args)
				ck.commandIndex++
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
