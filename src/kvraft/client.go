package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.lastLeader = 0 //默认初始化lastLeader是0
	ck.commandIndex = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	i := ck.lastLeader
	for {
		args := GetArgs{
			Key:          key,
			ClientId:     ck.clientId,
			CommandIndex: ck.commandIndex,
		}
		reply := GetReply{}
		DPrintf("Clerk准备发送Get():%v key:%v ->kvserver %v", key, args, i)
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			DPrintf("Clerk发送Get():%v ->kvserver %v失败", args, i)
		}
		if ok {

			// if reply.Err != ErrWrongLeader {
			//
			// }
			//只有两种情况可以return
			if reply.Err == OK {
				DPrintf("Clerk成功收到了Get(): %v", reply)
				ck.lastLeader = i
				ck.commandIndex++
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
		}
		//说明两种可能的情况
		//1、无法联系到该kvserver
		//2、kvserver的peer不是Leader
		//3、超时

		i = (i + 1) % len(ck.servers)

	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	i := ck.lastLeader
	for {
		// time.Sleep(time.Millisecond * 1000)  You want to destory me? fxxking Sleep()
		args := PutAppendArgs{Key: key,
			Value:         value,
			ClientId:      ck.clientId,
			CommandIndex:  ck.commandIndex,
			OperationType: op,
		}
		reply := PutAppendReply{}
		DPrintf("Clerk准备发送%v():%v key:%v value:%v ->kvserver %v", op, key, value, args, i)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			DPrintf("Clerk发送%v():%v ->kvserver %v失败", op, args, i)
		}

		if ok {

			if reply.Err == OK {
				DPrintf("Clerk成功收到了%v(): %v", args, reply)
				ck.lastLeader = i
				ck.commandIndex++
				return
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
				i = (i + 1) % len(ck.servers)
				continue
			}
		}

		i = (i + 1) % len(ck.servers)

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
