package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	identifier []int64 //每个请求的标识符集合
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.identifier = make([]int64, 0)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	id := nrand()
	args := GetArgs{
		Key:        key,
		Identifier: id,
	}
	reply := GetReply{}

	ok := ck.server.Call("KVServer.Get", &args, &reply)

	for ok == false {
		// fmt.Println("Get操作失败,自动重试")
		ok = ck.server.Call("KVServer.Get", &args, &reply)

	}

	argsTaskComplete := TaskCompleteArgs{
		Identifier: id,
	}
	replyTaskComplete := TaskCompleteReply{}
	ok = ck.server.Call("KVServer.TaskComplete_Handeler", &argsTaskComplete, &replyTaskComplete)

	for ok == false {
		// fmt.Println("Server没有收到Get操作完成的通知")
		ok = ck.server.Call("KVServer.TaskComplete_Handeler", &argsTaskComplete, &replyTaskComplete)
	}

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	id := nrand()
	args := PutAppendArgs{
		Key:        key,
		Value:      value,
		Identifier: id,
	}
	reply := PutAppendReply{}

	ret := ""
	ok := ck.server.Call("KVServer."+op, &args, &reply)

	for ok == false {
		// fmt.Printf("%v操作失败\n", op)
		ok = ck.server.Call("KVServer."+op, &args, &reply)

	}
	ret = reply.Value

	argsTaskComplete := TaskCompleteArgs{
		Identifier: id,
	}
	replyTaskComplete := TaskCompleteReply{}
	ok = ck.server.Call("KVServer.TaskComplete_Handeler", &argsTaskComplete, &replyTaskComplete)

	for ok == false {
		// fmt.Println("Server没有收到PutAppend操作完成的通知")
		ok = ck.server.Call("KVServer.TaskComplete_Handeler", &argsTaskComplete, &replyTaskComplete)
	}

	return ret
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
