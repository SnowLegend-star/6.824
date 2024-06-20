package kvsrv

import (
	"crypto/rand"
	"fmt"
	"math/big"

	// "time"

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
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}

	ok := ck.server.Call("KVServer.Get", &args, &reply)

	if ok {
		// fmt.Println("Get操作成功")
	} else {
		fmt.Println("Get操作失败")
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

	args := PutAppendArgs{
		Key:   key,
		Value: value,
	}
	reply := PutAppendReply{}

	var ok bool
	var ret string
	if op == "Put" { //我是傻逼，这里写个PUT。写牛魔啊啊啊啊
		ok = ck.server.Call("KVServer.Put", &args, &reply)
		ret = ""
	} else {
		ok = ck.server.Call("KVServer.Append", &args, &reply)
		ret = reply.Value
	}

	if ok {
		// fmt.Println("PutAppend操作成功")
	} else {
		fmt.Println("PutAppend操作失败")
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
