package kvsrv

import (
	"log"
	"sync"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvStorage map[string]string //存储键值对映射

}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvStorage[args.Key]
	// DPrintf("Get: Key=%s, Value=%s\n", args.Key, reply.Value)
	// if reply.Value == "" {
	// 	fmt.Println("Value是空字符串")
	// }
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) { //Put方法不需要返回值
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("Get: Key=%s, Value=%s\n", args.Key, args.Value)
	kv.kvStorage[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("Get: Key=%s, Value=%s\n", args.Key, args.Value)
	oldValue := kv.kvStorage[args.Key]
	kv.kvStorage[args.Key] = oldValue + args.Value
	// kv.kvStorage[args.Key] = args.Value + oldValue	//这里追加方式颠倒了，应该oldValue在前面
	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	return kv
}
