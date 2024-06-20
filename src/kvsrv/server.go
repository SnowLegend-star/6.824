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
	kvStorage  map[string]string //存储键值对映射
	opReply    map[int64]string  //维护每个请求应该收到的回复
	opComplete map[int64]int     //维护每个请求是否完成
}

func (kv *KVServer) TaskComplete_Handeler(args *TaskCompleteArgs, reply *TaskCompleteReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.opComplete, args.Identifier) //处理完这个请求后直接删除记录
	delete(kv.opReply, args.Identifier)    //及时删除记录
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//只有未完成的操作才可以被执行
	reply.Value = kv.kvStorage[args.Key]
	// if kv.opComplete[args.Identifier] == 0 {
	// 	kv.opComplete[args.Identifier] = 1
	// 	reply.Value = kv.kvStorage[args.Key]
	// } else {
	// 	reply.Value = kv.kvStorage[args.Key]
	// }

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
	if kv.opComplete[args.Identifier] == 0 {
		kv.opComplete[args.Identifier] = 1
		kv.kvStorage[args.Key] = args.Value
	}

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("Get: Key=%s, Value=%s\n", args.Key, args.Value)

	if kv.opComplete[args.Identifier] == 0 {
		kv.opComplete[args.Identifier] = 1
		oldValue := kv.kvStorage[args.Key]
		kv.kvStorage[args.Key] = oldValue + args.Value
		// kv.kvStorage[args.Key] = args.Value + oldValue	//这里追加方式颠倒了，应该oldValue在前面
		reply.Value = oldValue
		kv.opReply[args.Identifier] = oldValue
	} else {
		reply.Value = kv.opReply[args.Identifier]
	}

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	kv.opReply = make(map[int64]string)
	kv.opComplete = make(map[int64]int)
	return kv
}
