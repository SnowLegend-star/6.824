package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType       string //操作的类型
	OpKey        string
	OpValue      string
	ClientId     int64
	CommandIndex int //这个op的序列号
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// kvStorage       map[string]string           //本地存储
	kvMachine       kvDatabase
	AppliedIndexMax int                         //当前kvserver已经应用的来自raft的msg Index最大值
	reslutCh        map[int]chan ResultFromRaft //用来接收来自raft内容的channel数组 用index和channel进行匹配
	opCompleteState map[int64]int               //记录对于Client，已经应用的最大commandIndex
}

type ResultFromRaft struct {
	value string
	Err   Err
}

// 判断请求operation是否重复
func (kv *KVServer) isRepetitive(clientId int64, commandIndex int) bool {
	if tmp, exists := kv.opCompleteState[clientId]; exists {
		return commandIndex <= tmp
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	opKey := args.Key
	clientId := args.ClientId
	commandIndex := args.CommandIndex
	operation := Op{OpType: "Get",
		OpKey:        opKey,
		ClientId:     clientId,
		CommandIndex: commandIndex,
	}

	index, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("kvserver %v收到了op: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, operation, clientId, commandIndex)
	// //这个请求已经完成就直接返回
	// kv.mu.Lock()
	// if kv.isRepetitive(clientId, commandIndex) {
	// 	reply.Value = kv.kvMachine.KVStorage[args.Key]
	// 	reply.Err = OK
	// 	kv.mu.Unlock()
	// 	return
	// }
	// kv.mu.Unlock()
	result := kv.waitForResult(index)
	DPrintf("kvserver %v处理op: %v (ClientId, CommandIndex)=(%v,%v)的结果是%v", kv.me, operation, clientId, commandIndex, result.Err)
	reply.Value = result.value
	reply.Err = result.Err

	//及时释放资源
	go func() {
		kv.mu.Lock()
		delete(kv.reslutCh, index)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	opkey := args.Key
	opValue := args.Value
	clientId := args.ClientId
	commandIndex := args.CommandIndex
	operation := Op{OpType: args.OperationType,
		OpKey:        opkey,
		OpValue:      opValue,
		ClientId:     clientId,
		CommandIndex: commandIndex,
	}

	index, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("kvserver %v收到了op: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, operation, clientId, commandIndex)
	kv.mu.Lock()
	if kv.isRepetitive(clientId, commandIndex) {
		DPrintf("kvserver %v已经记录这条op了: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, args.OperationType, clientId, commandIndex)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("kvserver %v等待处理这条op: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, args.OperationType, clientId, commandIndex)
	result := kv.waitForResult(index)
	DPrintf("kvserver %v处理op: %v (ClientId, CommandIndex)=(%v,%v)的结果是%v", kv.me, operation, clientId, commandIndex, result.Err)
	reply.Err = result.Err

	go func() {
		kv.mu.Lock()
		delete(kv.reslutCh, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.PutAppend(args, reply)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.PutAppend(args, reply)
}

func (kv *KVServer) waitForResult(indexOld int) ResultFromRaft {
	kv.mu.Lock()

	//如果没有这个Ch就创建一个
	if _, ok := kv.reslutCh[indexOld]; !ok {
		kv.reslutCh[indexOld] = make(chan ResultFromRaft)
	}
	waitCh := kv.reslutCh[indexOld]
	kv.mu.Unlock()

	select {
	case result := <-waitCh:
		return result
	case <-time.After(time.Millisecond * 1000):
		return ResultFromRaft{Err: ErrTimeout}
	}
}

func (kv *KVServer) applier() {

	for !kv.killed() {
		// var msg raft.ApplyMsg
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op, ok := msg.Command.(Op) //将command重新转化为Op格式的内容
				if !ok {
					DPrintf("command转化为Op失败")
				}
				kv.mu.Lock()

				DPrintf("这条msg: %v的序列号为: %v ,当前kvserver %v的最大序列号为: %v", msg, op.CommandIndex, kv.me, kv.AppliedIndexMax)
				//如果command的序列号小于当前kvserver已经应用的最大序列号
				//commandIndex从1开始
				if msg.CommandIndex <= kv.AppliedIndexMax {
					kv.mu.Unlock()
					continue
				}
				kv.AppliedIndexMax = msg.CommandIndex
				DPrintf("kvserver %v收到从raft提交的msg: %v", kv.me, msg)
				var res ResultFromRaft
				if op.OpType == "Get" {
					//如果这个命令已经过时了，直接丢弃 只是针对于Get()
					if term, isLeader := kv.rf.GetState(); !isLeader || term != msg.CommandTerm {
						kv.mu.Unlock() //真的是被lock烦死了
						continue
					}
					res.value = kv.kvMachine.KVStorage[op.OpKey]
					if res.value == "" {
						res.Err = ErrNoKey
					} else {
						res.Err = OK
					}
				} else {
					//如果这个op已经被应用过了，忽略
					// DPrintf("这条msg: %v被记录过了吗%v?", msg, kv.opCompleteState[op.OpIdentifier])
					if kv.isRepetitive(op.ClientId, op.CommandIndex) {
						DPrintf("kvserver %v已经记录这条op了: (ClientId, CommandIndex)=(%v,%v)", kv.me, op.ClientId, op.CommandIndex)
						kv.mu.Unlock()
						continue
					}

					if op.OpType == "Put" {
						kv.kvMachine.KVStorage[op.OpKey] = op.OpValue
						res.Err = OK
					}
					if op.OpType == "Append" {
						if oldValue, exists := kv.kvMachine.KVStorage[op.OpKey]; exists {
							kv.kvMachine.KVStorage[op.OpKey] = oldValue + op.OpValue
						} else {
							kv.kvMachine.KVStorage[op.OpKey] = op.OpValue
						}
						res.Err = OK
					}

					kv.opCompleteState[op.ClientId] = op.CommandIndex
				}

				// DPrintf("kvserver %v记录op: %v id=%v", kv.me, op, op.OpIdentifier)
				DPrintf("kvserver %v应用msg之后的kvstorage的内容如下: %v", kv.me, kv.kvMachine.KVStorage)

				//处理完结果准备放入resultCh 其实就是针对Put/Append。能到这里的Get()都是经过考验的
				if _, isLeader := kv.rf.GetState(); isLeader {
					if ch, ok := kv.reslutCh[msg.CommandIndex]; ok {
						ch <- res
					}
				}

				if kv.whetherNeedSnapshot() {
					DPrintf("开始制作snapshot, 传入raft的index为: %v", msg.CommandIndex)
					kv.makeSnapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				//这条msg是快照
				kv.mu.Lock()
				kv.readSnapshot(msg.Snapshot)
				kv.AppliedIndexMax = msg.CommandIndex
				kv.mu.Unlock()
			}
		}

	}

}

func (kv *KVServer) whetherNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.maxraftstate < kv.rf.GetRaftStateSize()
}

// 制作snapshot
func (kv *KVServer) makeSnapshot(opIndexToSnapshot int) {
	//记录kvstorage和opCompleteState
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.opCompleteState)
	e.Encode(kv.kvMachine)

	kvraftState := w.Bytes()
	kv.rf.Snapshot(opIndexToSnapshot, kvraftState)
}

// 不需要分别读取data和snapshot，结合到一起读更方便
func (kv *KVServer) readSnapshot(stateData []byte) {
	if stateData == nil || len(stateData) < 1 {
		return
	}

	r := bytes.NewBuffer(stateData)
	d := labgob.NewDecoder(r)
	var opCompleteState map[int64]int
	var kvMachine kvDatabase

	if d.Decode(&opCompleteState) != nil ||
		d.Decode(&kvMachine) != nil {
		DPrintf("读取持久化状态失败")
	} else {
		kv.opCompleteState = opCompleteState
		kv.kvMachine = kvMachine
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvMachine = *newKVMachine()
	kv.reslutCh = make(map[int]chan ResultFromRaft)
	kv.AppliedIndexMax = 0
	kv.opCompleteState = make(map[int64]int)
	// You may need initialization code here.

	//读取快照
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.applier()
	return kv
}
