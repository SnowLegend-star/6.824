package shardkv

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// Your definitions here.
	KVStorage       map[string]string           //本地存储
	AppliedIndexMax int                         //当前kvserver已经应用的来自raft的msg Index最大值
	reslutCh        map[int]chan ResultFromRaft //用来接收来自raft内容的channel数组 用index和channel进行匹配
	opCompleteState map[int64]int               //记录对于Client，已经应用的最大commandIndex
	// curConfigNum    int                         //最新配置的序号
	curConfig shardctrler.Config //当前最新的配置
	mck       *shardctrler.Clerk
}

type ResultFromRaft struct {
	value string
	Err   Err
}

// 判断请求operation是否重复
func (kv *ShardKV) isRepetitive(clientId int64, commandIndex int) bool {
	if tmp, exists := kv.opCompleteState[clientId]; exists {
		return commandIndex <= tmp
	}
	return false
}

func (kv *ShardKV) whetherWrongGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shardId := key2shard(key)

	//最新的配置中，将这个shard分给了另一个group
	if kv.curConfig.Shards[shardId] != kv.gid {
		kv.DebugLeader(dConfig, "旧配置中, 当前shard对应gid=%v;新配置中, shard对应gid=%v", kv.gid, kv.curConfig.Shards[shardId])
		return true
	}
	return false
}

func (kv *ShardKV) waitForResult(indexOld int) ResultFromRaft {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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

	result := kv.waitForResult(index)
	kv.DebugLeader(dSKServer, "kvserver %v处理op: %v (ClientId, CommandIndex)=(%v,%v)的结果是%v", kv.me, operation, clientId, commandIndex, result.Err)
	if kv.whetherWrongGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Value = result.value
	reply.Err = result.Err

	//及时释放资源
	go func() {
		kv.mu.Lock()
		delete(kv.reslutCh, index)
		kv.mu.Unlock()
	}()

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// if kv.whetherWrongGroup(args.Key) {
	// 	reply.Err = ErrWrongGroup
	// 	return
	// }

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

	kv.DebugLeader(dSKServer, "kvserver %v收到了op: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, operation, clientId, commandIndex)
	kv.mu.Lock()
	if kv.isRepetitive(clientId, commandIndex) {
		kv.DebugLeader(dSKServer, "kvserver %v已经记录这条op了: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, args.OperationType, clientId, commandIndex)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.DebugLeader(dSKServer, "kvserver %v等待处理这条op: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, args.OperationType, clientId, commandIndex)
	result := kv.waitForResult(index)
	kv.DebugLeader(dSKServer, "kvserver %v处理op: %v (ClientId, CommandIndex)=(%v,%v)的结果是%v", kv.me, operation, clientId, commandIndex, result.Err)
	if kv.whetherWrongGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err = result.Err

	go func() {
		kv.mu.Lock()
		delete(kv.reslutCh, index)
		kv.mu.Unlock()
	}()
}

// 定期向shardCtrler请求最新配置
func (kv *ShardKV) monitorConfigChanges() {
	for !kv.killed() {
		newConfig := kv.mck.Query(-1)
		kv.mu.Lock()
		//更新配置
		if newConfig.Num > kv.curConfig.Num {
			kv.curConfig = newConfig
			kv.DebugLeader(dConfig, "Serverd的新配置为%v", kv.curConfig)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

// 定期从raft提取msg
func (kv *ShardKV) applier() {

	for !kv.killed() {
		// var msg raft.ApplyMsg
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				op, ok := msg.Command.(Op) //将command重新转化为Op格式的内容
				if !ok {
					kv.DebugLeader(dSKServer, "command转化为Op失败")
				}
				kv.mu.Lock()

				kv.DebugLeader(dSKServer, "这条msg: %v的序列号为: %v ,当前kvserver %v的最大序列号为: %v", msg, op.CommandIndex, kv.me, kv.AppliedIndexMax)
				//如果command的序列号小于当前kvserver已经应用的最大序列号
				//commandIndex从1开始
				if msg.CommandIndex <= kv.AppliedIndexMax {
					kv.mu.Unlock()
					continue
				}
				kv.AppliedIndexMax = msg.CommandIndex
				kv.DebugLeader(dRaft, "kvserver %v收到从raft提交的msg: %v", kv.me, msg)
				var res ResultFromRaft
				if op.OpType == "Get" {
					//如果这个命令已经过时了，直接丢弃 只是针对于Get()
					if term, isLeader := kv.rf.GetState(); !isLeader || term != msg.CommandTerm {
						kv.mu.Unlock()
						continue
					}
					res.value = kv.KVStorage[op.OpKey]
					if res.value == "" {
						res.Err = ErrNoKey
					} else {
						res.Err = OK
					}
				} else {
					//如果这个op已经被应用过了，忽略
					// kv.DebugLeader(dSKServer,"这条msg: %v被记录过了吗%v?", msg, kv.opCompleteState[op.OpIdentifier])
					if kv.isRepetitive(op.ClientId, op.CommandIndex) {
						kv.DebugLeader(dSKServer, "kvserver %v已经记录这条op了: (ClientId, CommandIndex)=(%v,%v)", kv.me, op.ClientId, op.CommandIndex)
						kv.mu.Unlock()
						continue
					}

					if op.OpType == "Put" {
						kv.KVStorage[op.OpKey] = op.OpValue
						res.Err = OK
					}
					if op.OpType == "Append" {
						if oldValue, exists := kv.KVStorage[op.OpKey]; exists {
							kv.KVStorage[op.OpKey] = oldValue + op.OpValue
						} else {
							kv.KVStorage[op.OpKey] = op.OpValue
						}
						res.Err = OK
					}

					kv.opCompleteState[op.ClientId] = op.CommandIndex
				}

				// kv.DebugLeader(dSKServer,"kvserver %v记录op: %v id=%v", kv.me, op, op.OpIdentifier)
				kv.DebugLeader(dSKServer, "kvserver %v应用msg之后的kvstorage的内容如下: %v", kv.me, kv.KVStorage)

				//处理完结果准备放入resultCh 其实就是针对Put/Append。能到这里的Get()都是经过考验的
				if _, isLeader := kv.rf.GetState(); isLeader {
					if ch, ok := kv.reslutCh[msg.CommandIndex]; ok {
						ch <- res
					}
				}

				if kv.whetherNeedSnapshot() {
					kv.DebugLeader(dSKServer, "开始制作snapshot, 传入raft的index为: %v", msg.CommandIndex)
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

func (kv *ShardKV) whetherNeedSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.maxraftstate < kv.rf.GetRaftStateSize()
}

// 制作snapshot
func (kv *ShardKV) makeSnapshot(opIndexToSnapshot int) {
	//记录kvstorage和opCompleteState
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.opCompleteState)
	e.Encode(kv.KVStorage)

	kvraftState := w.Bytes()
	kv.rf.Snapshot(opIndexToSnapshot, kvraftState)
}

// 不需要分别读取data和snapshot，结合到一起读更方便
func (kv *ShardKV) readSnapshot(stateData []byte) {
	if stateData == nil || len(stateData) < 1 {
		return
	}

	r := bytes.NewBuffer(stateData)
	d := labgob.NewDecoder(r)
	var opCompleteState map[int64]int
	var kvStorage map[string]string

	if d.Decode(&opCompleteState) != nil ||
		d.Decode(&kvStorage) != nil {
		kv.DebugLeader(dSnap, "读取持久化状态失败")
	} else {
		kv.opCompleteState = opCompleteState
		kv.KVStorage = kvStorage
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.KVStorage = make(map[string]string)
	kv.reslutCh = make(map[int]chan ResultFromRaft)
	kv.opCompleteState = make(map[int64]int)
	kv.AppliedIndexMax = 0
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//读取快照
	kv.readSnapshot(persister.ReadSnapshot())
	go kv.applier()
	go kv.monitorConfigChanges()
	return kv
}
