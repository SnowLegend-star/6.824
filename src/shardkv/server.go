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

	ShardId int       //这个op对应哪个shard
	Data    KVStorage //存储shardData

	NewConfig shardctrler.Config
	ValidConf bool //这次的shardData是否发送成功
}

type KVStorage map[string]string //本地存储

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

	shardStorage     []KVStorage                 //当前group内，每个shard的存储内容
	AppliedIndexMax  int                         //当前kvserver已经应用的来自raft的msg Index最大值
	reslutCh         map[int]chan ResultFromRaft //用来接收来自raft内容的channel数组 用index和channel进行匹配
	opCompleteState  map[int64]int               //记录对于Client，已经应用的最大commandIndex
	curConfig        shardctrler.Config          //当前最新的配置
	mck              *shardctrler.Clerk
	statePerShard    []bool //记录每个shard的迁移状态
	specialNum       int    //打印statePerShard的参数
	whetherBacktrace bool   //当前group应的有效config是否应该回溯
	validConfigNum   int    //上一次合法的config
}

type ResultFromRaft struct {
	value        string
	Err          Err
	shardDataOld string //shardData应用command之前的状态，确保可以进行状态回退   其实可以和value合并的，但是可读性不好
}

// 深拷贝shardData内容
func deepCopyMap(original KVStorage) KVStorage {
	copy := make(KVStorage)
	for key, value := range original {
		copy[key] = value
	}
	return copy
}

// 深拷贝config内容
func deepCopyConfig(newConfig shardctrler.Config) shardctrler.Config {
	tmpConfig := shardctrler.Config{
		Num:    newConfig.Num,
		Shards: newConfig.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range tmpConfig.Groups {
		tmpConfig.Groups[gid] = servers
	}
	return tmpConfig
}

// 判断接收方group B是否成功完成了所有shardData的接收
// 亦可判断接收方group A是否成功完成了所有shardData的发送
// 当第一个group加入集群的时候，group 0不会传递shardData
func (kv *ShardKV) isHandleShardDataComplete() bool {
	if kv.curConfig.Num == 1 {
		return true
	}
	for _, value := range kv.statePerShard {
		if !value {
			return false
		}
	}
	// kv.DebugLeader(DMoveShard, "成功更新config%v的shardData", kv.curConfig.Num)
	return true
}

// command是否需要进行阻塞
func (kv *ShardKV) whetherBlock() {
	for kv.isHandleShardDataComplete() == false {
		time.Sleep(15 * time.Millisecond)
		kv.DebugLeader(dConfig, "kv.statePerShard为%v", kv.statePerShard)
	}
}

// 判断请求operation是否重复
func (kv *ShardKV) isRepetitive(clientId int64, commandIndex int) bool {
	if tmp, exists := kv.opCompleteState[clientId]; exists {
		return commandIndex <= tmp
	}
	return false
}

// 判断是否访问了错误的group
// 不需要加锁
func (kv *ShardKV) whetherWrongGroup(key string) bool {
	shardId := key2shard(key)
	//最新的配置中，将这个shard分给了另一个group
	if kv.curConfig.Shards[shardId] != kv.gid {
		kv.DebugLeader(dConfig, "旧配置中, 当前shard对应gid=%v;新配置中, shard对应gid=%v", kv.gid, kv.curConfig.Shards[shardId])
		return true
	}
	return false
}

// 等待channel的结果
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

// group A -> group B shard的kvstorage
// 需要加锁吗
func (kv *ShardKV) moveShardData(data KVStorage, shardId int, newConfig shardctrler.Config, newGid int) Err {
	//如果更新了配置,那就是op中的这个shardID需要被移动
	args := MoveShardDataArgs{
		KvData:    data,
		ShardId:   shardId,
		ConfigNum: newConfig.Num,
	}

	gidRecv := newGid
	for i := 0; i < 10; i++ {
		if servers, ok := newConfig.Groups[gidRecv]; ok {
			for si := 0; si < len(servers); si++ {
				var reply MoveShardDataReply
				srv := kv.make_end(servers[si])
				ok := srv.Call("ShardKV.HandleShardData", &args, &reply)
				if reply.Err != ErrWrongLeader {
					kv.DebugLeader(DMoveShard, "Config%v 尝试进行group %v -> group %v with shard %v:(%v)", newConfig.Num, kv.gid, gidRecv, shardId, data)
				}
				if ok && reply.Err == OK {
					kv.DebugLeader(DMoveShard, "Config%v group %v -> group %v SUCCESS! with shard %v:(%v) ", newConfig.Num, kv.gid, gidRecv, shardId, data)
					kv.statePerShard[shardId] = true //更新状态
					kv.whetherBacktrace = false
					return OK
				}
				if ok && reply.Err == ErrOldConfigSend {
					kv.whetherBacktrace = true
					return reply.Err
				}
				// kv.DebugLeader(DMoveShard, "这次的处理结果是%v", reply.Err)
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	kv.DebugLeader(DMoveShard, "group %v -> group %v with shard %v failed!", kv.gid, gidRecv, shardId)
	return "Error"
}

// group B 处理shard的kvstorage
func (kv *ShardKV) HandleShardData(args *MoveShardDataArgs, reply *MoveShardDataReply) {
	kv.mu.Lock()
	// kv.DebugLeader(DBug, "I'm onto you, bug! Almost gotcha!")

	if kv.curConfig.Num > args.ConfigNum {
		//接收方的config更新
		kv.DebugLeader(DMoveShard, "kv.curConfig.Num=%v, args.ConfigNum=%v", kv.curConfig.Num, args.ConfigNum)
		kv.whetherBacktrace = true
		reply.Err = ErrOldConfigSend
		kv.mu.Unlock()
		return
	} else if kv.curConfig.Num < args.ConfigNum {
		//发送方的config更新
		kv.DebugLeader(DMoveShard, "kv.curConfig.Num=%v, args.ConfigNum=%v。跳过此阶段进行下一次配置查询! ", kv.curConfig.Num, args.ConfigNum)
		kv.whetherBacktrace = true
		for i := 0; i < NShards; i++ {
			kv.statePerShard[i] = true
		}
		reply.Err = ErrOldConfigRecv
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//算是一个特殊的operation
	operation := Op{
		OpType:  "MoveShard",
		Data:    args.KvData,
		ShardId: args.ShardId,
	}

	index, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	result := kv.waitForResult(index)
	reply.Err = result.Err
	if reply.Err == OK {
		kv.statePerShard[args.ShardId] = true
		kv.whetherBacktrace = false
		kv.validConfigNum = kv.curConfig.Num
	}

	//及时释放资源
	go func() {
		kv.mu.Lock()
		delete(kv.reslutCh, index)
		kv.mu.Unlock()
	}()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opKey := args.Key
	clientId := args.ClientId
	commandIndex := args.CommandIndex
	shardId := key2shard(args.Key)
	operation := Op{OpType: "Get",
		OpKey:        opKey,
		ClientId:     clientId,
		CommandIndex: commandIndex,
		ShardId:      shardId,
	}
	kv.DebugLeader(dSKServer, "kvserver %v收到了op: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, operation, clientId, commandIndex)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if kv.whetherWrongGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}
	kv.whetherBlock()

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

	opkey := args.Key
	opValue := args.Value
	clientId := args.ClientId
	commandIndex := args.CommandIndex
	shardId := key2shard(args.Key)
	operation := Op{OpType: args.OperationType,
		OpKey:        opkey,
		OpValue:      opValue,
		ClientId:     clientId,
		CommandIndex: commandIndex,
		ShardId:      shardId,
	}
	kv.DebugLeader(dSKServer, "kvserver %v收到了op: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, operation, clientId, commandIndex)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if kv.whetherWrongGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return
	}

	kv.whetherBlock()

	index, _, isLeader := kv.rf.Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.DebugLeader(dSKServer, "kvserver %v收到了op: %v (ClientId, CommandIndex)=(%v,%v)", kv.me, operation, clientId, commandIndex)
	kv.mu.Lock()
	if kv.isRepetitive(clientId, commandIndex) {

		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	result := kv.waitForResult(index)

	if kv.whetherWrongGroup(args.Key) {
		reply.Err = ErrWrongGroup
		//对应用了command的shardData进行回退
		kv.shardStorage[operation.ShardId][operation.OpKey] = result.shardDataOld
		return
	}
	reply.Err = result.Err
	kv.DebugLeader(dSKServer, "kvserver %v处理op: %v (ClientId, CommandIndex)=(%v,%v)的结果是%v", kv.me, operation, clientId, commandIndex, result.Err)
	go func() {
		kv.mu.Lock()
		delete(kv.reslutCh, index)
		kv.mu.Unlock()
	}()
}

// 定期向shardCtrler请求最新配置
// 只有Leader才可以进行
func (kv *ShardKV) monitorConfigChanges() {

	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		newConfig := kv.mck.Query(-1)
		oldConfig := kv.curConfig
		//更新配置
		if newConfig.Num > kv.curConfig.Num {
			kv.DebugLeader(dConfig, "Server %v的newConfig.Num=%v, kv.curConfig.Num=%v", kv.me, newConfig.Num, kv.curConfig.Num)

			kv.mu.Lock()

			//例如config 5回退到config4
			if kv.whetherBacktrace {
				oldConfig = kv.mck.Query(kv.validConfigNum)
				kv.DebugLeader(dConfig, "Server %v的有效config是: %v", kv.me, oldConfig)
			}
			for shardId, oldGid := range oldConfig.Shards {
				newGid := newConfig.Shards[shardId]

				// 分片ShardId从当前组迁移到其他组
				if oldGid == kv.gid && newGid != kv.gid {
					kv.statePerShard[shardId] = false
					shardData := deepCopyMap(kv.shardStorage[shardId])
					err := kv.moveShardData(shardData, shardId, newConfig, newGid)
					//发送方更新自己的config
					if err == ErrOldConfigSend {
						for i := 0; i < NShards; i++ {
							kv.statePerShard[i] = true
						}
						break
					}
					// kv.shardStorage[shardId] = make(KVStorage) //在applier中重置  感觉可以删掉
				}
				//当前组需要接收分片shardId
				//如果初始集群中只有group0, 此时加入的group特殊考虑
				if oldGid != kv.gid && newGid == kv.gid && newConfig.Num != 1 {
					kv.statePerShard[shardId] = false
				}
			}

			//发送方全部发送完shardData才更新validConfig
			//循环结束两种情况：1、出现backtrace
			//2、全部发送成功
			if kv.isHandleShardDataComplete() && !kv.whetherBacktrace {
				kv.validConfigNum = newConfig.Num
				for shardId, oldGid := range kv.curConfig.Shards {
					newGid := newConfig.Shards[shardId]
					if oldGid == kv.gid && newGid != kv.gid {
						kv.shardStorage[shardId] = make(KVStorage)
					}
				}
			}

			kv.mu.Unlock()
			kv.curConfig = newConfig //能不能删掉？？？

			//在raft中同步config
			configTmp := shardctrler.Config{
				Num:    newConfig.Num,
				Shards: newConfig.Shards,
				Groups: make(map[int][]string),
			}
			for gid, servers := range newConfig.Groups {
				configTmp.Groups[gid] = servers
			}
			operation := Op{
				OpType:    "MoveConfig",
				NewConfig: configTmp,
				ValidConf: kv.whetherBacktrace,
			}

			index, _, _ := kv.rf.Start(operation)
			result := kv.waitForResult(index)
			if result.Err != OK {
				kv.DebugLeader(dConfig, "Server %v同步config失败: ", kv.me, result.Err)
			}

			kv.DebugLeader(dConfig, "server %v的新配置为%v", kv.me, kv.curConfig)
			kv.DebugLeader(dConfig, "对应的kvstorage为%v", kv.shardStorage)

		}

		//阻塞直至shardData更新完成
		kv.whetherBlock()

		if kv.specialNum != kv.curConfig.Num {
			kv.DebugLeader(dConfig, "Server %v成功更新configNum=%v阶段的shardData!", kv.me, kv.curConfig.Num)
			kv.specialNum = kv.curConfig.Num
		}

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
				// kv.DebugLeader(dRaft, "Gotcha, you pesky bug :)")
				kv.mu.Lock()

				//如果command的序列号小于当前kvserver已经应用的最大序列号
				if msg.CommandIndex <= kv.AppliedIndexMax {
					kv.mu.Unlock()
					continue
				}
				kv.AppliedIndexMax = msg.CommandIndex
				kv.DebugLeader(dRaft, "收到从raft提交的msg: %v", msg)

				var res ResultFromRaft
				if op.OpType == "Get" {
					//如果这个命令已经过时了，直接丢弃 只是针对于Get()
					if term, isLeader := kv.rf.GetState(); !isLeader || term != msg.CommandTerm {
						kv.mu.Unlock()
						continue
					}

					res.value = kv.shardStorage[op.ShardId][op.OpKey]
					if res.value == "" {
						res.Err = ErrNoKey
					} else {
						res.Err = OK
					}
				} else if op.OpType == "MoveShard" {
					kv.shardStorage[op.ShardId] = op.Data
					res.Err = OK
				} else if op.OpType == "MoveConfig" {
					//分片ShardId从当前组迁移到其他组
					//Follower的处理方式是直接把相应的ShardData置为空
					//如果当前config需要回溯，不用处理kvstorage
					for shardId, oldGid := range kv.curConfig.Shards {
						newGid := op.NewConfig.Shards[shardId]
						if oldGid == kv.gid && newGid != kv.gid && op.ValidConf == false {
							kv.shardStorage[shardId] = make(KVStorage)
						}
					}
					//对于普通的config只需要这一步
					kv.curConfig = deepCopyConfig(op.NewConfig)
					kv.DebugLeader(dConfig, "Server %v成功更新config", kv.me)
					res.Err = OK
					kv.makeSnapshot(msg.CommandIndex)
				} else {
					//如果这个op已经被应用过了，忽略
					// kv.DebugLeader(dSKServer,"这条msg: %v被记录过了吗%v?", msg, kv.opCompleteState[op.OpIdentifier])
					if kv.isRepetitive(op.ClientId, op.CommandIndex) {
						kv.DebugLeader(dSKServer, "已经记录这条op了: (ClientId, CommandIndex)=(%v,%v)", op.ClientId, op.CommandIndex)
						kv.mu.Unlock()
						continue
					}

					if op.OpType == "Put" {
						res.shardDataOld = kv.shardStorage[op.ShardId][op.OpKey] //存储旧的shardData

						kv.shardStorage[op.ShardId][op.OpKey] = op.OpValue
						res.Err = OK
					}
					if op.OpType == "Append" {
						res.shardDataOld = kv.shardStorage[op.ShardId][op.OpKey] //存储旧的shardData

						kv.shardStorage[op.ShardId][op.OpKey] += op.OpValue
						res.Err = OK
					}
					kv.opCompleteState[op.ClientId] = op.CommandIndex
				}

				if op.OpType != "MoveConfig" {
					kv.DebugLeader(dKVStorage, "应用msg之后的kvstorage的内容如下: %v", kv.shardStorage)
				}

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
	if kv.maxraftstate == -1 {
		return
	}
	//记录kvstorage和opCompleteState
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.opCompleteState)
	e.Encode(kv.curConfig)
	e.Encode(kv.shardStorage)
	e.Encode(kv.statePerShard)

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
	var curConfig shardctrler.Config
	var shardStroage []KVStorage
	var statePerShard []bool

	if d.Decode(&opCompleteState) != nil ||
		d.Decode(&curConfig) != nil ||
		d.Decode(&shardStroage) != nil ||
		d.Decode(&statePerShard) != nil {
		kv.DebugLeader(dSnap, "读取持久化状态失败")
	} else {
		kv.opCompleteState = opCompleteState
		kv.curConfig = curConfig
		kv.shardStorage = shardStroage
		kv.statePerShard = statePerShard
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
	kv.shardStorage = make([]KVStorage, NShards)
	for i := 0; i < NShards; i++ {
		kv.shardStorage[i] = make(KVStorage)
	}
	kv.reslutCh = make(map[int]chan ResultFromRaft)
	kv.opCompleteState = make(map[int64]int)
	kv.AppliedIndexMax = 0
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.statePerShard = make([]bool, NShards)
	for i := 0; i < NShards; i++ {
		kv.statePerShard[i] = true
	}
	kv.whetherBacktrace = false
	kv.validConfigNum = 1
	//读取快照
	kv.readSnapshot(persister.ReadSnapshot())
	kv.specialNum = kv.curConfig.Num
	go kv.applier()
	go kv.monitorConfigChanges()
	// go kv.isRecvShardDataComplete()
	return kv
}
