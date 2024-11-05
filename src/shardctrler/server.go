package shardctrler

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num	相当于kvstorage

	AppliedIndexMax int                         //当前kvserver已经应用的来自raft的msg Index最大值
	reslutCh        map[int]chan ResultFromRaft //用来接收来自raft内容的channel数组 用index和channel进行匹配
	opCompleteState map[int64]int               //记录对于Client，已经应用的最大commandIndex
}

type Parameter struct {
	Servers map[int][]string //join(servers)
	Gids    []int            //leave(gids)
	Shard   int              //move(shard,gid)
	Gid     int
	Num     int //query(num)
}

type ResultFromRaft struct {
	Err         Err
	ConfigValue Config
}

type Op struct {
	// Your data here.
	OpType       string //操作的类型
	OpParemeter  Parameter
	ClientId     int64
	CommandIndex int //这个op的序列号
}

// 判断请求operation是否重复
func (sc *ShardCtrler) isRepetitive(clientId int64, commandIndex int) bool {
	if tmp, exists := sc.opCompleteState[clientId]; exists {
		return commandIndex <= tmp
	}
	return false
}

func (sc *ShardCtrler) waitForResult(indexOld int) ResultFromRaft {
	sc.mu.Lock()

	//如果没有这个Ch就创建一个
	if _, ok := sc.reslutCh[indexOld]; !ok {
		sc.reslutCh[indexOld] = make(chan ResultFromRaft)
	}
	waitCh := sc.reslutCh[indexOld]
	sc.mu.Unlock()

	select {
	case result := <-waitCh:
		return result
	case <-time.After(time.Millisecond * 1000):
		sc.DebugLeader(dSCServer, "index=%v的chan等待超时", indexOld)
		return ResultFromRaft{Err: ErrTimeout}
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (sc *ShardCtrler) rebalanceShards(newConfig *Config) {
	groupNum := len(newConfig.Groups)      //这个集群里面有多少组 0 2 3直接算2组
	hasAssignedExtra := make(map[int]bool) //记录每个gid是否已经被分配了extraShard
	//组数重新归0的特殊情况
	if groupNum == 0 {
		return
	}

	//新的配置中，每个Server可以有拥有的shards的数量是：[average, average+1)
	averageShards := NShards / groupNum
	extraShards := NShards % groupNum
	shardPerGroup := make(map[int]int)

	//统计原来的Shards对应的gid的情况
	for _, gid := range newConfig.Shards {
		if gid != 0 {
			shardPerGroup[gid]++
		}
	}

	//获得升序的gid数组
	groupList := make([]int, 0, groupNum)
	for gid := range newConfig.Groups {
		groupList = append(groupList, gid)
	}
	sort.Ints(groupList)

	//遍历Shards设置可以被分配的shard   这段代码十分精妙
	availableShards := []int{}
	for shardId, gid := range newConfig.Shards {
		if !hasAssignedExtra[gid] {
			if gid == 0 || shardPerGroup[gid] > averageShards+boolToInt(extraShards > 0) {
				availableShards = append(availableShards, shardId)
				shardPerGroup[gid]--
				newConfig.Shards[shardId] = 0
			} else if shardPerGroup[gid] == averageShards+boolToInt(extraShards > 0) {
				extraShards--
				hasAssignedExtra[gid] = true
			}
		}

	}

	//将未分配的shard进行分配  gid0不参与分配
	for _, gid := range groupList {
		if gid == 0 {
			continue
		}
		// 计算该 group 应分配的 shard 数量
		expectedShards := averageShards
		if extraShards > 0 {
			expectedShards++
			extraShards--
		}

		for shardPerGroup[gid] < expectedShards && len(availableShards) > 0 {
			shard := availableShards[len(availableShards)-1]
			availableShards = availableShards[:len(availableShards)-1]
			newConfig.Shards[shard] = gid
			shardPerGroup[gid]++
		}
	}

	// // 确保没有shard分配给gid=0
	// if len(availableShards) > 0 && groupNum > 0 {
	// 	for _, shard := range availableShards {
	// 		newConfig.Shards[shard] = groupList[0]
	// 	}
	// }
}

func (sc *ShardCtrler) JoinHandler(args Parameter) ResultFromRaft {

	sc.mu.Lock()
	defer sc.mu.Unlock()

	oldConfigNum := len(sc.configs) - 1
	oldConfig := sc.configs[oldConfigNum]
	newConfig := Config{
		Num:    oldConfigNum + 1,
		Groups: make(map[int][]string), //group需要深拷贝
		Shards: oldConfig.Shards,
	}

	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	//添加新的gid->servers映射
	for gid, servers := range args.Servers {
		newConfig.Groups[gid] = servers
	}

	//对Shard进行重新分片
	sc.rebalanceShards(&newConfig)

	sc.configs = append(sc.configs, newConfig)
	res := ResultFromRaft{Err: OK}

	return res
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	//组的数量不可能大于分片的质量

	sc.mu.Lock()
	oldConfigNum := len(sc.configs) - 1
	oldConfig := sc.configs[oldConfigNum]
	sc.mu.Unlock()
	//传入的gid不可以重复
	for gid1 := range oldConfig.Groups {
		for gid2 := range args.Servers {
			if gid1 == gid2 {
				reply.Err = ErrGid
				return
			}
		}
	}

	operation := Op{
		OpType:       "Join",
		OpParemeter:  Parameter{Servers: args.Servers},
		ClientId:     args.ClientId,
		CommandIndex: args.CommandIndex,
	}

	index, _, isLeader := sc.Raft().Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sc.DebugLeader(dSCServer, "Server %v成功收到Join: %v", sc.me, args.Servers)
	//重复的op直接返回ok
	sc.mu.Lock()
	clientId := args.ClientId
	commandIndex := args.CommandIndex
	if sc.isRepetitive(clientId, commandIndex) {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	result := sc.waitForResult(index)

	//更新回复
	reply.WrongLeader = false
	if result.Err == OK {
		sc.DebugLeader(dSCServer, "Server %v成功处理Join: %v", sc.me, args.Servers)
		sc.DebugLeader(dConfig, "新的config为: %v", sc.configs[len(sc.configs)-1])
	}
	reply.Err = result.Err
	go func() {
		sc.mu.Lock()
		delete(sc.reslutCh, index)
		sc.mu.Unlock()
	}()

}

func (sc *ShardCtrler) LeaveHandler(args Parameter) ResultFromRaft {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	oldConfigNum := len(sc.configs) - 1
	oldConfig := sc.configs[oldConfigNum]
	newConfig := Config{
		Num:    oldConfigNum + 1,
		Groups: make(map[int][]string), // 深拷贝 group
		Shards: oldConfig.Shards,
	}

	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	// 移除指定的gid
	// 将对应的Shards[gid]置为0
	for _, gidToLeave := range args.Gids {
		delete(newConfig.Groups, gidToLeave)
		for shardId, gid := range newConfig.Shards {
			if gidToLeave == gid {
				newConfig.Shards[shardId] = 0
			}
		}
	}

	// 重新分片
	sc.rebalanceShards(&newConfig)

	sc.configs = append(sc.configs, newConfig)
	res := ResultFromRaft{Err: OK}
	return res
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	operation := Op{
		OpType:       "Leave",
		OpParemeter:  Parameter{Gids: args.GIDs},
		ClientId:     args.ClientId,
		CommandIndex: args.CommandIndex,
	}

	index, _, isLeader := sc.Raft().Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sc.DebugLeader(dSCServer, "Server %v成功收到Leave: %v", sc.me, args.GIDs)
	sc.mu.Lock()
	clientId := args.ClientId
	commandIndex := args.CommandIndex
	if sc.isRepetitive(clientId, commandIndex) {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	result := sc.waitForResult(index)

	//更新回复
	reply.WrongLeader = false
	if result.Err == OK {
		sc.DebugLeader(dSCServer, "Server %v成功处理Leave: %v", sc.me, args.GIDs)
		sc.DebugLeader(dConfig, "新的config为: %v", sc.configs[len(sc.configs)-1])
	}
	reply.Err = result.Err

	go func() {
		sc.mu.Lock()
		delete(sc.reslutCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) MoveHandler(args Parameter) ResultFromRaft {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	oldConfigNum := len(sc.configs) - 1
	oldConfig := sc.configs[oldConfigNum]
	newConfig := Config{
		Num:    oldConfigNum + 1,
		Groups: make(map[int][]string), // 深拷贝 group
		Shards: oldConfig.Shards,
	}

	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = servers
	}

	newConfig.Shards[args.Shard] = args.Gid
	sc.configs = append(sc.configs, newConfig)
	res := ResultFromRaft{Err: OK}
	return res
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	operation := Op{
		OpType:       "Move",
		OpParemeter:  Parameter{Shard: args.Shard, Gid: args.GID},
		ClientId:     args.ClientId,
		CommandIndex: args.CommandIndex,
	}

	index, _, isLeader := sc.Raft().Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sc.DebugLeader(dSCServer, "Server %v成功收到Move: Shard=%v, Gid=%v", sc.me, args.Shard, args.GID)
	sc.mu.Lock()
	clientId := args.ClientId
	commandIndex := args.CommandIndex
	if sc.isRepetitive(clientId, commandIndex) {
		reply.Err = OK
		reply.WrongLeader = false
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	result := sc.waitForResult(index)

	//更新回复
	reply.WrongLeader = false
	reply.Err = result.Err
	if result.Err == OK {
		sc.DebugLeader(dSCServer, "Server %v成功处理Move: Shard=%v, Gid=%v", sc.me, args.Shard, args.GID)
		sc.DebugLeader(dConfig, "新的config为: %v", sc.configs[len(sc.configs)-1])
	}

	go func() {
		sc.mu.Lock()
		delete(sc.reslutCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) QueryHandler(args Parameter) ResultFromRaft {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	latestConfigNum := len(sc.configs) - 1
	latestConfig := sc.configs[latestConfigNum]
	var value ResultFromRaft
	if args.Num == -1 {
		value.ConfigValue = latestConfig
	} else {
		value.ConfigValue = sc.configs[args.Num]
	}
	value.Err = OK
	return value
}
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	operation := Op{
		OpType:       "Query",
		OpParemeter:  Parameter{Num: args.Num},
		ClientId:     args.ClientId,
		CommandIndex: args.CommandIndex,
	}

	index, _, isLeader := sc.Raft().Start(operation)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	sc.DebugLeader(dSCServer, "Server %v成功收到Query: %v", sc.me, args.Num)
	sc.mu.Lock()
	clientId := args.ClientId
	commandIndex := args.CommandIndex
	if sc.isRepetitive(clientId, commandIndex) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	result := sc.waitForResult(index)
	reply.WrongLeader = false
	reply.Config = result.ConfigValue
	reply.Err = result.Err
	if result.Err == OK {
		sc.DebugLeader(dSCServer, "Server %v成功处理Query: %v", sc.me, args.Num)
	}

	go func() {
		sc.mu.Lock()
		delete(sc.reslutCh, index)
		sc.mu.Unlock()
	}()

}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				op, ok := msg.Command.(Op)
				if !ok {
					fmt.Printf("command转化为Op失败")
				}

				sc.mu.Lock()
				//如果command的序列号小于当前kvserver已经应用的最大序列号
				//commandIndex从1开始
				if msg.CommandIndex <= sc.AppliedIndexMax {
					sc.mu.Unlock()
					continue
				}
				sc.DebugLeader(dRaft, "Server %v收到来自raft的msg: %v", sc.me, msg.Command)
				sc.AppliedIndexMax = msg.CommandIndex

				if sc.isRepetitive(op.ClientId, op.CommandIndex) {
					// sc.DebugLeader("kvserver %v已经记录这条op了: (ClientId, CommandIndex)=(%v,%v)", kv.me, op.ClientId, op.CommandIndex)
					sc.mu.Unlock()
					continue
				}
				sc.mu.Unlock()
				var res ResultFromRaft

				if op.OpType == "Join" {
					res = sc.JoinHandler(op.OpParemeter)
				} else if op.OpType == "Leave" {
					res = sc.LeaveHandler(op.OpParemeter)
				} else if op.OpType == "Move" {
					res = sc.MoveHandler(op.OpParemeter)
				} else if op.OpType == "Query" {
					// sc.DebugLeader("applier()准备处理query")
					res = sc.QueryHandler(op.OpParemeter)
				}
				sc.mu.Lock()
				sc.opCompleteState[op.ClientId] = op.CommandIndex
				if _, isLeader := sc.rf.GetState(); isLeader {
					if ch, ok := sc.reslutCh[msg.CommandIndex]; ok {
						ch <- res
					}
				}
				sc.mu.Unlock()

			}
		}

	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.reslutCh = make(map[int]chan ResultFromRaft)
	sc.opCompleteState = make(map[int64]int)
	sc.AppliedIndexMax = 0
	go sc.applier()
	return sc
}
