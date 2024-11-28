package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrTimeout       = "ErrTimeout"
	ErrOldConfigSend = "ErrOldConfigSend"
	ErrOldConfigRecv = "ErrOldConfigRecv"
)

type Err string

// The number of shards.
const NShards = 10

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key           string
	Value         string
	OperationType string //"Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId     int64
	CommandIndex int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId     int64
	CommandIndex int
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardDataArgs struct {
	KvData    KVStorage
	ShardId   int
	ConfigNum int
}

type MoveShardDataReply struct {
	Err Err
}
