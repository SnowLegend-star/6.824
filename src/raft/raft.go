package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Entry interface{} //日志的每个条目
	// heartBeat int    //心跳信息
	Term int //这条entry对应的term
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//3A
	currentTerm       int //当前服务器能看到的最新的Term
	voteFor           int //给哪个candidate投票了	-1表示还没进行投票
	log               []LogEntry
	serverType        string    //每个服务器的身份
	lastMsgFromLeader time.Time //当前Server最后一次收到leader消息的时间
	voteCount         int       //candidate的投票计数  记得重置投票！
	lastLogIndex      int
	lastLogTerm       int
	// voteState         map[int]bool //如果当前服务器是candidate，记录其他follower的投票情况

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg

	//3D
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshotData      []byte
	logLength         int //用这个来反映Log的最大index
}

// 重新设置index
// snapshot也相当于一条entry
func (rf *Raft) ResetIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func nrand() int64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(1 << 62)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { //西巴，这里是State啊？！我给看成Start了

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.serverType == "Leader" {
		isleader = true
	}

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).

	//什么时候进行进程persist呢？
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.voteFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)

	//snapshot
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshotData)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshotData []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	//这里Decoder变量的顺序要和encode保持一致
	if d.Decode(&voteFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		Debug(dError, "读取持久化状态失败")
	} else {
		rf.voteFor = voteFor
		rf.currentTerm = currentTerm
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.logLength = len(rf.log) + rf.lastIncludedIndex //重新设置LogLength
	}

	//Read snapshotData
	//是否要判断snapshotData的合法性？
	rf.snapshotData = snapshotData
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

	//这里的Index是基于最初始的那个Log还是  基于动态变化的Log呢？

	//1、在log修改之前获得Term
	//2、修改log
	//3、修改lastIncludedIndex
	//在snapshot中，其实不应该修改LogLength吧。

	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dInfo, "Leader %v在index=%v处调用了snapshot", rf.me, index)
	newIndex := rf.ResetIndex(index)
	rf.lastIncludedTerm = rf.log[newIndex].Term
	rf.log = rf.log[newIndex:]
	rf.log[0].Entry = nil
	//开始制作snapshot
	rf.lastIncludedIndex = index
	rf.logLength = len(rf.log) + rf.lastIncludedIndex //Log包括了snapshot这一位的长度

	rf.snapshotData = snapshot
	rf.persist()
	Debug(dLog2, "它的新Log为: %v", rf.log)
}

type SnapshotArgs struct {
	LeaderId          int
	LeaderTerm        int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapshotData      []byte
}

type SnapshotReply struct {
	FollowerTerm int
}

// Leader发送snapshot
func (rf *Raft) sendSnapshot(serverId int, args SnapshotArgs) {

	reply := SnapshotReply{}
	ok := rf.peers[serverId].Call("Raft.InstallSnapshot", &args, &reply)

	if !ok {
		Debug(dError, "Leader %v向Server %v发送snapshot失败", rf.me, serverId)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//任期过期
	if args.LeaderTerm != rf.currentTerm {
		return
	}

	if reply.FollowerTerm > rf.currentTerm {
		Debug(dInfo, "Leader %v, Term=%v退位", rf.me, rf.currentTerm)
		rf.currentTerm = reply.FollowerTerm
		rf.serverType = "Follower"
		rf.voteCount = 0
		rf.voteFor = -1
		//这个时候没有涉及snapshot
		rf.persist() //持久化记录
	}

	//发送完snapshot后修改nextIndex
	rf.nextIndex[serverId] = rf.lastIncludedIndex + 1
}

// Follower处理snapshot
func (rf *Raft) InstallSnapshot(args *SnapshotArgs, reply *SnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.FollowerTerm = rf.currentTerm

	if rf.currentTerm > args.LeaderTerm {
		return
	}

	if rf.currentTerm < args.LeaderTerm {
		rf.currentTerm = args.LeaderId
		rf.persist()
		rf.serverType = "Follower"
	}

	rf.lastMsgFromLeader = time.Now() //更新收到消息的时间

	//如果Server包含这个snapshot就直接返回
	if rf.lastIncludedIndex == args.LastIncludedIndex &&
		// (rf.logLength-1) >= args.LastIncludedIndex &&
		rf.log[rf.ResetIndex(rf.lastIncludedIndex)].Term == args.LastIncludedTerm {
		return
	}

	//snapshot不匹配
	//Log的长度不足，涉及LogLength的长度变化
	if (rf.logLength - 1) < args.LastIncludedIndex {
		rf.log = make([]LogEntry, 0)
		entryTmp := LogEntry{Term: args.LastIncludedTerm} //把snapshot的最后状态添加进新的Log
		rf.log = append(rf.log, entryTmp)
		rf.logLength = args.LastIncludedIndex + 1
	} else {
		//Log长度足够
		rf.log = rf.log[rf.ResetIndex(args.LastIncludedIndex):]
	}

	//尝试强制将index=0的command置为nil
	rf.log[0].Entry = nil

	//处理完log的长度问题开始修改相关参数
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshotData = args.SnapshotData
	rf.persist()
	//更新完snapshot后继续更新
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)

	Debug(dInfo, "Server %v成功收到Leader %v发送的lastIncludedIndex=%v的snapshot", rf.me, args.LeaderId, args.LastIncludedIndex)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshotData,
		SnapshotIndex: rf.lastIncludedIndex,
		SnapshotTerm:  rf.lastIncludedTerm,
	}

	rf.applyCh <- msg
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateTerm int //Candidate的term
	CandidateId   int //peer数组的下标就是每个服务器的编号
	LastLogIndex  int
	LastLogTerm   int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	FollowerTerm int  //follower的term，用于candidate更新它的term
	VoteGranted  bool //true说明candidate收到这个投票了
}

type AppendEntryArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry //为空则表示心跳信息
	LeaderCommit int
	EntryTmp     LogEntry
}

type AppendEntryReply struct {
	FollowerTerm int
	Success      bool
	ApplyFail    bool //日志的对齐检查是否失败

	ConflictIndex int //快速回退相关
	ConflictTerm  int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Initialize reply
	reply.VoteGranted = false
	reply.FollowerTerm = rf.currentTerm

	Debug(dInfo, "Server %v,Term=%v的voteFor=%v", rf.me, rf.currentTerm, rf.voteFor)

	// If the candidate's term is less than current term, reject the vote
	if args.CandidateTerm < rf.currentTerm {
		return
	}

	// If the candidate's term is greater than current term, update term and convert to follower
	if args.CandidateTerm > rf.currentTerm {
		rf.currentTerm = args.CandidateTerm
		rf.voteFor = -1
		rf.persist()
		rf.serverType = "Follower"
	}

	// Check if this server has already voted for someone else in the same term
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		return
	}

	// Check candidate's log term and index to decide whether to grant vote
	// logLength := len(rf.log)
	logLength := rf.logLength
	if rf.ResetIndex(logLength-1) < 0 {
		Debug(dError, "Server %v的logLength=%v, lastIncludedIndex=%v", rf.me, rf.logLength, rf.lastIncludedIndex)
	}
	lastLogTerm := rf.log[rf.ResetIndex(logLength-1)].Term
	lastLogIndex := logLength - 1
	// Debug(dInfo, "在RequestVote()中, Server %v的logLength=%v", rf.me, rf.logLength)

	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
		// Grant vote
		rf.voteFor = args.CandidateId
		rf.persist()
		rf.lastMsgFromLeader = time.Now()
		reply.FollowerTerm = rf.currentTerm
		reply.VoteGranted = true
		// Debug(dVote, "Server %v, Term=%v给%v进行投票了", rf.me, rf.currentTerm, args.CandidateId)
		// Debug(dInfo, "Server %v的Log为: %v,LastLogTerm=%v, LastLogIndex=%v", rf.me, rf.log, lastLogTerm, lastLogIndex)
		// Debug(dInfo, "Candidate %v的LastLogTerm=%v, LastLogIndex=%v", args.CandidateId, args.LastLogTerm, args.LastLogIndex)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) sendRequestVote() {
	//重置自己的状态
	rf.mu.Lock()
	rf.serverType = "Candidate"
	rf.currentTerm++
	rf.persist() //持久化记录
	currentTermConst := rf.currentTerm
	rf.lastMsgFromLeader = time.Now() //重置选举超时计时器

	rf.voteCount = 1 //先给自己投票
	rf.voteFor = rf.me
	// logLength := len(rf.log) //这个length其实包含了index=0的那个占位符
	logLength := rf.logLength

	if rf.ResetIndex(logLength-1) < 0 {
		Debug(dError, "Candidate %v的下标有问题, logLength=%v,lastIncludedIndex=%v", rf.me, rf.logLength, rf.lastIncludedIndex)
		Debug(dError, "此时的Log为: %v", rf.log)
	}

	args := &RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  logLength - 1,
		LastLogTerm:   rf.log[rf.ResetIndex(logLength-1)].Term,
	}
	rf.mu.Unlock()

	Debug(dCandidate, "Candidate %v准备发起选举,Term=%v\n", rf.me, currentTermConst)
	// 向其他节点发送投票请求
	for serverId := range rf.peers {
		if serverId == rf.me { //如果是自己就跳过此次循环
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			ok := rf.peers[i].Call("Raft.RequestVote", args, reply)
			if !ok {
				// Debug(dVote, "Candidate %v, Term= %v向Server %v请求投票failed", rf.me, currentTermConst, i)
				return
			}

			rf.mu.Lock()
			// defer rf.mu.Unlock()
			//任期过期就直接退出
			if rf.currentTerm != args.CandidateTerm {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.VoteGranted {
				rf.mu.Lock()
				Debug(dVote, "Candidate %v, Term= %v向Server %v, Term=%v请求投票成功", rf.me, currentTermConst, i, reply.FollowerTerm)
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					rf.serverType = "Leader"
					Debug(dLeader, "%v成为了Leader,term=%v", rf.me, currentTermConst)
					rf.lastMsgFromLeader = time.Now()
					if len(rf.log) != 1 {
						Debug(dLog2, "Leader=%v, Term= %v的日志为: %v", rf.me, currentTermConst, rf.log)
					}
					//当选后重置nextIndex和matchIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.logLength
						rf.matchIndex[i] = 0
					}
				}
				// Debug(dVote, "Candidate %v, Term= %v持有%v票", rf.me, rf.currentTerm, rf.voteCount)
				rf.mu.Unlock()
			} else { //Follower不同意投票
				rf.mu.Lock()
				if reply.FollowerTerm > rf.currentTerm {
					rf.currentTerm = reply.FollowerTerm
					rf.persist() //持久化记录
					rf.serverType = "Follower"
				}
				rf.mu.Unlock()
			}
		}(serverId)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock() //又tm是这里导致的死锁
	// Your code here (3B).
	if rf.serverType != "Leader" {
		return index, term, false
	}

	//如果当前Server是Leader，进行日志的复制工作

	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Entry: command})
	rf.logLength++
	rf.persist()
	index = rf.logLength - 1 //下标从1开始

	// for i := 0; i < len(rf.peers); i++ {
	// 	rf.nextIndex[i] = index + 1 //有待商榷
	// 	rf.matchIndex[i] = 0
	// }
	if len(rf.log) != 1 {
		Debug(dAppendEntry, "Leader=%v, Term= %v的日志为: %v", rf.me, rf.currentTerm, rf.log)
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {

	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) HeartBeat(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Debug(dHeartbeat, "%v %v 收到来自Leader=%v,Term=%v的Heartbeat", rf.serverType, rf.me, args.LeaderId, args.LeaderTerm)

	//收到leader的消息后，Server重置自己的选举状态

	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	reply.FollowerTerm = rf.currentTerm
	reply.Success = false

	//Leader过期
	if args.LeaderTerm < rf.currentTerm {
		Debug(dCommit, "Apply entry failed in heartbeat! Leader=%v Term=%v ->Server=%v Term=%v", args.LeaderId, args.LeaderTerm, rf.me, rf.currentTerm)
		return
	}

	if args.PrevLogIndex < 0 {
		Debug(dCommit, "Apply entry failed! Invalid PrevLogIndex: %v", args.PrevLogIndex)
		return
	}

	rf.lastMsgFromLeader = time.Now() //更新收到消息的时间 收到旧的Leader发来的消息不会更新自己的lastMsgFromLeader

	//Follower的最大日志Index小于leader发来的prevLogIndex
	// Debug(dInfo, "在HeartBeat()中, Server %v的logLength=%v", rf.me, rf.logLength)
	if rf.logLength-1 < args.PrevLogIndex {
		Debug(dError, "Leader=%v发送的PrevLogIndex=%v 大于 Follower=%v的最大日志索引%d", args.LeaderId, args.PrevLogIndex, rf.me, rf.logLength-1)
		Debug(dLog, "Follower=%v, Term= %v的日志为: %v", rf.me, rf.currentTerm, rf.log)
		reply.ApplyFail = true
		// reply.ConflictIndex = len(rf.log) //这里要不要减1呢？
		if rf.lastApplied != 0 {
			// reply.ConflictIndex = rf.commitIndex //试试投机取巧
			reply.ConflictIndex = rf.lastApplied
		} else {
			reply.ConflictIndex = 1
		}
		return
	}

	//一个条件不满足就是不匹配

	if !(rf.log[rf.ResetIndex(args.PrevLogIndex)].Term == args.PrevLogTerm && rf.log[rf.ResetIndex(args.PrevLogIndex)].Entry == args.EntryTmp.Entry) {
		//snapshot的command部分是nil, 要排除在外   但是snapshot是必定匹配的，就像以前index=0的entry是一样的
		if !(rf.log[rf.ResetIndex(args.PrevLogIndex)].Entry == nil && rf.log[rf.ResetIndex(args.PrevLogIndex)].Term != -1) {
			Debug(dError, "Leader的args.PrevLogIndex=%v ", args.PrevLogIndex)
			Debug(dError, "Leader=%v, args.PrevLogTerm=%v 不等于Server=%v log[args.PrevLogIndex].term=%v", args.LeaderId, args.PrevLogTerm, rf.me, rf.log[rf.ResetIndex(args.PrevLogIndex)].Term)

			reply.ApplyFail = true //日志的对齐检查是否失败

			//开始执行快速回退
			Debug(dInfo, "Server %v的lastIncludedIndex=%v, rf.ResetIndex(args.PrevLogIndex)=%v, LogLength=%v", rf.me, rf.lastIncludedIndex, rf.ResetIndex(args.PrevLogIndex), rf.logLength)
			Debug(dInfo, "Server %v的Log为: %v", rf.me, rf.log)
			reply.ConflictTerm = rf.log[rf.ResetIndex(args.PrevLogIndex)].Term
			// for i := args.PrevLogIndex; i >= 0; i-- {
			// 	if rf.log[rf.ResetIndex(i)].Term != reply.ConflictTerm {
			// 		reply.ConflictIndex = i + 1
			// 		break
			// 	}
			// }
			if rf.lastApplied != 0 {
				// reply.ConflictIndex = rf.commitIndex //试试投机取巧
				reply.ConflictIndex = rf.lastApplied
			} else {
				reply.ConflictIndex = 1
			}
			return
		}
	}

	//日志匹配上了
	rf.log = rf.log[:rf.ResetIndex(args.PrevLogIndex+1)]
	rf.logLength = len(rf.log) + rf.lastIncludedIndex
	Debug(dInfo, "Server %v, LogLength=%v,len(rf.log)=%v, lastIncludedIndex=%v", rf.me, rf.logLength, len(rf.log), rf.lastIncludedIndex)

	rf.persist() //持久化记录
	if len(args.Entries) > 0 {
		rf.log = append(rf.log, args.Entries...)
		rf.logLength = len(rf.log) + rf.lastIncludedIndex
		rf.persist() //持久化记录
	}
	reply.Success = true
	reply.FollowerTerm = rf.currentTerm

	//更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		tmp := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, rf.logLength-1)
		rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex) // 保证LastApplied不小于LastIncludedIndex
		Debug(dCommit, "Server %v的commitIndex从%v->%v", rf.me, tmp, rf.commitIndex)
	}

	if len(rf.log) != 1 {
		Debug(dLog, "Follower=%v, Term= %v的日志为: %v", rf.me, rf.currentTerm, rf.log)
	}

	rf.currentTerm = args.LeaderTerm
	rf.serverType = "Follower"
	rf.voteCount = 0
	rf.persist() //持久化记录

}

// 发送心跳信息
func (rf *Raft) sendHeartBeat() {

	//先把在运行时可能发生的变量存起来
	rf.mu.Lock()
	currentTermConst := rf.currentTerm
	commitIndexConst := rf.commitIndex
	PrevLogIndexConst := make([]int, len(rf.peers))
	PrevLogTermConst := make([]int, len(rf.peers))
	EntryTmpConst := make([]LogEntry, len(rf.peers))
	entriesConst := make([][]LogEntry, len(rf.peers))
	legalState := make([]int, len(rf.peers))
	for i := range rf.peers {
		PrevLogIndexConst[i] = rf.nextIndex[i] - 1
		// Debug(dInfo, "ResetIndex(rf.nextIndex[i]-1)=%v,rf.nextIndex[i]-1=%v,rf.lastIncludedIdx=%v", rf.ResetIndex(rf.nextIndex[i]-1), rf.nextIndex[i]-1, rf.lastIncludedIndex)
		//数组下标合法性检查
		if rf.ResetIndex(rf.nextIndex[i]-1) < 0 {
			legalState[i] = -1
			continue
		}
		PrevLogTermConst[i] = rf.log[rf.ResetIndex(rf.nextIndex[i]-1)].Term
		EntryTmpConst[i] = rf.log[rf.ResetIndex(rf.nextIndex[i]-1)]
		//if last log index >= nextIndex for a follower, 发送的Heartbeat附加日志
		if rf.logLength-1 >= rf.nextIndex[i] {
			if i != rf.me {
				Debug(dInfo, "Follower=%v 需要进行日志对齐rf.nextIndex[i]=%v,prevlogindex=%v", i, rf.nextIndex[i], PrevLogIndexConst[i])
			}
			// entriesConst[i] = rf.log[rf.ResetIndex(rf.nextIndex[i]):]  //浅拷贝有风险啊
			lengthTmp := len(rf.log[rf.ResetIndex(rf.nextIndex[i]):])
			entriesConst[i] = make([]LogEntry, lengthTmp)
			copy(entriesConst[i], rf.log[rf.ResetIndex(rf.nextIndex[i]):])

		} else {
			entriesConst[i] = []LogEntry{} //不需要附加日志就设置为空
		}
	}
	rf.lastMsgFromLeader = time.Now()
	rf.mu.Unlock()

	Debug(dInfo, "Leader %v的PrevLogIndexConst为: %v", rf.me, PrevLogIndexConst)

	for serverId := range rf.peers {
		if serverId != rf.me && rf.serverType == "Leader" {
			go func(i int) {
				//添加错误检查
				if rf.nextIndex[i] <= 0 || rf.nextIndex[i] > rf.logLength {
					Debug(dError, "sendHeartBeat: rf.nextIndex[%d] out of range, rf.nextIndex[%d] = %d, Log的最大长度 = %d", i, i, rf.nextIndex[i], rf.logLength)
					return
				}

				//判断是否需要发送snapshot
				if legalState[i] == -1 {
					argsSnapshot := SnapshotArgs{
						LeaderId:          rf.me,
						LeaderTerm:        currentTermConst,
						LastIncludedIndex: rf.lastIncludedIndex,
						LastIncludedTerm:  rf.log[rf.ResetIndex(rf.lastIncludedIndex)].Term,
						SnapshotData:      rf.snapshotData,
					}

					go rf.sendSnapshot(i, argsSnapshot)
					return
				}

				argsAppendEntry := &AppendEntryArgs{
					LeaderTerm:   currentTermConst,
					LeaderId:     rf.me,
					PrevLogIndex: PrevLogIndexConst[i],
					PrevLogTerm:  PrevLogTermConst[i],
					Entries:      entriesConst[i],
					LeaderCommit: commitIndexConst,
					// EntryTmp:     rf.log[rf.ResetIndex(PrevLogIndexConst[i])],
					EntryTmp: EntryTmpConst[i],
				}

				replyAppendEntry := &AppendEntryReply{}

				// Debug(dHeartbeat, "Leader=%v, Term=%v -> Server=%v", rf.me, rf.currentTerm, i)
				ok := rf.peers[i].Call("Raft.HeartBeat", argsAppendEntry, replyAppendEntry)
				if !ok {
					// Debug(dError, "Heartbeat: Leader=%v, Term=%v -> Server=%v failed!", rf.me, rf.currentTerm, i)
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// rpc的回复过期,直接放弃
				if rf.currentTerm != argsAppendEntry.LeaderTerm {
					return
				}

				//日志一致性检查通过
				if replyAppendEntry.Success {
					rf.matchIndex[i] = argsAppendEntry.PrevLogIndex + len(argsAppendEntry.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1

					//如果此次Heartbeat附加了entry，那就尝试更新leader的commitIndex
					if len(argsAppendEntry.Entries) > 0 {
						Debug(dInfo, "Leader %v的matchIndex为: %v", rf.me, rf.matchIndex)
						N := rf.commitIndex
						// for index := rf.commitIndex + 1; index < rf.logLength; index++ {
						// for index := rf.lastIncludedIndex + 1; index < rf.logLength; index++ {
						for index := rf.logLength - 1; index > rf.commitIndex; index-- {
							count := 1 // 包括 Leader 自己
							for i := range rf.peers {
								if rf.matchIndex[i] >= index && i != rf.me {
									count++
								}
							}
							if rf.ResetIndex(index) < 0 {
								Debug(dInfo, "commitIndex=%v, lastApplied=%v", commitIndexConst, rf.lastApplied)
								Debug(dError, "在sendHeartBeat()中, Leader %v的logLength=%v, lastIncludedIndex=%v, index=%v", rf.me, rf.logLength, rf.lastIncludedIndex, index)
							}

							if count > len(rf.peers)/2 && rf.log[rf.ResetIndex(index)].Term == rf.currentTerm {
								N = index
								break //由于是从后往前遍历，找到最大的元素就直接进退出了
							}
						}
						if N != rf.commitIndex {
							rf.commitIndex = N
						}
						rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
						Debug(dInfo, "Leader %v在index=%v的entry可以提交了", rf.me, N)
					}
					return //通过了一致性检查直接返回这个goroutine
				}

				//任期检查匹配失败
				if replyAppendEntry.FollowerTerm > rf.currentTerm {
					//旧王已陨，新帝当立
					Debug(dLeader, "Leader: %v,term=%v退位了", rf.me, rf.currentTerm)
					rf.currentTerm = replyAppendEntry.FollowerTerm
					rf.serverType = "Follower"
					rf.voteCount = 0
					rf.voteFor = -1
					rf.persist() //持久化记录
					return
				}

				//快速回退nextIndex
				if replyAppendEntry.ApplyFail {
					// if replyAppendEntry.ConflictTerm != -1 {
					// 	lastIndexInTerm := -1
					// 	for j := len(rf.log) - 1; j >= 0; j-- {
					// 		if rf.log[j].Term == replyAppendEntry.ConflictTerm {
					// 			lastIndexInTerm = j
					// 			break
					// 		}
					// 	}
					// 	if lastIndexInTerm != -1 {
					// 		rf.nextIndex[i] = lastIndexInTerm + 1
					// 	} else {
					// 		rf.nextIndex[i] = replyAppendEntry.ConflictIndex
					// 	}
					// } else {
					// 	rf.nextIndex[i] = replyAppendEntry.ConflictIndex
					// }
					rf.nextIndex[i] = replyAppendEntry.ConflictIndex //投机取巧
					Debug(dInfo, "rf.nextIndex[%d]快速回退到%v", i, rf.nextIndex[i])
				}

			}(serverId)
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 500 + (rand.Int63() % 400)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		checkTime := time.Now()
		duration := checkTime.Sub(rf.lastMsgFromLeader)
		//500~900ms没收到leader的消息就发起选举
		if duration > time.Duration(ms)*time.Millisecond && rf.serverType != "Leader" {
			rf.mu.Unlock()
			Debug(dInfo, "距离上一次收到Leader的消息过去了:%v ms\n", duration.Milliseconds())
			Debug(dInfo, "Follower %v准备发起选举\n", rf.me)
			rf.sendRequestVote()
		} else {
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.serverType == "Leader" {
			rf.mu.Unlock()
			// leader发送消息
			rf.sendHeartBeat()
		} else {
			rf.mu.Unlock()
		}
		//每100ms发送一轮
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// 应用日志条目到状态机，并更新 lastApplied
func (rf *Raft) applier() {
	msg := ApplyMsg{CommandValid: false}
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.logLength-1 { //少用for循环来避免goroutine长时间持有锁
			rf.lastApplied++
			if rf.ResetIndex(rf.lastApplied) < 0 {
				Debug(dError, "在applier()中, Server %v的lastApplied=%v, lastIncludedIndex=%v", rf.me, rf.lastApplied, rf.lastIncludedIndex)
			}
			msg = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.ResetIndex(rf.lastApplied)].Entry,
				CommandIndex: rf.lastApplied,
			}
		}
		rf.mu.Unlock()
		if msg.CommandValid {
			rf.applyCh <- msg
			msg.CommandValid = false
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.voteCount = 0
	rf.voteFor = -1
	rf.serverType = "Follower"
	rf.lastMsgFromLeader = time.Time{}
	rf.lastLogIndex = -1
	rf.lastLogTerm = -1
	// rf.voteState = make(map[int]bool)
	//log的0号元素拿来占位
	rf.log = make([]LogEntry, 1)
	rf.log[0].Entry = -1
	rf.log[0].Term = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1
	rf.snapshotData = make([]byte, 0)
	rf.logLength = 1

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.logLength //有待商榷
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeat()

	go rf.applier()

	return rf
}
