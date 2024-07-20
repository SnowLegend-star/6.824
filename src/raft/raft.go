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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	command     interface{}

	// applyEntry []bool //维护Follower是否成功复制了Entry
	applyEntrySuccess int //维护有几个Follower成功复制Entry
	applyCh           chan ApplyMsg
	hasApply          bool //维护当前command是否已经提交

}

type AppendEntry struct {
	// heartBeat int
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.voteFor)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("Follower: %v开始处理来自Candidate: %v的投票请求\n", rf.me, args.CandidateId)

	reply.VoteGranted = false
	reply.FollowerTerm = rf.currentTerm
	// rf.lastMsgFromLeader=time

	//比较发出投票请求的candidate与自己的term大小
	if args.CandidateTerm < rf.currentTerm {
		return
	}
	//一定要持久化记录投票情况
	if args.CandidateTerm > rf.currentTerm {
		rf.currentTerm = args.CandidateTerm //更新Follower的term
		rf.voteFor = -1                     //重置投票状态
	}

	//这里不等你直接使用rf.lastLogTerm
	logLength := len(rf.log)
	if rf.voteFor == -1 {
		if args.LastLogTerm > rf.log[logLength-1].Term {
			rf.voteFor = args.CandidateId
			reply.FollowerTerm = rf.currentTerm
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.log[logLength-1].Term && args.LastLogIndex >= logLength-1 {
			rf.voteFor = args.CandidateId
			reply.FollowerTerm = rf.currentTerm
			reply.VoteGranted = true
		}
	}

	// if rf.voteFor == args.CandidateId { //说明同一个Candidate因为网络问题发送了好几个投票请求
	// 	reply.FollowerTerm = rf.currentTerm
	// 	reply.VoteGranted = true //只投一次票
	// 	// fmt.Printf("Follower:%v给Candidate:%v投票了\n", rf.me, args.CandidateId)
	// } else {
	// 	reply.FollowerTerm = rf.currentTerm
	// 	reply.VoteGranted = false
	// }

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
		return a
	} else {
		return b
	}
}

// 处于持有rf.mu的状态
func (rf *Raft) sendRequestVote() {
	//重置自己的状态
	rf.voteCount = 0
	rf.serverType = "Candidate"
	rf.currentTerm++

	rf.lastMsgFromLeader = time.Now() //重置选举超时计时器
	//首先给每个follower发送信息
	rf.voteCount++ //先给自己投票
	rf.voteFor = rf.me
	logLength := len(rf.log) //这个length其实包含了index=0的那个占位符

	var muSendRequestVote sync.Mutex

	args := &RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  logLength - 1,
		LastLogTerm:   rf.log[logLength-1].Term,
	}
	Debug(dCandidate, "Candidate %v准备发起选举,Term=%v\n", rf.me, rf.currentTerm)
	// 向其他节点发送投票请求
	for serverId := range rf.peers {
		if serverId == rf.me { //如果是自己就跳过此次循环
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			ok := rf.peers[i].Call("Raft.RequestVote", args, reply)
			if !ok {
				// Debug(dVote, "Candidate %v, Term= %v向Server %v请求投票failed", rf.me, rf.currentTerm, i)
			}
			// else { //不是吧，在打印日志这里加了“Term=%v的投票”就过了吗
			// 	// Debug(dVote, "Candidate %v, Term= %v收到了来自Server %v,Term=%v的投票,结果是%v", rf.me, rf.currentTerm, i, reply.FollowerTerm, reply.VoteGranted)
			// }
			muSendRequestVote.Lock()
			defer muSendRequestVote.Unlock()
			if reply.VoteGranted {
				rf.voteCount++
				if rf.voteCount > len(rf.peers)/2 {
					rf.serverType = "Leader"
					Debug(dLeader, "%v成为了Leader,term=%v", rf.me, rf.currentTerm)
					rf.lastMsgFromLeader = time.Now()
					if len(rf.log) != 1 {
						Debug(dLog2, "Leader=%v, Term= %v的日志为: %v", rf.me, rf.currentTerm, rf.log)
					}
					//当选后重置nextIndex和matchIndex
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.log) //有待商榷
						rf.matchIndex[i] = 0
					}

					//开始发送心跳信息进行集权统治
					rf.sendHeartBeat()
				}
				// Debug(dVote, "Candidate %v, Term= %v持有%v票", rf.me, rf.currentTerm, rf.voteCount)

			} else { //Follower不同意投票
				if reply.FollowerTerm > rf.currentTerm {
					rf.currentTerm = reply.FollowerTerm
				}
			}

		}(serverId)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	// defer rf.mu.Lock() 我是傻逼
	defer rf.mu.Unlock()

	reply.FollowerTerm = rf.currentTerm
	reply.Success = false

	if args.LeaderTerm < rf.currentTerm {
		Debug(dCommit, "Apply entry failed! Server=%v Term=%v > Leader=%v Term=%v", rf.me, rf.currentTerm, args.LeaderId, args.LeaderTerm)
		return
	}

	rf.lastMsgFromLeader = time.Now()
	rf.currentTerm = args.LeaderTerm
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		Debug(dCommit, "Apply entry failed! Server=%v log[args.PrevLogIndex].term=%v", rf.me, rf.log[args.PrevLogIndex])
		rf.log = rf.log[:args.PrevLogIndex] //如果不匹配，删除当前以后续元素
	} else { //匹配上了,把args中的entry一股脑添加上去
		if len(args.Entries) > 0 { //不能把Heartbeat中的空entry也添加进去
			rf.log = append(rf.log, args.Entries...)
		}
		reply.Success = true
		reply.FollowerTerm = rf.currentTerm
	}

	//更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.applyLogs()
	}

}

// 已经持有了rf.mu
func (rf *Raft) sendAppendEntries() {
	var muHeartBeat sync.Mutex

	rf.lastMsgFromLeader = time.Now()
	for serverId := range rf.peers {
		argsAppendEntry := &AppendEntryArgs{
			LeaderTerm:   rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.log) - 1,
			PrevLogTerm:  rf.log[len(rf.log)-1].Term,
			Entries:      []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}

		if serverId == rf.me {
			continue
		}

		go func(i int) {
			replyAppendEntry := &AppendEntryReply{}
			Debug(dInfo, "Leader=%v, Term=%v -> Server=%v  添加日志", rf.me, rf.currentTerm, i)
			ok := rf.peers[i].Call("Raft.AppendEntries", argsAppendEntry, replyAppendEntry)
			// if !ok {
			// 	Debug(dHeartbeat, "Leader=%v, Term=%v -> Server=%v没有收到回应", rf.me, rf.currentTerm, i)
			// }
			muHeartBeat.Lock()
			defer muHeartBeat.Unlock()
			if ok {
				if replyAppendEntry.FollowerTerm > rf.currentTerm { //其实这种情况就是Success=false
					//旧王已陨，新帝当立
					Debug(dLeader, "Leader: %v,term=%v退位了", rf.me, rf.currentTerm)

					rf.currentTerm = replyAppendEntry.FollowerTerm
					rf.serverType = "Follower"
					rf.voteCount = 0
					rf.voteFor = -1
					rf.applyEntrySuccess = 0
					rf.hasApply = false
				} else {
					//处理日志对齐的问题
					for replyAppendEntry.Success == false && ok && rf.serverType == "Leader" {
						// replyAppendEntry.Success=false	//每次都重置Success
						rf.nextIndex[i]--                                  //如果pervlogindex没有匹配上，准备重发
						argsAppendEntry.PrevLogIndex = rf.nextIndex[i] - 1 //PrevLogIndex回退一个
						argsAppendEntry.Entries = append([]LogEntry{rf.log[argsAppendEntry.PrevLogIndex+1]}, argsAppendEntry.Entries...)
						argsAppendEntry.PrevLogTerm = rf.log[argsAppendEntry.PrevLogIndex].Term
						ok := rf.peers[i].Call("Raft.AppendEntries", argsAppendEntry, replyAppendEntry)
						if !ok {
							Debug(dAppendEntry, "Leader=%v, Term=%v -> Server=%v重发失败", rf.me, rf.currentTerm, i)
						}
					}
					if replyAppendEntry.Success == true { //如果日志对齐成功
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = argsAppendEntry.PrevLogIndex + len(argsAppendEntry.Entries)
					}
				}

				if replyAppendEntry.Success == true {
					rf.applyEntrySuccess++
					//准备更新Leader的commitIndex
					if rf.applyEntrySuccess > len(rf.peers)/2 && rf.hasApply == false {

						rf.matchIndex[rf.me] = rf.commitIndex

						N := rf.commitIndex
						for index := rf.commitIndex + 1; index < len(rf.log); index++ {
							count := 1 // 包括 Leader 自己
							for i := range rf.peers {
								if rf.matchIndex[i] >= index {
									count++
								}
							}
							if count > len(rf.peers)/2 && rf.log[index].Term == rf.currentTerm {
								N = index
							}
						}
						if N != rf.commitIndex {
							rf.commitIndex = N
						}
						msg := ApplyMsg{
							CommandValid: true,
							Command:      rf.command,
							CommandIndex: rf.commitIndex,
						}
						rf.applyCh <- msg
						rf.hasApply = true
					}
				}
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
	defer rf.mu.Unlock()
	// Your code here (3B).
	if rf.serverType != "Leader" {
		return index, term, false
	}

	//如果当前Server是Leader，进行日志的复制工作
	rf.command = command
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Entry: command})
	index = len(rf.log) - 1  //下标从1开始
	rf.applyEntrySuccess = 1 //Leader也算一票
	rf.hasApply = false

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = index + 1 //有待商榷
		rf.matchIndex[i] = 0
	}
	if len(rf.log) != 1 {
		Debug(dLog2, "Leader=%v, Term= %v的日志为: %v", rf.me, rf.currentTerm, rf.log)
	}
	// rf.mu.Lock()
	// rf.sendAppendEntries()
	rf.sendHeartBeat()
	// rf.mu.Unlock()

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
	rf.serverType = "Follower"
	rf.voteFor = -1
	rf.voteCount = 0
	rf.applyEntrySuccess = 0
	rf.hasApply = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	reply.FollowerTerm = rf.currentTerm
	reply.Success = false

	if args.LeaderTerm < rf.currentTerm {
		Debug(dCommit, "Apply entry failed in heartbeat! Leader=%v Term=%v ->Server=%v Term=%v", args.LeaderId, args.LeaderTerm, rf.me, rf.currentTerm)
		return
	}
	rf.lastMsgFromLeader = time.Now() //更新收到消息的时间 收到旧的Leader发来的消息不会更新自己的lastMsgFromLeader

	rf.currentTerm = args.LeaderTerm

	if args.PrevLogIndex < 0 {
		Debug(dCommit, "Apply entry failed! Invalid PrevLogIndex: %v", args.PrevLogIndex)
		return
	}

	//Follower的最大日志Index小于leader发来的prevLogIndex
	if len(rf.log)-1 < args.PrevLogIndex {
		Debug(dError, "Leader=%v发送的PrevLogIndex=%v 大于 Follower=%v的最大日志索引%d", args.LeaderId, args.PrevLogIndex, rf.me, len(rf.log)-1)
		Debug(dLog, "Follower=%v, Term= %v的日志为: %v", rf.me, rf.currentTerm, rf.log)
		reply.ApplyFail = true
		reply.ConflictIndex = len(rf.log) //这里要不要减1呢？
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		Debug(dError, "Leader=%v, args.PrevLogTerm=%v 不等于Server=%v log[args.PrevLogIndex].term=%v", args.LeaderId, args.PrevLogTerm, rf.me, rf.log[args.PrevLogIndex].Term)

		reply.ApplyFail = true //日志的对齐检查是否失败

		//开始执行快速回退
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				reply.ConflictIndex = i + 1
				break
			}
		}
		rf.log = rf.log[:args.PrevLogIndex] //如果不匹配，删除当前以后续元素
		return
	} else { //匹配上了,把args中的entry一股脑添加上去
		Debug(dInfo, "Follower=%v在args.PrevLogIndex=%v处对齐了,附带的entry为%v", rf.me, args.PrevLogIndex, args.Entries)
		//例如Leader日志 [-1 10]
		//Follower日志[-1 10 20]
		//我们需要删除20这条entry
		rf.log = rf.log[:args.PrevLogIndex+1]

		if len(args.Entries) > 0 {
			rf.log = append(rf.log, args.Entries...)
		}
		reply.Success = true
		reply.FollowerTerm = rf.currentTerm

	}

	//更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		// rf.applyLogs()
	}
	if len(rf.log) != 1 {
		Debug(dLog, "Follower=%v, Term= %v的日志为: %v", rf.me, rf.currentTerm, rf.log)
	}

}

// 发送心跳信息  已经获得了rf.mu
func (rf *Raft) sendHeartBeat() {

	var muHeartBeat sync.Mutex
	// if rf.serverType == "Leader" {
	// 	Debug(dLeader, "Leader: %v,term=%v发送心跳消息", rf.me, rf.currentTerm)
	// }

	// for i := 0; i < len(rf.peers); i++ {
	// 	rf.nextIndex[i] = len(rf.log) //有待商榷
	// 	rf.matchIndex[i] = 0
	// }
	rf.lastMsgFromLeader = time.Now()
	for serverId := range rf.peers {
		if serverId != rf.me && rf.serverType == "Leader" {
			go func(i int) {

				//添加错误检查
				if rf.nextIndex[i] <= 0 || rf.nextIndex[i] > len(rf.log) {
					Debug(dError, "sendHeartBeat: rf.nextIndex[%d] out of range, rf.nextIndex[%d] = %d, len(rf.log) = %d", i, i, rf.nextIndex[i], len(rf.log))
					return
				}

				muHeartBeat.Lock() //加锁保护变量
				argsAppendEntry := &AppendEntryArgs{
					LeaderTerm:   rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1, //这里和nextIndex[i]-1的效果是一样的
					PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}

				replyAppendEntry := &AppendEntryReply{}

				//if last log index >= nextIndex for a follower, 发送的Heartbeat附加日志
				if len(rf.log)-1 >= rf.nextIndex[i] {
					Debug(dInfo, "Follower=%v 需要进行日志对齐rf.nextIndex[i]=%v,prevlogindex=%v", i, rf.nextIndex[i], argsAppendEntry.PrevLogIndex)
					argsAppendEntry.Entries = rf.log[rf.nextIndex[i]:]
				}
				muHeartBeat.Unlock()

				// Debug(dHeartbeat, "Leader=%v, Term=%v -> Server=%v", rf.me, rf.currentTerm, i)
				ok := rf.peers[i].Call("Raft.HeartBeat", argsAppendEntry, replyAppendEntry)
				if !ok {
					Debug(dError, "Heartbeat: Leader=%v, Term=%v -> Server=%v failed!", rf.me, rf.currentTerm, i)
					return
				}

				// rpc的回复过期,直接放弃
				if rf.currentTerm != argsAppendEntry.LeaderTerm {
					return
				}

				muHeartBeat.Lock()
				defer muHeartBeat.Unlock()

				//日志一致性检查通过
				if replyAppendEntry.Success {
					rf.matchIndex[i] = argsAppendEntry.PrevLogIndex + len(argsAppendEntry.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1

					//如果此次Heartbeat附加了entry，那就尝试更新leader的commitIndex
					if len(argsAppendEntry.Entries) > 0 {
						N := rf.commitIndex
						for index := rf.commitIndex + 1; index < len(rf.log); index++ {
							count := 1 // 包括 Leader 自己
							for i := range rf.peers {
								if rf.matchIndex[i] >= index {
									count++
								}
							}
							if count > len(rf.peers)/2 && rf.log[index].Term == rf.currentTerm {
								N = index
							}
						}
						if N != rf.commitIndex {
							rf.commitIndex = N
						}
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
					rf.applyEntrySuccess = 0
					rf.hasApply = false
					return

				}

				//日志对齐有问题 得用快速回退的方法
				// if replyAppendEntry.ApplyFail {
				// 	rf.nextIndex[i]--
				// 	Debug(dInfo, "rf.nextIndex[%d]递减为%v", i, rf.nextIndex[i])

				// }

				if replyAppendEntry.ApplyFail {
					if replyAppendEntry.ConflictTerm != -1 {
						lastIndexInTerm := -1
						for j := len(rf.log) - 1; j >= 0; j-- {
							if rf.log[j].Term == replyAppendEntry.ConflictTerm {
								lastIndexInTerm = j
								break
							}
						}
						if lastIndexInTerm != -1 {
							rf.nextIndex[i] = lastIndexInTerm + 1
						} else {
							rf.nextIndex[i] = replyAppendEntry.ConflictIndex
						}
					} else {
						rf.nextIndex[i] = replyAppendEntry.ConflictIndex
					}
					Debug(dInfo, "rf.nextIndex[%d]快速回退到%v", i, rf.nextIndex[i])
				}
				// else {
				// 	//处理日志对齐的问题
				// 	// Debug(dInfo, "Server %v, Term=%v现在的状态是%v", rf.me, rf.currentTerm, rf.serverType)
				// 	for replyAppendEntry.Success == false && ok && rf.serverType == "Leader" { //不能用for循环
				// 		muHeartBeat.Lock()
				// 		Debug(dError, "日志没对齐")
				// 		// replyAppendEntry.Success=false	//每次都重置Success
				// 		rf.nextIndex[i]--                                  //如果pervlogindex没有匹配上，准备重发
				// 		argsAppendEntry.PrevLogIndex = rf.nextIndex[i] - 1 //PrevLogIndex回退一个
				// 		if argsAppendEntry.PrevLogIndex < 0 {
				// 			Debug(dError, "Leader %v, Term %v的PrevLogIndex为%v", rf.me, rf.currentTerm, argsAppendEntry.PrevLogIndex)
				// 		}

				// 		argsAppendEntry.Entries = append([]LogEntry{rf.log[argsAppendEntry.PrevLogIndex+1]}, argsAppendEntry.Entries...)
				// 		argsAppendEntry.PrevLogTerm = rf.log[argsAppendEntry.PrevLogIndex].Term
				// 		muHeartBeat.Unlock()
				// 		ok := rf.peers[i].Call("Raft.HeartBeat", argsAppendEntry, replyAppendEntry)
				// 		if !ok {
				// 			Debug(dAppendEntry, "Leader=%v, Term=%v -> Server=%v重发失败", rf.me, rf.currentTerm, i)
				// 		}
				// 	}
				// 	if replyAppendEntry.Success { //如果日志对齐成功
				// 		// rf.nextIndex[i] = len(rf.log)

				// 	}
				// }

			}(serverId)
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 500 + (rand.Int63() % 400)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		checkTime := time.Now()
		rf.mu.Lock()
		duration := checkTime.Sub(rf.lastMsgFromLeader)
		//500~900ms没收到leader的消息就发起选举
		if duration > time.Duration(ms)*time.Millisecond && rf.serverType != "Leader" {
			Debug(dInfo, "距离上一次收到Leader的消息过去了:%v ms\n", duration.Milliseconds())
			Debug(dInfo, "Follower %v准备发起选举\n", rf.me)
			rf.sendRequestVote()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.serverType == "Leader" {
			// leader发送消息
			rf.sendHeartBeat()
		}
		rf.mu.Unlock()
		//每100ms发送一轮
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// 应用日志条目到状态机，并更新 lastApplied
func (rf *Raft) applyLogs() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < len(rf.log)-1 {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Entry,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
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
	// rf.applyEntry = make([]bool, 0)
	rf.applyEntrySuccess = 0
	rf.hasApply = false

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) //有待商榷
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeat()

	go rf.applyLogs()

	return rf
}
