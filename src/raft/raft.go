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

	//	"6.5840/labgob"
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
	entry     []byte //日志的每个条目
	heartBeat int    //心跳信息
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
	log               LogEntry
	serverType        string    //每个服务器的身份
	lastMsgFromLeader time.Time //当前Server最后一次收到leader消息的时间
	voteCount         int       //candidate的投票计数  记得重置投票！
	lastLogIndex      int
	lastLogTerm       int
	voteState         map[int]bool //如果当前服务器是candidate，记录其他follower的投票情况

}

type AppendEntry struct {
	heartBeat int
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.voteFor)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
	TermLeader   int
	LeaderId     int
	PreLogIndex  int
	Entries      []string //为空则表示心跳信息
	LeaderCommit int
}

type AppendEntryReply struct {
	FollowerTerm int
	Success      bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// fmt.Printf("Follower: %v开始处理来自Candidate: %v的投票请求\n", rf.me, args.CandidateId)

	if rf.serverType != "Follower" {
		reply.VoteGranted = false
		return
	}

	// rf.lastMsgFromLeader=time

	//比较发出投票请求的candidate与自己的term大小
	if args.CandidateTerm < rf.currentTerm {
		// fmt.Println("发出请求的Candidate的Term比Follower小")
		reply.FollowerTerm = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//一定要持久化记录投票情况
	if args.CandidateTerm > rf.currentTerm {
		// reply.FollowerTerm = rf.currentTerm
		// rf.currentTerm = args.CandidateTerm
		// rf.serverType = "Follower"
		rf.voteFor = -1
	}

	if rf.voteFor == -1 {
		if args.LastLogTerm > rf.lastLogTerm {
			rf.voteFor = args.CandidateId
			reply.FollowerTerm = rf.currentTerm
			reply.VoteGranted = true
		} else if args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex >= rf.lastLogIndex {
			rf.voteFor = args.CandidateId
			reply.FollowerTerm = rf.currentTerm
			reply.VoteGranted = true
		}
	}

	if rf.voteFor == args.CandidateId { //说明同一个Candidate因为网络问题发送了好几个投票请求
		reply.FollowerTerm = rf.currentTerm
		reply.VoteGranted = true //只投一次票
		// fmt.Printf("Follower:%v给Candidate:%v投票了\n", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted = false
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
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	if reply.VoteGranted == true {
		rf.voteCount++
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//收到leader的消息后，Server重置自己的选举状态
	// rf.serverType = "Follower"
	rf.voteFor = -1
	rf.voteCount = 0
	rf.voteState = make(map[int]bool)

	rf.lastMsgFromLeader = time.Now() //更新收到消息的时间
	//如果leader的term小于Follower的term
	if args.TermLeader < rf.currentTerm {
		reply.Success = false
		reply.FollowerTerm = rf.currentTerm
		return
	}

	reply.Success = true
	reply.FollowerTerm = rf.currentTerm
	rf.currentTerm = args.TermLeader

}

// 向serverId这个服务器发送心跳信息
func (rf *Raft) sendAppendEntries(serverId int, args *AppendEntryArgs, reply *AppendEntryReply) {

	ok := rf.peers[serverId].Call("Raft.AppendEntries", args, reply)
	//如果Leader发送的心跳消息失败了，直接创建个goroutine进行不断
	if !ok {
		// for { //每秒最多可以给某个Follower发送10-1次次消息
		// 	ok := rf.peers[serverId].Call("Raft.AppendEntries", args, reply)
		// 	if ok { //如果成功发送，则退出
		// 		break
		// 	}
		// 	// time.Sleep(time.Second / 9)
		// }
		// // go rf.repeatlySendAppendEntries(serverId, args, reply)

		// fmt.Printf("Leader:%v在Term=%v时,给%v发送heartbeat失败\n", rf.me, rf.currentTerm, serverId)
		return
	}

	rf.mu.Lock()
	if reply.FollowerTerm > rf.currentTerm {
		//旧王已陨，新帝当立
		// fmt.Printf("********%v陨落,它的Term=%v********\n", rf.me, rf.currentTerm)
		rf.currentTerm = reply.FollowerTerm
		rf.serverType = "Follower"
	}
	rf.mu.Unlock()
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

	// Your code here (3B).

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

// //选出leader后重置所有服务器状态
// func (rf *Raft)SetServerState(serverId int,args){
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// }

// 处理选举的函数
func (rf *Raft) ElectionStart() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//重置自己的状态

	rf.voteState = make(map[int]bool)
	rf.serverType = "Candidate"
	rf.currentTerm++
	//重置选举超时计时器
	rf.lastMsgFromLeader = time.Now()
	//首先给每个follower发送信息
	rf.voteCount++ //先给自己投票
	rf.voteFor = rf.me

	// fmt.Printf("Candidate %v准备发起选举,Term=%v\n", rf.me, rf.currentTerm)
	// 向其他节点发送投票请求
	for serverId := range rf.peers {
		args := RequestVoteArgs{
			CandidateTerm: rf.currentTerm,
			CandidateId:   rf.me,
		}
		reply := RequestVoteReply{}
		if serverId == rf.me { //如果是自己就跳过此次循环
			continue
		}
		go rf.sendRequestVote(serverId, &args, &reply)
	}

	// fmt.Printf("Candidate: %v,获得的选票是: %v\n", rf.me, rf.voteCount)

	if rf.voteCount >= (len(rf.peers)+1)/2 {
		rf.serverType = "Leader"
		// fmt.Printf("********%v成为了Leader,term=%v********\n", rf.me, rf.currentTerm)
		//如果当前Server成为了Leader，就开始不断发送消息巩固统治地位
		for rf.serverType == "Leader" {
			argsAppendEntry := AppendEntryArgs{
				TermLeader: rf.currentTerm,
				LeaderId:   rf.me,
				Entries:    make([]string, 0),
			}
			for serverId := range rf.peers {
				replyAppendEntry := AppendEntryReply{}
				if serverId != rf.me {
					go rf.sendAppendEntries(serverId, &argsAppendEntry, &replyAppendEntry)
				}
			}
			if rf.serverType != "Leader" {
				break
			}
			//发送完一轮heartbeat后休眠一段时间
			time.Sleep(100 * time.Millisecond)
		}

	}

	// //选举完成后重置所有Follower的状态
	// for serverId, peer := range rf.peers {

	// }

	//candidate选举失败后就重置自己的状态
	// fmt.Printf("Candidate:%v在Term=%v的选举失败了\n", rf.me, rf.currentTerm)
	rf.serverType = "Follower"
	rf.voteFor = -1
	rf.voteCount = 0
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
		duration := checkTime.Sub(rf.lastMsgFromLeader)
		// fmt.Printf("距离上一次收到Leader的消息过去了:%v s\n", duration)

		if duration > time.Duration(ms)*time.Millisecond && rf.serverType != "Leader" { //600ms没收到leader的消息就发起选举
			// fmt.Printf("Follower %v准备发起选举\n", rf.me)
			rf.ElectionStart()
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

	rf.currentTerm = 0
	rf.voteCount = 0
	rf.voteFor = -1
	rf.serverType = "Follower"
	rf.lastMsgFromLeader = time.Time{}
	rf.lastLogIndex = -1
	rf.lastLogTerm = -1
	rf.voteState = make(map[int]bool)
	rf.log = LogEntry{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
