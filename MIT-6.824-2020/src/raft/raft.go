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
/*
ApplyMsg
每次一个新条目被提交到日志中时，每个筏对等点
应该向服务(或测试人员)发送ApplyMsg
在同一服务器上。
*/
import (
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "labgob"

/*
	对于整体任务有四种状态 :
	MASTERMAP : 正常初始化后就是 MAP
	MASTERREDUCE : 所有 MAP 成功完成后
	MASTERCOMPLETE : 所有 REDUCE 成功完成后
	MASTERFAILED : 异常初始化后
*/
type ERaftStatus int32
const (
	RaftFollower ERaftStatus = iota
	RaftCandidate
	RaftLeader
)

const NOLEADER = -1
const MINHEARTBEATTIMEOUT int64 = 1000000
const HEARTBEATTIMEOUTSECTIONSIZE int64 = 1000 // 如若为负数会报错

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
/*
当每个筏对等点意识到连续的日志条目是
提交后，对等方应该发送一个ApplyMsg到服务(或
通过传递给Make()的applyCh在同一服务器上。
集
命令valid为true表示ApplyMsg包含一个新
提交的日志条目。

在实验3中，你会想要发送其他类型的信息(例如，
快照)在applyCh上;
此时，您可以添加字段到
ApplyMsg，但将命令有效设置为false用于其他用途。
*/
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers RPC所有对等点的端点,依赖该属性进行rpc通信。
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill() 服务器是否死亡...
	cond	  *sync.Cond	      // 用来控制raft主动行为的条件变量
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	/*
	关于 raftStatus 的特殊说明 :
		raft当前所处的状态,会通过该事件来判断当前raft所处的状态来进行不同的主动行为,在事件发生时,会通过修改该属性来切换raft状态。

	在32bit cpu 上,一次写四字节,所以简单的读写操作如若上下文无关,则无需加锁。
	*/
	raftStatus ERaftStatus
	CurrTerm uint32					/* 当前选举周期 */
	hasVote	bool					/* 在当前选举周期是否已经投过了票,投给自己也算 */
	acquiredVote uint				/* 在当前选举周期获得的票数 */
	acquiredVoteStatus []bool		/* 获取票数的状态,只在候选者状态有用(转换为候选者状态时恢复) */
	currLeader int					/* 当前届领导者 */
	heartbeatTimer *time.Timer 		/* 心跳定时器,只有在追随者状态下才有效 */
	voteTimer *time.Timer 			/* 选举定时器,只有在候选者状态下才有效 */
}




/*
选举超时处理函数 : 只有在当前处于候选者状态下会被调用
选举周期加一,获得的票数清零,获得的票数加一,向其他服务器发起投票请求。
*/
func (rf *Raft) voteTimeoutEventProc(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.raftStatus != RaftCandidate{
		log.Fatal("voteTimeoutEventProc, rf.raftStatus != RaftCandidate, 错误的raft状态")
	}
	if rf.voteTimer != nil { // 对候选定时器做处理
		rf.voteTimer = nil
	}
	rf.CurrTerm++
	rf.acquiredVote = 1
	for i := 0; i < len(rf.peers); i++{
		rf.acquiredVoteStatus[i] = i == rf.me
	}
	rf.currLeader = NOLEADER
}

/*
心跳超时处理函数 : 只有在当前处于追随者状态下会被调用
raft变为候选人,获得的票数清零,选举周期加一,获得的票数加一,向其他服务器发起投票请求。
*/
func (rf *Raft) heartTimeoutEventProc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.raftStatus != RaftFollower {
		log.Fatal("heartTimeoutEventProc, rf.raftStatus != RaftFollower, 错误的raft状态")
	}
	if rf.heartbeatTimer != nil { // 对心跳定时器做处理
		rf.heartbeatTimer = nil
	}
	rf.CurrTerm++
	rf.acquiredVote = 1 // 自己投自己一票
	for i := 0; i < len(rf.peers); i++{
		rf.acquiredVoteStatus[i] = i == rf.me
	}
	rf.currLeader = NOLEADER
	rf.raftStatus = RaftCandidate
	rf.cond.Broadcast()
}

/*
以下是无锁状态下的三种主动行为。
*/
func (rf *Raft) asCandidate () {
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.voteTimer = time.AfterFunc(limit, rf.voteTimeoutEventProc) // 开启选举超时
	args := RequestVoteArgs{
		Requester : rf.me,
		CurrTerm : rf.CurrTerm,
	}
	CurrTerm := rf.CurrTerm
	voteSucceed := false
	for rf.raftStatus == RaftCandidate && rf.CurrTerm == CurrTerm && rf.acquiredVote < rf.acquiredVote / 2 + 1 {
		for i := 0; i < len(rf.peers); i++{
			if !rf.acquiredVoteStatus[i] {
				// 趁着发送消息的间隙,解锁,看看这时候有没有事件发生
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				rf.sendRequestVote(i, &args, &reply)
				rf.mu.Lock()

				// 看一下投票请求过程中是否有事件发生,如若状态发生改变,则退出。
				if rf.raftStatus != RaftCandidate || rf.CurrTerm != CurrTerm {
					break
				}
				// 当前状态下仍然是当前届投票状态
				if reply.ReplyStatus && !rf.acquiredVoteStatus[reply.Replyer] {
					rf.acquiredVoteStatus[reply.Replyer] = true
					rf.acquiredVote++
					if rf.acquiredVote >= rf.acquiredVote / 2 + 1 {
						voteSucceed = true
						break
					}
				}
			}
		}
	}
	// 如若投票成功
	if voteSucceed {
		rf.acquiredVote = 0
		rf.currLeader = rf.me
		rf.raftStatus = RaftLeader
	}
}

func (rf *Raft) asLeader () {
	args := HeartbeatArgs{
		Sender: rf.me,
		CurrTerm: rf.CurrTerm,
	}
	CurrTerm := rf.CurrTerm
	for rf.raftStatus == RaftLeader && rf.CurrTerm == CurrTerm {
		for i := 0; i < len(rf.peers); i++{
			if i != rf.me{
				// 趁着发送消息的间隙,解锁,看看这时候有没有事件发生
				rf.mu.Unlock()
				reply := HeartbeatReply{}
				rf.sendHeartbeat(i, &args, &reply)
				rf.mu.Lock()
				// 看一下投票请求过程中是否有事件发生,如若状态发生改变,则退出。
				if rf.raftStatus != RaftLeader || rf.CurrTerm != CurrTerm {
					break
				}
			}
		}
		// 根据定时器,来定时堵塞(也可以唤醒)。
	}
}

func (rf *Raft) asFollower () {
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
	for rf.raftStatus == RaftFollower{
		rf.cond.Wait()
	}
}

func (rf *Raft) asFollowerProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	reply.Replyer = rf.me
	if args.CurrTerm < rf.CurrTerm {
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	// 如若之前心跳计时器开着,则先关闭
	if rf.heartbeatTimer != nil{
		if !rf.heartbeatTimer.Stop(){
			log.Fatal(rf.me,"作为追随者关闭定时器异常")
		}
	}
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
	// 判断异常
	if args.CurrTerm == rf.CurrTerm && args.Sender != rf.currLeader{
		log.Fatal(rf.me,"在第 ",rf.CurrTerm," 领导应该是 ",rf.currLeader," 却收到 ",args.Sender," 发送的心跳包")
	}
	if args.CurrTerm > rf.CurrTerm {
		rf.CurrTerm = args.CurrTerm
		rf.hasVote = true // 其他当前论候选者的投票请求都拒绝。
		rf.currLeader = args.Sender
		rf.raftStatus = RaftFollower
	}
}

func (rf *Raft) asCandidateProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	reply.Replyer = rf.me
	if args.CurrTerm < rf.CurrTerm {
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	// 如若之前候选计时器开着,则先关闭
	if rf.voteTimer != nil {
		if !rf.voteTimer.Stop(){
			log.Fatal(rf.me,"作为候选者关闭定时器异常")
		}
	}
	rf.CurrTerm = args.CurrTerm
	rf.hasVote = true // 其他当前论候选者的投票请求都拒绝。
	rf.currLeader = args.Sender
	rf.raftStatus = RaftFollower
}

func (rf *Raft) asLeaderProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	reply.Replyer = rf.me
	if args.CurrTerm < rf.CurrTerm {
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	if args.CurrTerm == rf.CurrTerm {
		log.Fatal(rf.me,"在第 ",rf.CurrTerm," 领导应该是自己却收到 ",args.Sender," 发送的心跳包")
	}

	rf.CurrTerm = args.CurrTerm
	rf.hasVote = true // 其他当前论候选者的投票请求都拒绝。
	rf.currLeader = args.Sender
	rf.raftStatus = RaftFollower
}

func (rf *Raft) asFollowerProcRequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Replyer = rf.me
	if args.CurrTerm < rf.CurrTerm {
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	if args.CurrTerm == rf.CurrTerm && rf.hasVote{
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	if rf.heartbeatTimer != nil {
		if !rf.heartbeatTimer.Stop(){
			log.Fatal(rf.me,"作为追随者关闭定时器异常")
		}
	}

	reply.CurrTerm = rf.CurrTerm
	if args.CurrTerm > rf.CurrTerm{reply.CurrTerm = rf.CurrTerm // 把原来的任期发给他
		rf.CurrTerm = args.CurrTerm
	}else {
		limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
		rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
	}
	rf.hasVote = true
	rf.currLeader = NOLEADER
	rf.raftStatus = RaftFollower
	reply.ReplyStatus = true
}

func (rf *Raft) asCandidateProcRequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Replyer = rf.me
	if args.CurrTerm < rf.CurrTerm {
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	if args.CurrTerm == rf.CurrTerm {
		if !rf.hasVote{ // 当前状态一定要是已投票,否则不安全。
			log.Fatal(rf.me, "在 asCandidateProcRequestVote 中 !rf.hasVote 出错。")
		}
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	// 如若之前候选计时器开着,则先关闭
	if rf.voteTimer != nil {
		if !rf.voteTimer.Stop(){
			log.Fatal(rf.me,"作为候选者关闭定时器异常")
		}
	}

	reply.CurrTerm = rf.CurrTerm // 把原来的任期发给他
	rf.CurrTerm = args.CurrTerm
	rf.hasVote = true
	rf.currLeader = NOLEADER
	rf.raftStatus = RaftFollower
	reply.ReplyStatus = true
}

func (rf *Raft) asLeaderProcRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Replyer = rf.me
	if args.CurrTerm < rf.CurrTerm {
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	if args.CurrTerm == rf.CurrTerm {
		log.Fatal(rf.me,"在第 ",rf.CurrTerm," 领导应该是自己却收到 ",args.Requester," 发送的心跳包")
	}

	rf.CurrTerm = args.CurrTerm
	rf.hasVote = true // 其他当前论候选者的投票请求都拒绝。
	rf.currLeader = args.Requester
	rf.raftStatus = RaftFollower
}

// return currentTerm and whether this server
// believes it is the leader.
// 返回 本届任期 和 该服务器是否认为自己是leader。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return int(rf.CurrTerm), rf.currLeader == rf.me
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
/*
将筏子的持续状态保存为稳定存储，在碰撞后可以重新启动。请参阅paper的图2，了解什么应该是持久的。
*/
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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


type HeartbeatArgs struct{
	Sender int
	CurrTerm uint32
}

type HeartbeatReply struct{
	ReplyStatus bool // 答复状态
	Replyer int		// 答复者
	CurrTerm uint32 // 答复者的当前任期
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// RequestVote RPC参数结构示例。
// 字段名称必须以大写字母开头!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Requester int // 请求者
	CurrTerm uint32 // 当前选举的届数
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ReplyStatus bool // 答复状态
	Replyer int// 答复者
	CurrTerm uint32 // 答复者的当前任期
}

/*
对外提供的服务 : 接收心跳包

*/
func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.raftStatus{
	case RaftFollower :
		rf.asFollowerProcHeartbeat(args, reply)
		break
	case RaftCandidate :
		rf.asCandidateProcHeartbeat(args, reply)
		break
	case RaftLeader :
		rf.asLeaderProcHeartbeat(args, reply)
		break
	default:
		log.Fatal(rf.me,"当前处于未注册的状态中 : rf.raftStatus = ",rf.raftStatus)
	}
}


//
// example RequestVote RPC handler.
//
// 接收投票消息,并返回结果。
func (rf *Raft) RequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch rf.raftStatus{
	case RaftFollower :
		rf.asFollowerProcRequestVote(args, reply)
		break
	case RaftCandidate :
		rf.asCandidateProcRequestVote(args, reply)
		break
	case RaftLeader :
		rf.asLeaderProcRequestVote(args, reply)
		break
	default:
		log.Fatal(rf.me,"当前处于未注册的状态中 : rf.raftStatus = ",rf.raftStatus)
	}
}

//
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
//
// 向编号为i的服务器发送投票消息
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)
	return ok
}

//
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
//
/*
第一个返回值是命令在提交时出现的索引。
第二个返回值是本届任期。
如果该服务器认为自己是leader，则第三个返回值为true。
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}



/*
函数功能 : 三种模式下表现为三种策略。并且在这三种策略中进行转换。
*/
func (rf *Raft) run () {
	for true{
		rf.mu.Lock()
		switch rf.raftStatus {
		case RaftFollower :
			rf.asFollower()
			break
		case RaftCandidate :
			rf.asCandidate()
			break
		case RaftLeader :
			rf.asLeader()
			break
		default:
			panic("未定义的状态")
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
/*
服务或测试人员希望创建一个Raft服务器。
所有Raft服务器(包括这个服务器)的端口都在peer[]中。
此服务器的端口是peer[me]。所有服务器的对等点[]数组具有相同的顺序。
persister是此服务器保存其持久状态的地方，最初还保存最近保存的状态(如果有的话)。
applyCh是测试者或服务期望筏发送ApplyMsg消息的通道，
Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutines。
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers : peers,
		persister : persister,
		me : me,
		dead : 0,
		raftStatus : RaftFollower,
		CurrTerm : 0,
		hasVote : false,
		acquiredVote : 0,
		currLeader : 0,
		heartbeatTimer : nil,
		voteTimer : nil,
	}
	rf.cond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	// 在崩溃前从持久化状态进行初始化
	rf.readPersist(persister.ReadRaftState())

	go rf.run()
	return rf
}
