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
	leaderSendTimer *time.Timer 	/* 领导者发送心跳包计时器 */
	voteTimer *time.Timer 			/* 选举定时器,只有在候选者状态下才有效 */
}



/*
选举成功处理
*/
func (rf *Raft) voteSucceedEventProc(){
	rf.acquiredVote = 0
	rf.currLeader = rf.me
}
/*
选举超时处理函数
选举周期加一,获得的票数清零,获得的票数加一,向其他服务器发起投票请求。
*/
func (rf *Raft) voteTimeoutEventProc(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.CurrTerm++
	rf.acquiredVote = 1
	for i := 0; i < len(rf.peers); i++{
		rf.acquiredVoteStatus[i] = i == rf.me
	}
	rf.currLeader = NOLEADER
}

/*
心跳超时处理函数
raft变为候选人,获得的票数清零,选举周期加一,获得的票数加一,向其他服务器发起投票请求。
*/
func (rf *Raft) heartTimeoutEventProc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.CurrTerm++
	rf.acquiredVote = 1 // 自己投自己一票
	for i := 0; i < len(rf.peers); i++{
		rf.acquiredVoteStatus[i] = i == rf.me
	}
	rf.currLeader = NOLEADER
	rf.raftStatus = RaftCandidate
	rf.cond.Broadcast()
}


func (rf *Raft) sendHeartTimeoutEventProc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := HeartbeatArgs{
	Sender: rf.me,
	CurrTerm: rf.CurrTerm,
	}
	for i := 0; i < len(rf.peers); i++{
		if i != rf.me{
			reply := HeartbeatReply{}
			rf.sendHeartbeat(i, &args, &reply)
		}
	}
}

func (rf *Raft) sendRequestVoteLoop(){
	args := RequestVoteArgs{
		Requester : rf.me,
		CurrTerm : rf.CurrTerm,
	}
	for rf.raftStatus == RaftCandidate && rf.acquiredVote < rf.acquiredVote / 2 + 1 {
		for i := 0; i < len(rf.peers); i++{
			reply := RequestVoteReply{}
			if !rf.acquiredVoteStatus[i] {
				rf.sendRequestVote(i, &args, &reply)
				if reply.ReplyStatus && !rf.acquiredVoteStatus[reply.Replyer] {
					rf.acquiredVoteStatus[reply.Replyer] = true
					rf.acquiredVote++
					if rf.raftStatus != RaftCandidate || rf.acquiredVote >= rf.acquiredVote / 2 + 1 {
						break
					}
				}
			}
		}
	}
	// 如若投票成功
	rf.voteSucceedEventProc()
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
	reply.Replyer = rf.me
	if args.CurrTerm < rf.CurrTerm { // 拒绝任何更低任期的心跳
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	// 臣服于任何大于等于当前任期的领导者
	if args.CurrTerm > rf.CurrTerm { // 如若自己消息落后了。
		rf.CurrTerm = args.CurrTerm
		rf.hasVote = true // 其他当前论候选者的投票请求都拒绝。
		rf.currLeader = NOLEADER
		rf.raftStatus = RaftFollower
	}
}


func (rf *Raft) sendHeartbeat(server int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)
	return ok
}
//
// example RequestVote RPC handler.
//
// 接收投票消息,并返回结果。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Replyer = rf.me
	if args.CurrTerm < rf.CurrTerm {
		reply.CurrTerm = rf.CurrTerm
		reply.ReplyStatus = false
		return
	}
	reply.CurrTerm = rf.CurrTerm // 把原始的 term 作为回复
	if args.CurrTerm > rf.CurrTerm { // 如若是新一轮选举,则任何人都会变成追随者(包括上一届领导者)
		rf.CurrTerm = args.CurrTerm
		rf.hasVote = false
		rf.currLeader = NOLEADER
		rf.raftStatus = RaftFollower
	}

	// 当前双方对选举任期一致
	reply.ReplyStatus = !rf.hasVote
	if reply.ReplyStatus {
		rf.hasVote = true
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
以下是无锁状态下的三种主动行为。
*/
func (rf *Raft) asCandidate () {

}

func (rf *Raft) asLeader () {
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.sendHeartTimeoutEventProc)
	for rf.raftStatus == RaftLeader{
		rf.cond.Wait()
	}
}

func (rf *Raft) asFollower () {
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
	for rf.raftStatus == RaftFollower{
		rf.cond.Wait()
	}
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
