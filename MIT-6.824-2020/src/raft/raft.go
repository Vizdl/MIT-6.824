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
import "sync"
import "sync/atomic"
import "labrpc"

// import "bytes"
// import "labgob"



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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
// 返回 本届任期 和 该服务器是否认为自己是leader。
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// RequestVote RPC参数结构示例。
// 字段名称必须以大写字母开头!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Requester int // 请求者
	CurrVoteTimes int // 当前选举的届数
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ReplyStatus int // 答复状态
	Replyer // 答复者
}

//
// example RequestVote RPC handler.
//
// 接收投票消息,并返回结果。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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

func (rf *Raft) run () {

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
/*
制造出一个 Raft 服务器
填写RequestVoteArgs和 RequestVoteReply结构。
修改 Make()以创建一个后台goroutine，
该后台goroutine将在有一段时间没有收到其他对等方的请求时
通过发出RequestVote RPC来定期启动领导者选举。
这样，同伴将了解谁是领导者（如果已经有领导者），或者成为领导者本身。
实现RequestVote() RPC处理程序，以便服务器相互投票。

思考 : 还是定时器问题.
如何判断谁是领导呢?

概述 : 
	在Raft中，有两个超时(选举超时和心跳超时)设置可控制选举。
	首先是选举超时。
	选举超时 : 选举超时是指追随者成为候选人之前的等待时间。
	选举超时被随机分配在150毫秒至300毫秒之间。
	选举超时后，关注者成为候选人并开始新的选举任期 ...
	为自己投票...并将请求投票消息发送给其他节点。
	如果接收节点在这个学期还没有投票，那么它将投票给候选人。
	...并且节点重置其选举超时。一旦候选人获得多数票，它就会成为领导者。
	领导者开始向其关注者发送“ 添加条目”消息。这些消息以心跳超时指定的时间间隔发送。
	跟随者然后响应每个追加条目消息。
	此选举任期将持续到追随者停止接收心跳并成为候选人为止。

	让我们停止领导，观察选举连任。
	节点B现在是任期2的负责人。
	要获得多数票，可以保证每学期只能选出一位领导人。
	如果两个节点同时成为候选节点，则可能会发生拆分表决。
	让我们看一个分割投票的例子...
	两个节点都开始以相同的任期进行选举...
	...每个都先到达一个跟随者节点。
	现在，每位候选人都有2票，并且在这个任期中将无法获得更多选票。
	则超时等待新的任期..

问题 : 
	1) 如何保证只有一个领导者。


跟随者 <-> 候选人 -> 领导者
两种超时 : 
	1) 心跳超时 : 跟随者在一段时间没有收到心跳包,自动开启选举超时。
	2) 选举超时 : 选举超时被随机分配在150毫秒至300毫秒之间。
	如若选举超时则自己到下一期选举状态去。开启新一轮投票。
关系转换条件 :
	群众超时没收到心跳包,自动成为候选人。
	候选人得到大多数选票,升级成为领导者。
	候选人未得到大多数选票,退回群众。
	领导者如若没有死,就一直是领导者。
选票行为规范 :
	1) 只要有人说 : 我是领导,那么所有人都会认为他是领导。
	2) 只要心跳超时,马上就变候选人了。
	3) 如若已经有一半一直无法恢复,则无法选出新的领导。
	4) 对于一轮选举而言
		每个候选者都会记载自己的投票结果(三种) : 赞成,反对,无响应。
		每个追随者 : 这轮选了谁。
情境状态 : 
	1) 如若当前集群中存在领导,则存在所有的状态可能。
	但无论其他人处于什么状态。收到了领导的心跳包,就认为他是领导(他是具有大多数票的)。
	如若当前无领导,则可能存在两种状态 : 追随者, 候选人。
	2) 无论如何,如若一个raft得到大多数选票,则直接向 状态1) 转换。
	其他人不可能再拿到大多数选票了。
	问题 : 是否存在当前领导当选,但某一raft以为自己进入新一轮选举了?
	回答 : 可能。
	问题 : 如何避免上述问题导致可能选出两个领导来?
		× 1) 就算他进入了第 x + 1 轮选举,其他显示已经投了 x 轮任期领导的raft只要不支持他的工作就行。
			理论上可行,但是与选举状态的举措冲突(只要有更新的选举任期投票请求,就投)。
		√? 2) 他会发选票给领导,领导直接驳回就行了。
			这似乎与 得到大多数选票立刻成为领导 的决策有冲突,因为可能在没被驳回之前就已经得到大多数选票了。
			
选举状态分析/选举任期分析/候选人与追随者行为分析 : 
	1) 存在一个候选者
		1) 候选者掌握的选举周期最大。 
			追随者 : 收到投票后,同步选举周期为候选人选举周期。然后投票。
		2) 候选人掌握的选举周期比所有追随者都小
			追随者 : 收到投票后,直接投反对票(存疑)。
	2) 存在多个候选者
		
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	// 在崩溃前从持久化状态进行初始化
	rf.readPersist(persister.ReadRaftState())


	return rf
}
