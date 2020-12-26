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
	"bytes"
	"fmt"
	"labgob"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"


type ERaftStatus int32
const (
	RaftFollower ERaftStatus = iota
	RaftCandidate
	RaftLeader
	RaftDead
)

const NOLEADER = -1
const NOVOTEFOR = -1
const MINHEARTBEATTIMEOUT int64 = 150000000
const HEARTBEATTIMEOUTSECTIONSIZE int64 = 200000000 // 如若为负数会报错
const MINVOTETIMEOUT int64 = 150000000
const VOTETIMEOUTTIMEOUTSECTIONSIZE int64 = 200000000 // 如若为负数会报错
const HEARTBEATTIMEOUT int64 = 50000000
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
当每个raft意识到接替的日志条目是提交后，
对等方应该发送一个ApplyMsg到同一服务器上的service(or tester),
通过传递给Make()的applyCh,将CommandValid设置为true，
表示ApplyMsg包含一个新提交的日志条目。

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

type LogEntries struct {
	Term 	int				/* 日志产生的任期 */
	Command interface{}		/* 日志消息 */
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers RPC所有对等点的端点,依赖该属性进行rpc通信。
	persister *Persister          // Object to hold this peer's persisted state
	applyCh   chan ApplyMsg		  // 发送到服务器..
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill() 服务器是否死亡...4字节简单数据,赋值不需要加锁
	cond	  *sync.Cond	      // 用来控制raft主动行为的条件变量

	// state a Raft server must maintain.

	raftStatus ERaftStatus

	currTerm int					/* 当前选举任期数,需要持久化 */
	voteFor	int						/* 在当前选举任期是否已经投过了票,投给自己也算,需要持久化 */
	lastLogIndex int 				/* 上一条日志的索引 */
	lastLogTerm int 				/* 上一条日志的任期 */
	commitIndex int					/* 当前节点已知的,最大的,已提交的日志索引 */
	lastApplied int 				/* 表示当前节点最后一条被应用到状态机的日志索引 */
	logBuff []LogEntries			/* 日志缓存,内含日志条目与日志产生的任期 */

	currLeader int					/* 作为追随者有效, 当前届领导者 */
	heartbeatTimer *time.Timer 		/* 作为追随者有效, 心跳定时器 */
	acquiredVote uint				/* 作为候选者有效, 在当前选举周期获得的票数 */
	hasVoteResult []bool			/* 作为候选者有效, 对方在当前轮是否给我投过票,即使对方投的是反对票,这里也会变为true */
	voteTimer *time.Timer 			/* 作为候选者有效, 选举定时器 */
	nextIndex []int					/* 作为领导者有效, nextIndex[i] : ID 为 i 的 raft 下一条日志的索引的猜测值,用以同步日志。 */
	logPersistRecord []int			/* 作为领导者有效, logPersistRecord[i] : 第 i 条日志条目被 logPersistRecord[i] 台 raft 持久化,领导者用来统计当前任期内日志是否应该被提交 */
}

/*
服务器状态转换系列函数
*/
func (rf *Raft) toBeFollower (currTerm int, voteFor int, currLeader int){
	rf.raftStatus = RaftFollower
	rf.currLeader = currLeader
	rf.currTerm = currTerm
	rf.voteFor = voteFor
	rf.persist()
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
}

func (rf *Raft) toBeCandidate(){
	if rf.voteTimer != nil && rf.voteTimer.Stop(){ // 对候选定时器做处理
		log.Fatal("成为追随者上一个定时器却仍未关闭")
	}
	rf.currTerm++
	rf.acquiredVote = 1 // 自己投自己一票
	rf.voteFor = rf.me
	for i := 0; i < len(rf.peers); i++{
		rf.hasVoteResult[i] = i == rf.me
	}
	rf.currLeader = NOLEADER
	rf.raftStatus = RaftCandidate
	limit := time.Duration(MINVOTETIMEOUT + rand.Int63n(VOTETIMEOUTTIMEOUTSECTIONSIZE))
	rf.voteTimer = time.AfterFunc(limit, rf.voteTimeoutEventProc) // 开启选举超时
	// 开启追随者主动行为协程
	for i := 0; i < len(rf.peers); i++{
		if rf.me != i {
			go rf.toSendRequestVote(rf.currTerm, i)
		}
	}
}

func (rf *Raft) toBeLeader(){
	rf.voteSucceedLog()
	rf.acquiredVote = 0
	rf.currLeader = rf.me
	rf.raftStatus = RaftLeader
	for i := 0; i <= rf.lastLogIndex; i++{
		rf.logPersistRecord = append(rf.logPersistRecord, 1)
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1
		if i != rf.me {
			go rf.toSendHeartbeat(rf.currTerm, i)
		}
	}
}

/*
选举超时处理函数 : 只有在当前处于候选者状态下会被调用
选举周期加一,获得的票数清零,获得的票数加一,向其他服务器发起投票请求。
*/
func (rf *Raft) voteTimeoutEventProc(){
	rf.mu.Lock() // 只要进入了这个函数,就必定是是超时。
	defer rf.mu.Unlock()
	rf.voteTimeoutEventProcLog()
	if rf.raftStatus != RaftCandidate{
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"届发生选举超时, raftStatus =",rf.raftStatus,"错误的raft状态")
	}
	rf.toBeCandidate()
	rf.cond.Broadcast() // 避免关闭定时器失败时,主线程堵塞在条件变量里面了。
}

/*
心跳超时处理函数 : 只有在当前处于追随者状态下会被调用
raft变为候选人,获得的票数清零,选举周期加一,获得的票数加一,向其他服务器发起投票请求。
*/
func (rf *Raft) heartTimeoutEventProc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.raftStatus != RaftFollower {
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"届发生心跳超时, raftStatus =",rf.raftStatus,"错误的raft状态")
	}
	rf.heartTimeoutEventProcLog()
	rf.toBeCandidate()
}

func (rf *Raft) toSendRequestVote(CurrTerm int, raftId int){
	args := RequestVoteArgs{
		Requester : rf.me,
		CurrTerm : CurrTerm,
		LastLogTerm : rf.lastLogTerm,
		LastLogIndex: rf.lastLogIndex,
		CommitIndex: rf.commitIndex,
	}
	voteSucceed := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.raftStatus == RaftCandidate && rf.currTerm == CurrTerm && !rf.hasVoteResult[raftId] {
		/* 趁着发送消息的间隙,解锁,看看这时候有没有事件发生 */
		rf.mu.Unlock()
		reply := RequestVoteReply{}
		isCalled := rf.sendRequestVote(raftId, &args, &reply)
		rf.mu.Lock()
		/* 看一下投票请求过程中是否有事件发生,如若状态发生改变,则退出。 */
		if rf.raftStatus != RaftCandidate || rf.currTerm != CurrTerm {
			break
		}
		/* 如若调用失败 */
		if !isCalled {
			continue
		}
		/* 检验是否正确的回复格式 */
		if reply.Replyer != raftId {
			continue
		}

		if rf.voteTimer == nil {
			log.Fatal("第",rf.me,"台服务器处于候选者状态但是rf.voteTimer == nil")
		}
		/* 已经正确地请求投票且得到结果,对结果进行处理 */
		rf.hasVoteResult[raftId] = true
		if !reply.ReplyStatus && reply.CurrTerm > rf.currTerm{
			/* 如若关闭成功,则直接变成下一届的追随者,如若失败,等待超时事件发生 */
			if rf.voteTimer.Stop() {
				rf.toBeFollower(reply.CurrTerm, NOVOTEFOR, NOLEADER)
			}else {
				for rf.raftStatus == RaftCandidate && rf.currTerm == CurrTerm {
					rf.cond.Wait()
				}
			}
		}
		if reply.ReplyStatus && reply.CurrTerm <= rf.currTerm{
			rf.acquiredVote++
			if rf.acquiredVote >= uint(len(rf.peers)) / 2 + 1 {
				/* 如若关闭成功,则直接变成下一届的追随者,如若失败,等待超时事件发生 */
				if rf.voteTimer.Stop(){
					voteSucceed = true
				}else {
					for rf.raftStatus == RaftCandidate && rf.currTerm == CurrTerm {
						rf.cond.Wait()
					}
				}
			}
		}
		/* 只要有结果了,就退出循环 */
		break
	}
	// 如若投票成功
	if voteSucceed {
		rf.toBeLeader()
	}
}

func (rf *Raft) toSendHeartbeat(CurrTerm int, raftId int){
	/* 确保刚进入就一定能发出心跳 */
	lastTick := time.Now().UnixNano() - HEARTBEATTIMEOUT
	isMatch := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.raftStatus == RaftLeader && rf.currTerm == CurrTerm {
		tick := time.Now().UnixNano()
		if tick - lastTick >= HEARTBEATTIMEOUT {
			lastTick = tick
			args := HeartbeatArgs{
				Sender: rf.me,
				CurrTerm: CurrTerm,
				PrevIndex: rf.nextIndex[raftId] - 1,
				PrevTerm: rf.logBuff[rf.nextIndex[raftId] - 1].Term,
				HaveEnt: rf.nextIndex[raftId] <= rf.lastLogIndex && isMatch,
				CommitIndex: rf.commitIndex,
			}
			if args.HaveEnt {
				args.Entries = rf.logBuff[rf.nextIndex[raftId]]
			}
			/* 趁着发送消息的间隙,解锁,看看这时候有没有事件发生 */
			rf.mu.Unlock()
			reply := HeartbeatReply{}
			isCalled := rf.sendHeartbeat(raftId, &args, &reply)
			rf.mu.Lock()
			/* 看一下投票请求过程中是否有事件发生,如若状态发生改变,则退出。*/
			if rf.raftStatus != RaftLeader || rf.currTerm != CurrTerm {
				break
			}
			/* 如若调用失败 */
			if !isCalled {
				continue
			}
			/* 对返回的结果进行检验 */
			if reply.Replyer != raftId {
				continue
			}
			/* 正常地发送了此次的心跳 */
			if reply.CurrTerm > rf.currTerm {
				rf.toBeFollower(reply.CurrTerm, NOVOTEFOR, NOLEADER)
				break
			}
			// 如若匹配失败
			if !reply.ReplyStatus {
				isMatch = false
				rf.nextIndex[raftId] = reply.LastIndex + 1
			}else {
				isMatch = true
				if args.HaveEnt {
					if args.Entries.Term == rf.currTerm {
						rf.logPersistRecord[rf.nextIndex[raftId]]++
						if rf.logPersistRecord[rf.nextIndex[raftId]] >= len(rf.peers) / 2 + 1{
							if rf.commitIndex < rf.nextIndex[raftId]{
								rf.commitIndex = rf.nextIndex[raftId]
							}
						}
					}
					flag := false
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++{
						applyMsg := ApplyMsg {
							CommandValid : true,
							Command : rf.logBuff[i].Command,
							CommandIndex : i,
						}
						rf.applyCh <- applyMsg
						rf.lastApplied++
						rf.commitLog(applyMsg)
						flag = true
					}
					if flag{
						rf.persist()
					}
					rf.nextIndex[raftId]++
				}else {
					for i := 1; i <= args.PrevIndex; i++{
						rf.logPersistRecord[i]++
					}
				}
			}
		}
		/* 如若状态没发生改变,根据定时器,来定时堵塞。 */
		if rf.raftStatus == RaftLeader && rf.currTerm == CurrTerm {
			if time.Now().UnixNano() - lastTick > 10000000{
				rf.mu.Unlock()
				time.Sleep(time.Duration(10 * time.Millisecond))
				rf.mu.Lock()
			}else {
				time.Sleep(time.Duration(time.Millisecond))
			}
		}
	}
}

func (rf *Raft) asFollowerProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	/* 参数初始化 */
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false
	/* 检查自身状态是否正常 */
	if rf.heartbeatTimer == nil {
		fmt.Println("第 ",rf.me," 台服务器作为追随者时, rf.heartbeatTimer == nil")
		return
	}
	/* 阻拦无效数据包 */
	if args.CurrTerm < rf.currTerm {
		return
	}
	/* 如若心跳事件已经发生,拒绝所有当前心跳包,等待状态转换。 */
	if !rf.heartbeatTimer.Stop() {
		fmt.Println("第 ",rf.me," 台服务器作为追随者关闭定时器异常,表示心跳超时已经发生了")
		return
	}
	/* 如若出现两个领导者 */
	if rf.currLeader != NOLEADER && args.CurrTerm == rf.currTerm && args.Sender != rf.currLeader{
		log.Println("第 ",rf.me," 台服务器在第 ",rf.currTerm," 届收到心跳包,但领导应该是 ",rf.currLeader," 却收到 ",args.Sender," 发送的心跳包")
		return
	}
	/* args.PrevIndex <= rf.lastLogIndex 是考虑到 对方比我方日志更少 */
	reply.ReplyStatus = args.PrevIndex <= rf.lastLogIndex && args.PrevTerm ==  rf.logBuff[args.PrevIndex].Term
	/* 如若找到最后一条相同日志 */
	if reply.ReplyStatus {
		/* 删除无用日志 */
		if args.PrevIndex < rf.lastLogIndex {
			rf.logBuff = rf.logBuff[:args.PrevIndex + 1]
			rf.lastLogIndex = args.PrevIndex
			rf.lastLogTerm = rf.logBuff[rf.lastLogIndex].Term
		}
		/* 追加日志 */
		if args.HaveEnt {
			rf.logBuff = append(rf.logBuff, args.Entries)
			rf.lastLogIndex++
			rf.lastLogTerm = args.Entries.Term
		}
		/* 更新日志提交索引 */
		if args.CommitIndex > rf.commitIndex{
			/* 需要考虑匹配上的日志数量少于提交数量的情况 */
			if args.CommitIndex <= rf.lastLogIndex {
				rf.commitIndex = args.CommitIndex
			}else {
				rf.commitIndex = rf.lastLogIndex
			}
		}
	}else {
		if args.PrevIndex > rf.lastLogIndex {
			reply.LastIndex = rf.lastLogIndex
		}else {
			reply.LastIndex = args.PrevIndex - 1
		}
	}
	flag := false
	/* 向k/v服务器发送已提交未应用的消息 */
	for i := rf.lastApplied + 1; i <= rf.commitIndex && i < len(rf.logBuff); i++{
		applyMsg := ApplyMsg {
			CommandValid : true,
			Command : rf.logBuff[i].Command,
			CommandIndex : i,
		}
		rf.applyCh <- applyMsg
		rf.lastApplied++
		rf.commitLog(applyMsg)
		flag = true
	}
	/* 发生变动了,持久化 */
	if flag {
		rf.persist()
	}
	if args.CurrTerm > rf.currTerm {
		rf.toBeFollower(args.CurrTerm, args.Sender, args.Sender)
		return
	}
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
}

func (rf *Raft) asCandidateProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false
	if args.CurrTerm < rf.currTerm {
		return
	}
	// 如若之前候选计时器开着,则先关闭
	if rf.voteTimer != nil && !rf.voteTimer.Stop(){
		fmt.Println("第",rf.me,"台服务器作为候选者关闭定时器异常")
		return
	}
	reply.ReplyStatus = args.PrevIndex == rf.lastLogIndex && args.PrevTerm ==  rf.lastLogTerm
	if !reply.ReplyStatus {
		if args.PrevIndex > rf.lastLogIndex {
			reply.LastIndex = rf.lastLogIndex
		}else {
			reply.LastIndex = args.PrevIndex - 1
		}
	}
	if args.HaveEnt {
		log.Fatal("第一次心跳就发送了日志,不正常的状态。")
	}
	rf.toBeFollower(args.CurrTerm, args.Sender, args.Sender)
}

func (rf *Raft) asLeaderProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false
	if args.CurrTerm < rf.currTerm {
		return
	}
	if args.CurrTerm == rf.currTerm {
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"领导应该是自己却收到",args.Sender,"发送的心跳包")
	}
	reply.ReplyStatus = args.PrevIndex == rf.lastLogIndex && args.PrevTerm ==  rf.lastLogTerm
	if !reply.ReplyStatus {
		if args.PrevIndex > rf.lastLogIndex {
			reply.LastIndex = rf.lastLogIndex
		}else {
			reply.LastIndex = args.PrevIndex - 1
		}
	}
	if args.HaveEnt {
		log.Fatal("第一次心跳就发送了日志,不正常的状态。")
	}
	rf.toBeFollower(args.CurrTerm, args.Sender, args.Sender)
}

func (rf *Raft) asFollowerProcRequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false
	if args.CurrTerm < rf.currTerm || (args.CurrTerm == rf.currTerm && rf.voteFor != NOVOTEFOR) {
		return
	}

	// 选举限制
	reply.ReplyStatus = args.CommitIndex > rf.commitIndex ||
		(args.CommitIndex == rf.commitIndex && (args.LastLogIndex >= rf.lastLogIndex && args.LastLogTerm >= rf.lastLogTerm))
	if reply.ReplyStatus{
		rf.voteFor = args.Requester
	}
	if args.CurrTerm > rf.currTerm{ // 如若是新一届,则需要更新心跳超时事件,否则啥也不做(无论我是否投票给他)。
		if rf.heartbeatTimer != nil && !rf.heartbeatTimer.Stop(){ // 如若关闭定时器失败,说明超时事件已经发生,那么直接返回false即可
			fmt.Println("第",rf.me,"台服务器作为追随者关闭定时器异常")
			reply.CurrTerm++ // 返回的任期要加一
			reply.ReplyStatus = false
			return
		}
		rf.toBeFollower(args.CurrTerm, rf.voteFor, NOLEADER)
	}
}

func (rf *Raft) asCandidateProcRequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	if args.CurrTerm < rf.currTerm {
		reply.ReplyStatus = false
		return
	}
	if args.CurrTerm == rf.currTerm {
		if rf.voteFor == NOVOTEFOR { // 当前状态一定要是已投票,否则不安全。
			log.Fatal(rf.me, "在 asCandidateProcRequestVote 中 !rf.hasVote 出错。")
		}
		reply.ReplyStatus = false
		return
	}
	// 如若对方任期数大于我,则身份转换为追随者
	if rf.voteTimer != nil && !rf.voteTimer.Stop(){
		fmt.Println("第",rf.me,"台服务器作为候选者关闭定时器异常")
		reply.ReplyStatus = false
		return
	}
	// 选举限制
	reply.ReplyStatus = args.CommitIndex > rf.commitIndex ||
		(args.CommitIndex == rf.commitIndex && (args.LastLogIndex >= rf.lastLogIndex && args.LastLogTerm >= rf.lastLogTerm))
	if reply.ReplyStatus {
		rf.voteFor = args.Requester
	}
	rf.toBeFollower(args.CurrTerm, rf.voteFor, NOLEADER)
}

func (rf *Raft) asLeaderProcRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	if args.CurrTerm <= rf.currTerm {
		reply.ReplyStatus = false
		return
	}
	// 选举限制
	reply.ReplyStatus = args.CommitIndex > rf.commitIndex ||
		(args.CommitIndex == rf.commitIndex && (args.LastLogIndex >= rf.lastLogIndex && args.LastLogTerm >= rf.lastLogTerm))
	if reply.ReplyStatus {
		rf.voteFor = args.Requester
	}
	rf.toBeFollower(args.CurrTerm, rf.voteFor, NOLEADER)
}

// return currentTerm and whether this server
// believes it is the leader.
// 返回 本届任期 和 该服务器是否认为自己是leader。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currTerm, rf.currLeader == rf.me
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.logBuff)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm int					/* 当前选举任期数,需要持久化 */
	var voteFor	int						/* 在当前选举任期是否已经投过了票,投给自己也算,需要持久化 */
	var commitIndex int					/* 当前节点已知的,最大的,已提交的日志索引 */
	var lastApplied int 				/* 表示当前节点最后一条被应用到状态机的日志索引 */
	var logBuff []LogEntries			/* 日志缓存,内含日志条目与日志产生的任期 */
	if d.Decode(&currTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil || d.Decode(&logBuff) != nil {
		log.Fatal("readPersist failed")
	}else {
		rf.currTerm = currTerm
		rf.voteFor = voteFor
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
		rf.logBuff = logBuff
		rf.lastLogIndex = len(logBuff) - 1
		rf.lastLogTerm = rf.logBuff[rf.lastLogIndex].Term
	}
}


type HeartbeatArgs struct{
	Sender 		int
	CurrTerm 	int
	PrevIndex 	int 		// 上一次的索引
	PrevTerm 	int 		// 上一次的任期
	HaveEnt		bool 		// 当前是否携带日志条目
	Entries 	LogEntries  // 日志条目
	CommitIndex int 		// 当前提交的索引
}

type HeartbeatReply struct{
	ReplyStatus bool 	// 答复状态
	Replyer 	int		// 答复者
	CurrTerm 	int 	// 答复者的当前任期
	LastIndex 	int		// 当前匹配的索引
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// RequestVote RPC参数结构示例。
// 字段名称必须以大写字母开头!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Requester 	int // 请求者
	CurrTerm 	int // 当前选举的届数
	LastLogIndex int // 下一条日志的下标
	LastLogTerm int // 上一条日志的任期
	CommitIndex int // 提交的索引
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	ReplyStatus bool // 答复状态
	Replyer int// 答复者
	CurrTerm int // 答复者的当前任期
}

/*
对外提供的服务 : 接收心跳包

*/
func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	if rf.dead != 0 { // 虽然状态是死亡,但是还没真正地被销毁。
		reply.CurrTerm = rf.currTerm
		reply.ReplyStatus = false
		reply.Replyer = rf.me
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	CurrTerm := rf.currTerm
	raftStatus := rf.raftStatus
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
	rf.HeartbeatLog(CurrTerm, raftStatus, args, reply)
}


//
// example RequestVote RPC handler.
//
// 接收投票消息,并返回结果。
func (rf *Raft) RequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	CurrTerm := rf.currTerm
	raftStatus := rf.raftStatus
	if rf.dead != 0 { // 虽然状态是死亡,但是还没真正地被销毁。
		reply.CurrTerm = rf.currTerm
		reply.ReplyStatus = false
		reply.Replyer = rf.me
		return
	}
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
	rf.RequestVoteLog(CurrTerm, raftStatus, args, reply)
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
	rf.sendRequestVoteLog(server, args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, args *HeartbeatArgs, reply *HeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.Heartbeat", args, reply)
	rf.sendHeartbeatLog(server, args, reply)
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
使用raft的服务(例如k/v服务器)想要启动
下一条命令要附加到raft的日志上。
如果这服务器不是leader，返回false。
否则启动同意并立即返回。
并不能保证指挥将永远被委身于raft上，
甚至这个领导者可能在选举中失败或失败。
就算raft实例被杀死了
这个函数应该优雅地返回。

第一个返回值是命令在提交时出现的索引(这次提交的索引)。
第二个返回值是本届任期。
如果该服务器认为自己是leader，则第三个返回值为true。
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	isSucceed := rf.me == rf.currLeader
	if isSucceed {
		le := LogEntries{
			Command: command,
			Term : rf.currTerm,
		}
		rf.logBuff = append(rf.logBuff, le)
		rf.logPersistRecord = append(rf.logPersistRecord, 1)
		rf.lastLogIndex++
	}
	rf.StartLog(command, isSucceed)
	return rf.lastLogIndex, rf.currTerm, isSucceed
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
	//atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	if rf.raftStatus == RaftFollower{
		if rf.heartbeatTimer != nil{
			if rf.heartbeatTimer.Stop() { // 尝试去关闭定时器,如若成功,那么表示超时事件未发生。
				rf.heartbeatTimer = nil
			}
		}
	}
	if rf.raftStatus == RaftCandidate{
		if rf.voteTimer != nil{
			if rf.voteTimer.Stop(){
				rf.voteTimer = nil
			}
		}
	}
	rf.raftStatus = RaftDead
	rf.cond.Broadcast()
	rf.mu.Unlock()
	rf.dead = 1
	rf.killLog()
}

func (rf *Raft) killed() bool {
	//z := atomic.LoadInt32(&rf.dead)
	return rf.dead == 1
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
	if peers == nil || len(peers) < 1 || me < 0 || me > len(peers) - 1 || persister == nil || applyCh == nil {
		return nil
	}
	rf := &Raft{
		peers : peers,
		persister : persister,
		applyCh : applyCh,
		me : me,
		dead : 0,
		raftStatus : RaftFollower,
		currTerm : 1, /* 任期初始化为1 */
		voteFor : NOVOTEFOR,
		acquiredVote : 0,
		currLeader : NOLEADER,
		heartbeatTimer : nil,
		voteTimer : nil,
		commitIndex : 0,
		lastApplied : 0,
		lastLogIndex : 0, /* 日志索引从1开始,所以初始化为0 */
		lastLogTerm : 0,
		hasVoteResult : make([]bool, len(peers)),
		nextIndex : make([]int, len(peers)),
	}
	le := LogEntries{
		Command: 0,
		Term : 0,
	}
	rf.logBuff = append(rf.logBuff, le)
	rf.logPersistRecord = append(rf.logPersistRecord, len(rf.peers))
	rf.cond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
