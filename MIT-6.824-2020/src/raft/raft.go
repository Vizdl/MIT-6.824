package raft

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// raft 节点状态
type ERaftStatus int32
const (
	RaftFollower ERaftStatus = iota
	RaftCandidate
	RaftLeader
	RaftDead
)

const NOLEADER = -1
const NOVOTEFOR = -1
// 心跳超时基数
const MINHEARTBEATTIMEOUT int64 = 200000000
// 心跳超时随机数
const HEARTBEATTIMEOUTSECTIONSIZE int64 = 200000000 // 如若为负数会报错
// 投票超时基数
const MINVOTETIMEOUT int64 = 150000000
// 投票超时随机数
const VOTETIMEOUTTIMEOUTSECTIONSIZE int64 = 200000000 // 如若为负数会报错
// 发送心跳请求的频率
const HEARTBEATTIMEOUT int64 = 50000000	// 如若太慢了会导致日志同步过慢
// 一次性传输日志的最大数
const ONEMAXLOGCOUNT int = 1

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

type Raft struct {
	/*** 通用数据 ***/
	// 状态
	mu        sync.Mutex          	// 状态锁
	raftStatus ERaftStatus			// 当前 raft 节点的状态
	currTerm int					// 当前选举任期数,需要持久化
	// raft rpc
	peers     []*labrpc.ClientEnd 	// RPC所有对等点的端点,依赖该属性进行rpc通信。
	me        int                 	// 当前节点编号
	// 状态持久化
	persister *Persister          	// 状态持久化对象
	// 日志持久化
	applyCh   chan ApplyMsg		  	// 日志持久化对象
	// 日志
	logBuff []LogEntries			// 日志缓存,内含日志条目与日志产生的任期
	lastLogIndex int 				// 上一条日志的索引
	lastLogTerm int 				// 上一条日志的任期
	commitIndex int					// 当前节点已知的,最大的,已提交的日志索引
	lastApplied int 				// 表示当前节点最后一条被应用到状态机的日志索引
	/*** 追随者和候选者有效 ***/
	voteFor	int						// 在当前选举任期是否已经投过了票,投给自己也算
	/*** 追随者有效 ***/
	currLeader int					// 当前届领导者
	heartbeatTimer *time.Timer 		// 心跳定时器
	/*** 候选者有效 ***/
	acquiredVote uint				// 在当前选举周期获得的票数
	voteTimer *time.Timer 			// 选举定时器
	/*** 领导者有效 ***/
	nextIndex []int					// nextIndex[i] : ID 为 i 的 raft 下一条日志的索引的猜测值,用以同步日志
	logPersistRecord []int			// logPersistRecord[i] : 第 i 条日志条目被 logPersistRecord[i] 台 raft 持久化,领导者用来统计当前任期内日志是否应该被提交
}

// 转换为追随者
func (rf *Raft) toBeFollower (currTerm int, voteFor int, currLeader int){
	// 初始化数据
	rf.raftStatus = RaftFollower
	rf.currLeader = currLeader
	rf.currTerm = currTerm
	rf.voteFor = voteFor
	// 持久化
	rf.persist()
	// 开启心跳定时器
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
}

// 转换为候选者
func (rf *Raft) toBeCandidate(){
	rf.currTerm++
	rf.acquiredVote = 1 // 自己投自己一票
	rf.voteFor = rf.me
	rf.currLeader = NOLEADER
	rf.raftStatus = RaftCandidate
	limit := time.Duration(MINVOTETIMEOUT + rand.Int63n(VOTETIMEOUTTIMEOUTSECTIONSIZE))
	rf.voteTimer = time.AfterFunc(limit, rf.voteTimeoutEventProc) // 开启选举超时
	// 开启协程,给其他 raft 节点发送投票请求
	for i := 0; i < len(rf.peers); i++{
		if rf.me != i {
			go rf.toSendRequestVote(rf.currTerm, i)
		}
	}
}

// 转换为领导者
func (rf *Raft) toBeLeader(){
	rf.voteSucceedLog()
	rf.currLeader = rf.me
	rf.raftStatus = RaftLeader
	// 日志持久化记录 初始化
	for i := 0; i <= rf.lastLogIndex; i++{
		rf.logPersistRecord = append(rf.logPersistRecord, 1)
	}
	// 所有节点默认追加日志都从当前节点日志开始
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = rf.lastLogIndex + 1
		}
	}
	// 开启协程,给其他 raft 节点发送心跳
	for i := 0; i < len(rf.peers); i++ {
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
	// 1. 状态检查
	if rf.raftStatus != RaftCandidate{
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"届发生选举超时, raftStatus =",rf.raftStatus,"错误的raft状态")
	}
	// 2. 状态转换
	rf.toBeCandidate()
	rf.voteTimeoutEventProcLog()
}

/*
心跳超时处理函数 : 只有在当前处于追随者状态下会被调用
raft变为候选人,获得的票数清零,选举周期加一,获得的票数加一,向其他服务器发起投票请求。
*/
func (rf *Raft) heartTimeoutEventProc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. 状态检查
	if rf.raftStatus != RaftFollower {
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"届发生心跳超时, raftStatus =",rf.raftStatus,"错误的raft状态")
	}
	// 2. 状态转换
	rf.toBeCandidate()
	rf.heartTimeoutEventProcLog()
}

//
// 获得成为领导者的资格
//
func (rf *Raft) qualifyLeader() bool {
	return rf.acquiredVote >= uint(len(rf.peers)) / 2 + 1
}

func (rf *Raft) toSendRequestVote(CurrTerm int, raftId int){
	args := RequestVoteArgs{
		Requester : rf.me,
		CurrTerm : CurrTerm,
		LastLogTerm : rf.lastLogTerm,
		LastLogIndex: rf.lastLogIndex,
		CommitIndex: rf.commitIndex,
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.raftStatus == RaftCandidate && rf.currTerm == CurrTerm {
		rf.mu.Unlock()
		reply := RequestVoteReply{}
		isCalled := rf.sendRequestVote(raftId, &args, &reply)
		rf.mu.Lock()
		// 如若状态发生改变,则退出。
		if rf.raftStatus != RaftCandidate || rf.currTerm != CurrTerm {
			break
		}
		// 对 rpc 调用进行检查
		if !isCalled {
			continue
		}
		// 检查回复 id
		if reply.Replyer != raftId {
			log.Fatal("向",raftId,"发送投票请求,但是收到回复id却是 :", reply.Replyer)
		}
		// 如若对方任期小于自己则重试
		if reply.CurrTerm < rf.currTerm {
			continue
		}
		// 如若对方任期大于等于自己,则按照对方的应答判断是否获得票
		if reply.ReplyStatus {
			rf.acquiredVote++
			if rf.qualifyLeader() {
				if rf.voteTimer != nil && rf.voteTimer.Stop() {
					rf.toBeLeader()
				}
			}
		}
		// 只要有结果了,就退出循环
		break
	}
}

func (rf *Raft) toSendHeartbeat(CurrTerm int, raftId int){
	lastTick := time.Now().UnixNano() - HEARTBEATTIMEOUT // 确保刚进入就一定能发出心跳
	/* 按照逻辑匹配成功后就不会失败 */
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
				CommitIndex: rf.commitIndex,
			}
			if rf.nextIndex[raftId] <= rf.lastLogIndex && isMatch {
				end := len(rf.logBuff)
				if len(rf.logBuff) >= rf.nextIndex[raftId] + ONEMAXLOGCOUNT {
					end = rf.nextIndex[raftId] + ONEMAXLOGCOUNT
				}
				args.Entries = rf.logBuff[rf.nextIndex[raftId]:end]
			}
			rf.mu.Unlock()
			reply := HeartbeatReply{}
			isCalled := rf.sendHeartbeat(raftId, &args, &reply)
			rf.mu.Lock()
			// 检查当前节点状态是否正确
			if rf.raftStatus != RaftLeader || rf.currTerm != CurrTerm {
				break
			}
			// 如若调用失败
			if !isCalled {
				continue
			}
			// 对返回的结果进行检验
			if reply.Replyer != raftId {
				continue
			}
			// 对任期进行处理
			// 如若发现存在任期大于自己的,立马更新任期成为追随者
			if reply.CurrTerm > rf.currTerm {
				rf.toBeFollower(reply.CurrTerm, NOVOTEFOR, NOLEADER)
				break
			}
			// 如若匹配失败
			if !reply.ReplyStatus {
				isMatch = false
				rf.nextIndex[raftId] = reply.LastIndex + 1
			} else {
				isMatch = true
				/* 如若发送了日志 */
				if len(args.Entries) > 0 {
					/* 更新 logPersistRecord 和 commitIndex */
					for i := 0; i < len(args.Entries); i++ {
						if args.Entries[i].Term == rf.currTerm {
							rf.logPersistRecord[rf.nextIndex[raftId] + i]++
							if rf.logPersistRecord[rf.nextIndex[raftId] + i] >= len(rf.peers) / 2 + 1{
								if rf.commitIndex < rf.nextIndex[raftId] + i{
									rf.commitIndex = rf.nextIndex[raftId]
								}
							}
						}
					}
					flag := false
					/* 将已提交但未应用的日志应用到 k/v 服务器 */
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
					rf.nextIndex[raftId] += len(args.Entries)
				}else {
					for i := 1; i <= args.PrevIndex; i++{
						rf.logPersistRecord[i]++
					}
				}
			}
		}
		/* 如若状态没发生改变,根据定时器,来定时堵塞。 */
		rf.mu.Unlock()
		if tick - lastTick > 10000000 {
			time.Sleep(time.Duration(10 * time.Millisecond))
		}else {
			time.Sleep(time.Duration(time.Millisecond))
		}
		rf.mu.Lock()
	}
}

func (rf *Raft) asFollowerProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	// 请求参数校验
	if rf.currLeader != NOLEADER && args.CurrTerm == rf.currTerm && args.Sender != rf.currLeader {
		log.Fatal("第 ",rf.me," 台服务器在第 ",rf.currTerm," 届收到心跳包,但领导应该是 ",rf.currLeader," 却收到 ",args.Sender," 发送的心跳包")
		return
	}
	// 默认回复
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false

	if args.CurrTerm < rf.currTerm {
		return
	}
	// 关闭心跳,如若心跳事件已经发生,拒绝所有当前心跳包,等待状态转换。
	if rf.heartbeatTimer == nil || !rf.heartbeatTimer.Stop() {
		fmt.Println("第 ",rf.me," 台服务器作为追随者关闭定时器异常,表示心跳超时已经发生了")
		return
	}
	// args.PrevIndex <= rf.lastLogIndex 是考虑到 对方比我方日志更少
	reply.ReplyStatus = args.PrevIndex <= rf.lastLogIndex && args.PrevTerm ==  rf.logBuff[args.PrevIndex].Term
	// 如若找到最后一条相同日志
	if reply.ReplyStatus {
		// 删除无用日志
		if args.PrevIndex < rf.lastLogIndex {
			rf.logBuff = rf.logBuff[:args.PrevIndex + 1]
			rf.lastLogIndex = args.PrevIndex
			rf.lastLogTerm = rf.logBuff[rf.lastLogIndex].Term
		}
		// 追加日志
		if len(args.Entries) > 0 {
			rf.logBuff = append(rf.logBuff, args.Entries...)
			rf.lastLogIndex += len(args.Entries)
			rf.lastLogTerm = args.Entries[len(args.Entries) - 1].Term
		}
		// 更新日志提交索引
		if args.CommitIndex > rf.commitIndex {
			// 需要考虑匹配上的日志数量少于提交数量的情况
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
	// 向k/v服务器发送已提交未应用的消息
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
	// 发生变动了,持久化
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
	if len(args.Entries) > 0 {
		log.Fatal("第一次心跳就发送了日志,不正常的状态。")
	}
	rf.toBeFollower(args.CurrTerm, args.Sender, args.Sender)
}

func (rf *Raft) asLeaderProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	if args.CurrTerm == rf.currTerm {
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"领导应该是自己却收到",args.Sender,"发送的心跳包")
	}
	// 默认回复
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false
	if args.CurrTerm < rf.currTerm {
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
	if len(args.Entries) > 0 {
		log.Fatal("第一次心跳就发送了日志,不正常的状态。")
	}
	rf.toBeFollower(args.CurrTerm, args.Sender, args.Sender)
}

func (rf *Raft) asFollowerProcRequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	// 默认答复
	reply.Replyer = rf.me
	reply.ReplyStatus = false
	reply.CurrTerm = rf.currTerm
	// 根据任期和是否投票决定是否需要拒绝
	if args.CurrTerm < rf.currTerm {
		return
	}
	if args.CurrTerm == rf.currTerm && rf.voteFor != NOVOTEFOR {
		return
	}
	if args.CurrTerm > rf.currTerm {
		// 如若关闭定时器失败,说明超时事件已经发生,那么直接拒绝
		if rf.heartbeatTimer != nil && !rf.heartbeatTimer.Stop() {
			fmt.Println("第",rf.me,"台服务器作为追随者关闭定时器异常")
			return
		}
		// 更新任期为最新
		rf.toBeFollower(args.CurrTerm, NOVOTEFOR, NOLEADER)
	}
	reply.CurrTerm = rf.currTerm
	// 选举限制
	if args.CommitIndex > rf.commitIndex || (args.CommitIndex == rf.commitIndex && (args.LastLogIndex >= rf.lastLogIndex && args.LastLogTerm >= rf.lastLogTerm)) {
		reply.ReplyStatus = true
		rf.voteFor = args.Requester
	}
}

func (rf *Raft) asCandidateProcRequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	// 默认回复
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false
	if args.CurrTerm < rf.currTerm {
		return
	}
	if args.CurrTerm == rf.currTerm {
		// 当前状态一定要是已投票,否则不安全。
		if rf.voteFor == NOVOTEFOR {
			log.Fatal(rf.me, "在 asCandidateProcRequestVote 中 !rf.hasVote 出错。")
		}
		return
	}
	// 对方任期数大于我
	// 如若已经候选定时器超时,则返回 false,等待超时
	if rf.voteTimer != nil && !rf.voteTimer.Stop(){
		fmt.Println("第",rf.me,"台服务器作为候选者关闭定时器异常")
		return
	}
	// 身份转换为追随者, 回复 false
	rf.toBeFollower(args.CurrTerm, NOVOTEFOR, NOLEADER)
	return
}

func (rf *Raft) asLeaderProcRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false
	if args.CurrTerm <= rf.currTerm {
		return
	}
	// 如若对方任期比自己高,则转换为追随者,但这次回复 false
	rf.toBeFollower(args.CurrTerm, NOVOTEFOR, NOLEADER)
	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currTerm, rf.currLeader == rf.me
}

//
// raft 持久化
//
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
// 从持久化数据里恢复状态
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	Sender 		int				// 发送者
	CurrTerm 	int				// 当前任期
	PrevIndex 	int 			// 上一次的索引
	PrevTerm 	int 			// 上一次的任期
	Entries 	[]LogEntries  	// 日志条目
	CommitIndex int 			// 当前提交的索引
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
// 投票请求答复
//
type RequestVoteReply struct {
	ReplyStatus bool 	// 答复状态
	Replyer int			// 答复者
	CurrTerm int 		// 答复者的当前任期
}

//
// 对外提供的服务 : 接收心跳包
//
func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
	rf.mu.Lock()
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
	case RaftDead :
		break
	default:
		log.Fatal(rf.me,"当前处于未注册的状态中 : rf.raftStatus = ",rf.raftStatus)
	}
	rf.mu.Unlock()
	rf.HeartbeatLog(CurrTerm, raftStatus, args, reply)
}

// 接收投票消息,并返回结果。
func (rf *Raft) RequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	CurrTerm := rf.currTerm
	raftStatus := rf.raftStatus
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
	case RaftDead :
		break
	default:
		log.Fatal(rf.me,"当前处于未注册的状态中 : rf.raftStatus = ",rf.raftStatus)
	}
	rf.RequestVoteLog(CurrTerm, raftStatus, args, reply)
}

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

func (rf *Raft) Kill() {
	rf.mu.Lock()
	if rf.raftStatus == RaftFollower {
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
	rf.mu.Unlock()
	rf.killLog()
}


/*
函数功能 : 提供给 k/v server 的服务,用来获取当前Raft状态
*/
func (rf *Raft) RaftStatus() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currLeader
}

func (rf *Raft) killed() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.raftStatus == RaftDead
}

/*
服务或测试人员希望创建一个Raft服务器。
所有Raft服务器(包括这个服务器)的端口都在peer[]中。
此服务器的端口是peer[me]。所有服务器的对等点[]数组具有相同的顺序。
persister是此服务器保存其持久状态的地方，最初还保存最近保存的状态(如果有的话)。
applyCh是测试者或服务期望筏发送ApplyMsg消息的通道，
Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutines。
*/
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// 1. 参数检测
	if peers == nil || len(peers) < 1 || me < 0 || me > len(peers) - 1 || persister == nil || applyCh == nil {
		return nil
	}
	// 2. 初始化 raft 节点
	rf := &Raft{
		peers : peers,
		persister : persister,
		applyCh : applyCh,
		me : me,
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
		nextIndex : make([]int, len(peers)),
	}
	le := LogEntries{
		Command: 0,
		Term : 0,
	}
	rf.logBuff = append(rf.logBuff, le)
	rf.logPersistRecord = append(rf.logPersistRecord, len(rf.peers))

	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
	// 恢复数据
	rf.readPersist(persister.ReadRaftState())
	return rf
}
