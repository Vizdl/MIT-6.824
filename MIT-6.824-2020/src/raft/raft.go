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
const FISTTERM = 1
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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// rpc 心跳请求参数
//
type HeartbeatArgs struct{
	Sender 		int				// 发送者
	CurrTerm 	int				// 当前任期
	PrevIndex 	int 			// 上一次的索引
	PrevTerm 	int 			// 上一次的任期
	Entries 	[]LogEntries  	// 日志条目
	CommitIndex int 			// 当前提交的索引
}

//
// rpc 心跳请求回复
//
type HeartbeatReply struct{
	ReplyStatus bool 		// 答复状态
	Replyer 	int			// 答复者
	CurrTerm 	int 		// 答复者的当前任期
	LastIndex 	int			// 当前匹配的索引
	RaftStatus 	ERaftStatus	// 答复者的状态
}

//
// rpc 投票请求参数
//
type RequestVoteArgs struct {
	Requester		int		// 请求者
	CurrTerm 		int		// 当前选举的届数
	LastLogIndex 	int		// 下一条日志的下标
	LastLogTerm 	int 	// 上一条日志的任期
	CommitIndex 	int 	// 提交的索引
}

//
// rpc 投票请求答复
//
type RequestVoteReply struct {
	ReplyStatus bool 	// 答复状态
	Replyer 	int		// 答复者
	CurrTerm 	int 	// 答复者的当前任期
}

type Raft struct {
	/*** 通用数据 ***/
	// 状态
	mu        sync.Mutex          	// 状态锁
	raftStatus ERaftStatus			// 当前 raft 节点的状态
	currTerm int					// 当前选举任期数,需要持久化
	currLeader int					// 当前届领导者,如若没有值为 NOLEADER
	voteFor	int						// 在当前选举任期票投给了谁
	// raft rpc
	peers     []*labrpc.ClientEnd 	// RPC所有对等点的端点,依赖该属性进行rpc通信。
	me        int                 	// 当前节点编号
	// 状态持久化
	persister *Persister          	// 状态持久化对象
	// 日志持久化
	applyCh   chan ApplyMsg		  	// 日志持久化对象
	// 日志
	logManager LogManager			// 日志管理单元
	/*** 追随者有效 ***/
	heartbeatTimer *time.Timer 		// 心跳定时器
	/*** 候选者有效 ***/
	acquiredVote uint				// 在当前选举周期获得的票数
	voteTimer *time.Timer 			// 选举定时器
	/*** 领导者有效 ***/
	logSynchronizer LogSynchronizer // 日志同步单元
}

func (rf *Raft) startHeartbeatTimer() {
	limit := time.Duration(MINHEARTBEATTIMEOUT + rand.Int63n(HEARTBEATTIMEOUTSECTIONSIZE))
	rf.heartbeatTimer = time.AfterFunc(limit, rf.heartTimeoutEventProc)
}

func (rf *Raft) stopHeartbeatTimer() bool {
	return rf.heartbeatTimer != nil && rf.heartbeatTimer.Stop()
}

func (rf *Raft) startVoteTimer() {
	limit := time.Duration(MINVOTETIMEOUT + rand.Int63n(VOTETIMEOUTTIMEOUTSECTIONSIZE))
	rf.voteTimer = time.AfterFunc(limit, rf.voteTimeoutEventProc) // 开启选举超时
}

func (rf *Raft) stopVoteTimer() bool {
	return rf.voteTimer != nil && rf.voteTimer.Stop()
}

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
// 获得成为领导者的资格
//
func (rf *Raft) willToBeLeader() bool {
	return rf.acquiredVote >= uint(len(rf.peers)) / 2 + 1
}

//
// 转换为追随者
//
func (rf *Raft) toBeFollower (currTerm int, voteFor int, currLeader int){
	// 初始化数据
	rf.raftStatus = RaftFollower
	rf.currLeader = currLeader
	rf.currTerm = currTerm
	rf.voteFor = voteFor
	// 持久化
	rf.persist()
	// 开启心跳定时器
	rf.startHeartbeatTimer()
}

//
// 转换为候选者
//
func (rf *Raft) toBeCandidate(){
	rf.currTerm++
	rf.acquiredVote = 1 // 自己投自己一票
	rf.voteFor = rf.me
	rf.currLeader = NOLEADER
	rf.raftStatus = RaftCandidate
	// 开启协程,给其他 raft 节点发送投票请求
	for i := 0; i < len(rf.peers); i++{
		if rf.me != i {
			go rf.toSendRequestVote(rf.currTerm, i)
		}
	}
	// 开启选举超时定时器
	rf.startVoteTimer()
}

//
// 转换为领导者
//
func (rf *Raft) toBeLeader(){
	rf.voteSucceedLog()
	rf.currLeader = rf.me
	rf.voteFor = rf.me
	rf.raftStatus = RaftLeader
	// 日志持久化记录 初始化
	rf.logSynchronizer.init(len(rf.peers), rf.logManager.getLastLogIndex())
	// 开启协程,给其他 raft 节点发送心跳
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.toSendHeartbeat(rf.currTerm, i)
		}
	}
}

//
// 选举超时处理函数 : 只有在当前处于候选者状态下会被调用
//
func (rf *Raft) voteTimeoutEventProc(){
	rf.mu.Lock() // 只要进入了这个函数,就必定是是超时。
	defer rf.mu.Unlock()
	// 1. 状态检查
	if rf.raftStatus == RaftDead {
		return
	}
	if rf.raftStatus != RaftCandidate{
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"届发生选举超时, raftStatus =",rf.raftStatus,"错误的raft状态")
	}
	// 2. 状态转换
	rf.toBeCandidate()
	rf.voteTimeoutEventProcLog()
}

//
// 心跳超时处理函数 : 只有在当前处于追随者状态下会被调用
//
func (rf *Raft) heartTimeoutEventProc() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 1. 状态检查
	if rf.raftStatus == RaftDead {
		return
	}
	if rf.raftStatus != RaftFollower {
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"届发生心跳超时, raftStatus =",rf.raftStatus,"错误的raft状态")
	}
	// 2. 状态转换
	rf.toBeCandidate()
	rf.heartTimeoutEventProcLog()
}

//
// 发送投票请求协程
//
func (rf *Raft) toSendRequestVote(CurrTerm int, raftId int){
	args := RequestVoteArgs{
		Requester : rf.me,
		CurrTerm : CurrTerm,
		LastLogTerm : rf.logManager.getLastLogTerm(),
		LastLogIndex: rf.logManager.getLastLogIndex(),
		CommitIndex: rf.logManager.getCommitIndex(),
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
			if rf.willToBeLeader() {
				if rf.stopVoteTimer() {
					rf.toBeLeader()
				}else {
					log.Println("即将成为领导者时关闭候选者定时器失败")
				}
			}
		}
		// 只要有结果了,就退出循环
		break
	}
}

//
// 发送心跳请求协程
//
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
			// 1. 初始化心跳参数
			// 未匹配上时, nextIndex 是猜测值。匹配上后, nextIndex 就是确定的值。
			nextIndex := rf.logSynchronizer.getNextIndex(raftId)
			args := HeartbeatArgs{
				Sender: rf.me,
				CurrTerm: rf.currTerm,
				PrevIndex: nextIndex - 1,
				PrevTerm: rf.logManager.getLogTerm(nextIndex - 1),
				CommitIndex: rf.logManager.getCommitIndex(),
			}
			// 2. 如若已经匹配,并且又有新的日志未同步
			if isMatch {
				args.Entries = rf.logManager.getNeedSyncLogEntries(nextIndex)
			}
			rf.mu.Unlock()
			reply := HeartbeatReply{}
			isCalled := rf.sendHeartbeat(raftId, &args, &reply)
			rf.mu.Lock()
			// 检查当前节点状态是否正确
			if rf.raftStatus != RaftLeader || rf.currTerm != CurrTerm {
				goto end
			}
			// 如若调用失败
			if !isCalled {
				continue
			}
			// 检查回复 raftid
			if reply.Replyer != raftId {
				log.Fatal("发送心跳时回复的 raftid 错误")
			}
			// 对任期进行处理
			// 如若发现存在任期大于自己的,立马更新任期成为追随者
			if reply.CurrTerm > rf.currTerm {
				rf.toBeFollower(reply.CurrTerm, NOVOTEFOR, NOLEADER)
				goto end
			}
			// 如若对方任期小于自己,等待对方更新任期后再发送新的心跳
			if reply.CurrTerm < rf.currTerm {
				break
			}
			// 如若不是追随者状态,等待对方更新状态后再发送新的心跳
			if reply.RaftStatus != RaftFollower {
				break
			}
			// 处理符合标准的回复
			if !reply.ReplyStatus {
				rf.logSynchronizer.setNextIndex(raftId, reply.LastIndex + 1)
			} else {
				// 如若第一次匹配到 : 没有发送日志
				if !isMatch {
					rf.logSynchronizer.logPersistRecordTo(args.PrevIndex)
					isMatch = true
				}else {
					nextIndex = rf.logSynchronizer.getNextIndex(raftId)
					/* 更新 logPersistRecord 和 commitIndex */
					for i := 0; i < len(args.Entries); i++ {
						if args.Entries[i].Term == rf.currTerm {
							rf.logSynchronizer.logPersistRecordInc(nextIndex + i)
							// 如若超出一半拥有该日志
							if rf.logSynchronizer.logPersistRecordCanCommit(nextIndex + i) {
								if rf.logManager.getCommitIndex() < nextIndex + i{
									rf.logManager.setCommitIndex(nextIndex)
								}
							}
						}
					}
					rf.logManager.submitCommitLog(rf.applyCh)
					rf.logSynchronizer.setNextIndex(raftId, nextIndex + len(args.Entries))
					rf.persist()
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
end:
	return
}

func (rf *Raft) asFollowerProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	// 如若同一任期出现两个不同的 Leader
	if rf.currLeader != NOLEADER && args.CurrTerm == rf.currTerm && args.Sender != rf.currLeader {
		log.Fatal("第 ",rf.me," 台服务器在第 ",rf.currTerm," 届收到心跳包,但领导应该是 ",rf.currLeader," 却收到 ",args.Sender," 发送的心跳包")
		return
	}
	// 默认回复
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.RaftStatus = rf.raftStatus
	reply.ReplyStatus = false

	if args.CurrTerm < rf.currTerm {
		return
	}
	if !rf.stopHeartbeatTimer() {
		fmt.Println("第 ",rf.me," 台服务器作为追随者关闭定时器异常,表示心跳超时已经发生了")
		return
	}
	// 如若对方任期大于自己,则需要等心跳计时器关闭后转换状态
	if args.CurrTerm > rf.currTerm {
		rf.toBeFollower(args.CurrTerm, args.Sender, args.Sender)
		return
	}
	// 如若是作为追随者第一次收到当前任期 Leader 的心跳
	if rf.currLeader == NOLEADER {
		rf.currLeader = args.Sender
	}
	reply.ReplyStatus, reply.LastIndex = rf.logManager.logSyncPorc(args.CommitIndex, args.PrevIndex, args.PrevTerm, args.Entries)
	rf.logManager.submitCommitLog(rf.applyCh)
	rf.persist()
	rf.startHeartbeatTimer()
}

func (rf *Raft) asCandidateProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.RaftStatus = rf.raftStatus
	reply.ReplyStatus = false
	if args.CurrTerm < rf.currTerm {
		return
	}
	if !rf.stopVoteTimer() {
		fmt.Println("第",rf.me,"台服务器作为候选者关闭定时器异常")
		return
	}
	rf.toBeFollower(args.CurrTerm, args.Sender, args.Sender)
}

func (rf *Raft) asLeaderProcHeartbeat (args *HeartbeatArgs, reply *HeartbeatReply) {
	// 如若同一任期出现两个不同的 Leader
	if args.CurrTerm == rf.currTerm {
		log.Fatal("第",rf.me,"台服务器在第",rf.currTerm,"领导应该是自己却收到",args.Sender,"发送的心跳包")
	}
	// 默认回复
	reply.Replyer = rf.me
	reply.CurrTerm = rf.currTerm
	reply.ReplyStatus = false
	reply.RaftStatus = rf.raftStatus
	if args.CurrTerm < rf.currTerm {
		return
	}
	// 如若作为领导者收到更高任期的心跳,则转换状态。
	rf.toBeFollower(args.CurrTerm, NOVOTEFOR, args.Sender)
}

func (rf *Raft) asFollowerProcRequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
	// 默认答复
	reply.Replyer = rf.me
	reply.ReplyStatus = false
	reply.CurrTerm = rf.currTerm
	if args.CurrTerm < rf.currTerm {
		return
	}
	if args.CurrTerm > rf.currTerm {
		if !rf.stopHeartbeatTimer() {
			fmt.Println("第",rf.me,"台服务器作为追随者关闭定时器异常")
			return
		}
		// 更新任期为最新
		rf.toBeFollower(args.CurrTerm, NOVOTEFOR, NOLEADER)
		return
	}
	if rf.voteFor != NOVOTEFOR {
		return
	}
	// 选举限制
	if rf.logManager.logLimit(args.CommitIndex, args.LastLogIndex, args.LastLogTerm) {
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
		// 当前状态一定要是已投票
		if rf.voteFor == NOVOTEFOR {
			log.Fatal(rf.me, "在 asCandidateProcRequestVote 中 !rf.hasVote 出错。")
		}
		return
	}
	// 对方任期数大于我
	if !rf.stopVoteTimer() {
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

//
// 提交日志
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isSucceed := rf.me == rf.currLeader
	if isSucceed {
		rf.logManager.logAppend(command, rf.currTerm)
		rf.logSynchronizer.logPersistRecordAppend()
	}
	rf.StartLog(command, isSucceed)
	return rf.logManager.getLastLogIndex(), rf.currTerm, isSucceed
}

//
// 杀死 raft 节点
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 这里关闭定时器失败后,不做任何处理,在定时器处理函数里处理。
	switch rf.raftStatus {
	case RaftFollower :
		rf.stopHeartbeatTimer()
		break
	case RaftCandidate :
		rf.stopVoteTimer()
		break
	case RaftLeader :
		break
	case RaftDead :
		log.Fatal("多次 kill 同一个 raft 节点")
	default:
		log.Fatal(rf.me,"当前处于未注册的状态中 : rf.raftStatus = ",rf.raftStatus)
	}
	rf.raftStatus = RaftDead
	rf.killLog()
}

//
// 判断 raft 节点是否被杀死
//
func (rf *Raft) killed() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.raftStatus == RaftDead
}

//
// 函数功能 : 提供给 k/v server 的服务,用来获取当前Raft状态
//
func (rf *Raft) RaftStatus() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currLeader
}

//
// 获取当前任期,以及判断当前节点是否是 Leader
//
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
	e.Encode(rf.logManager.getCommitIndex())
	e.Encode(rf.logManager.getAppliedIndex())
	e.Encode(rf.logManager.getLogBuff())
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
		rf.logManager.decode(commitIndex, lastApplied, logBuff)
	}
}

//
// rpc 处理函数 : 心跳包处理
//
func (rf *Raft) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) {
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
	case RaftDead :
		break
	default:
		log.Fatal(rf.me,"当前处于未注册的状态中 : rf.raftStatus = ",rf.raftStatus)
	}
	rf.HeartbeatLog(CurrTerm, raftStatus, args, reply)
}

//
// rpc 处理函数 : 接收投票消息,并返回结果。
//
func (rf *Raft) RequestVote (args *RequestVoteArgs, reply *RequestVoteReply) {
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

//
// 创建 raft 节点
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	// 1. 参数检测
	if peers == nil || len(peers) < 1 || me < 0 || me > len(peers) - 1 || persister == nil || applyCh == nil {
		log.Fatal("启动 raft 参数错误")
	}
	// 2. 初始化 raft 节点
	rf := &Raft{
		peers : peers,
		persister : persister,
		applyCh :        applyCh,
		me :             me,
		raftStatus :     RaftFollower,
		currTerm :       FISTTERM,
		voteFor :        NOVOTEFOR,
		acquiredVote :   0,
		currLeader :     NOLEADER,
		heartbeatTimer : nil,
		voteTimer :      nil,
	}
	rf.logManager.init()
	// 恢复数据
	rf.readPersist(persister.ReadRaftState())
	rf.startHeartbeatTimer()
	return rf
}
