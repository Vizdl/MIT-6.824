// +build !raft_debug

package raft

func (rf *Raft) voteTimeoutEventProcLog(){
}

func (rf *Raft) heartTimeoutEventProcLog() {
}


func (rf *Raft) sendRequestVoteLog(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
}

func (rf *Raft) sendHeartbeatLog(server int, args *HeartbeatArgs, reply *HeartbeatReply){
}

func (rf *Raft) killLog() {
}

func (rf *Raft) RequestVoteLog(CurrTerm int,raftStatus ERaftStatus, args *RequestVoteArgs, reply *RequestVoteReply){
}

func (rf *Raft) HeartbeatLog(CurrTerm int,raftStatus ERaftStatus, args *HeartbeatArgs, reply *HeartbeatReply){
}

func (rf *Raft) voteSucceedLog(){
}

func (rf *Raft) StartLog(command interface{}, isSucceed bool){
}

func (rf *Raft) commitLog(applyMsg ApplyMsg){
}