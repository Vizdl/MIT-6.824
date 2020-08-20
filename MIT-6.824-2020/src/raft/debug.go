// +build debug

package raft

import (
	"fmt"
)


func (rf *Raft) voteTimeoutEventProcLog(){
	fmt.Println("第",rf.me,"台服务器,第",rf.currTerm,"届发生选举超时")
}

func (rf *Raft) heartTimeoutEventProcLog() {
	fmt.Println("第",rf.me,"台服务器在第 ",rf.currTerm," 届发生心跳超时")
}


func (rf *Raft) sendRequestVoteLog(i int, args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Printf("第 %d 台服务器在第 %d 届向第 %d 台服务器发起投票请求,请求前有 %d 票,收到回复为 : %v\n",rf.me,args.CurrTerm,i,rf.acquiredVote,reply)
}

func (rf *Raft) sendHeartbeatLog(i int, args *HeartbeatArgs, reply *HeartbeatReply){
	fmt.Printf("第 %d 台服务器作为领导者在第 %d 届向第 %d 台服务器发送心跳包,收到回复为 : %v\n",rf.me,args.CurrTerm,i,reply)
}

func (rf *Raft) killLog() {
	fmt.Printf("第 %d 台服务器在第 %d 届以状态 %d 的形式被杀死\n",rf.me,rf.currTerm,rf.raftStatus)
}


func (rf *Raft) killedLog() {
	fmt.Println("Killed")
}


func (rf *Raft) RequestVoteLog(CurrTerm uint32,raftStatus ERaftStatus, args *RequestVoteArgs, reply *RequestVoteReply){
	fmt.Printf("第 %d 台服务器在第 %d 届以状态 %d 的形式收到第 %d 台服务器的投票请求 : %v, 答复为 : %v\n",rf.me,CurrTerm,raftStatus,args.Requester,args,reply)
}

func (rf *Raft) HeartbeatLog(CurrTerm uint32,raftStatus ERaftStatus, args *HeartbeatArgs, reply *HeartbeatReply){
	fmt.Printf("第 %d 台服务器在第 %d 届以状态 %d 的形式收到心跳包 : %v, 答复为 : %v\n",rf.me,CurrTerm,raftStatus,args,reply)
}

func (rf *Raft) voteSucceedLog(){
	fmt.Println("第",rf.me,"台服务器获取大多数选票成为第",rf.currTerm,"届的领导者")
}