// +build !debug

package raft

import (
	// "fmt"
)


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


func (rf *Raft) killedLog() {
}


func (rf *Raft) RequestVoteLog(CurrTerm uint32,raftStatus ERaftStatus, args *RequestVoteArgs, reply *RequestVoteReply){
}

func (rf *Raft) HeartbeatLog(CurrTerm uint32,raftStatus ERaftStatus, args *HeartbeatArgs, reply *HeartbeatReply){
}

func (rf *Raft) voteSucceedLog(){
}