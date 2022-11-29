package raft

type LogSynchronizer struct {
	peers int
	nextIndex []int					// nextIndex[i] : ID 为 i 的 raft 下一条日志的索引的猜测值,用以同步日志
	logPersistRecord []int			// logPersistRecord[i] : 第 i 条日志条目被 logPersistRecord[i] 台 raft 持久化,领导者用来统计当前任期内日志是否应该被提交
}

func (ls *LogSynchronizer) init (peers int, logIndex int) {
	ls.peers = peers
	ls.nextIndex = make([]int, peers)
	ls.logPersistRecord = append(ls.logPersistRecord, peers)
	for i := 0; i <= logIndex; i++{
		ls.logPersistRecord = append(ls.logPersistRecord, 1)
	}
	// 所有节点默认追加日志都从当前节点日志开始
	for i := 0; i < ls.peers; i++ {
		ls.nextIndex[i] = logIndex + 1
	}
}

func (ls *LogSynchronizer) getNextIndex (raftID int) int {
	return ls.nextIndex[raftID]
}

func (ls *LogSynchronizer) setNextIndex (raftID int, nextIndex int) {
	ls.nextIndex[raftID] = nextIndex
}

func (ls *LogSynchronizer) logPersistRecordInc (index int) {
	ls.logPersistRecord[index]++
}

func (ls *LogSynchronizer) logPersistRecordTo (index int) {
	for i := 1; i <= index; i++{
		ls.logPersistRecord[i]++
	}
}

func (ls *LogSynchronizer) logPersistRecordAppend () {
	ls.logPersistRecord = append(ls.logPersistRecord, 1)
}

func (ls *LogSynchronizer) logPersistRecordCanCommit (index int) bool {
	return ls.logPersistRecord[index] >= ls.peers / 2 + 1
}