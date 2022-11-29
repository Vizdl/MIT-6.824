package raft

type LogMonitor struct {
	peers int
	nextIndex []int					// nextIndex[i] : ID 为 i 的 raft 下一条日志的索引的猜测值,用以同步日志
	logPersistRecord []int			// logPersistRecord[i] : 第 i 条日志条目被 logPersistRecord[i] 台 raft 持久化,领导者用来统计当前任期内日志是否应该被提交
}

func (lm *LogMonitor) init (peers int, logIndex int) {
	lm.peers = peers
	lm.nextIndex = make([]int, peers)
	lm.logPersistRecord = append(lm.logPersistRecord, peers)
	for i := 0; i <= logIndex; i++{
		lm.logPersistRecord = append(lm.logPersistRecord, 1)
	}
	// 所有节点默认追加日志都从当前节点日志开始
	for i := 0; i < lm.peers; i++ {
		lm.nextIndex[i] = logIndex + 1
	}
}

func (lm *LogMonitor) getNextIndex (raftID int) int {
	return lm.nextIndex[raftID]
}

func (lm *LogMonitor) setNextIndex (raftID int, nextIndex int) {
	lm.nextIndex[raftID] = nextIndex
}

func (lm *LogMonitor) logPersistRecordInc (index int) {
	lm.logPersistRecord[index]++
}

func (lm *LogMonitor) logPersistRecordTo (index int) {
	for i := 1; i <= index; i++{
		lm.logPersistRecord[i]++
	}
}

func (lm *LogMonitor) logPersistRecordAppend () {
	lm.logPersistRecord = append(lm.logPersistRecord, 1)
}

func (lm *LogMonitor) logPersistRecordCanCommit (index int) bool {
	return lm.logPersistRecord[index] >= lm.peers / 2 + 1
}