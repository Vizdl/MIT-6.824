package raft

type LogEntries struct {
	Term 	int				// 日志产生的任期
	Command interface{}		// 日志消息
}

type LogManager struct {
	logBuff      []LogEntries 		// 日志缓存,内含日志条目与日志产生的任期
	commitIndex  int          		// 已提交的日志索引
	appliedIndex int          		// 已应用的日志索引
}

func (lm *LogManager) init () {
	lm.commitIndex = 0
	lm.appliedIndex = 0
	le := LogEntries{
		Command: 0,
		Term : 0,
	}
	lm.logBuff = append(lm.logBuff, le)
}

func (lm *LogManager) getAppliedIndex () int {
	return lm.appliedIndex
}

func (lm *LogManager) getCommitIndex () int {
	return lm.commitIndex
}

func (lm *LogManager) setAppliedIndex (appliedIndex int) {
	lm.appliedIndex = appliedIndex
}

func (lm *LogManager) setCommitIndex (commitIndex int) {
	lm.commitIndex = commitIndex
}

func (lm *LogManager) decode (commitIndex int, appliedIndex int, logBuff []LogEntries) {
	lm.commitIndex = commitIndex
	lm.appliedIndex = appliedIndex
	lm.logBuff = logBuff
}

func (lm *LogManager) logLimit (commitIndex int, lastLogIndex int, lastLogTerm int) bool {
	logIndex, logTerm := lm.getLastLogMsg()
	return commitIndex > lm.commitIndex ||
		(commitIndex == lm.commitIndex && (lastLogIndex >= logIndex && lastLogTerm >= logTerm))
}

//
// 获取最后一个日志的信息
//
func (lm *LogManager) getLastLogMsg() (int, int) {
	logIndex := len(lm.logBuff) - 1
	return logIndex, lm.logBuff[logIndex].Term
}

//
// 获取最后一个日志的索引
//
func (lm *LogManager) getLastLogIndex() int {
	return len(lm.logBuff) - 1
}

//
// 获取最后一个日志的任期
//
func (lm *LogManager) getLastLogTerm() int {
	return lm.logBuff[lm.getLastLogIndex()].Term
}

//
// 获取指定日志的任期
//
func (lm *LogManager) getLogTerm(index int) int {
	return lm.logBuff[index].Term
}

//
// 获取指定日志的任期
//
func (lm *LogManager) getLogBuffSize() int {
	return len(lm.logBuff)
}

func (lm *LogManager) getNeedSyncLogEntries (nextIndex int) []LogEntries {
	if nextIndex <= lm.getLastLogIndex() {
		end := lm.getLogBuffSize()
		if end >= nextIndex + ONEMAXLOGCOUNT {
			end = nextIndex + ONEMAXLOGCOUNT
		}
		return lm.logBuff[nextIndex:end]
	}
	return nil
}

//
// 提交未应用的日志
//
func (lm *LogManager) submitCommitLog (applyCh chan ApplyMsg) {
	// 向k/v服务器发送已提交未应用的消息
	for i := lm.appliedIndex + 1; i <= lm.commitIndex; i++{
		applyMsg := ApplyMsg {
			CommandValid : true,
			Command : lm.logBuff[i].Command,
			CommandIndex : i,
		}
		applyCh <- applyMsg
		//rf.commitLog(applyMsg)
		lm.appliedIndex++
	}
}

func (lm *LogManager) logAppend (command interface{}, term int) {
	le := LogEntries{
		Command: command,
		Term : term,
	}
	lm.logBuff = append(lm.logBuff, le)
}

func (lm *LogManager) logAppendArrays (logEntries []LogEntries) {
	lm.logBuff = append(lm.logBuff, logEntries...)
}

func (lm *LogManager) setLogBuff (logBuff []LogEntries) {
	lm.logBuff = logBuff
}

func (lm *LogManager) getLogBuff () []LogEntries {
	return lm.logBuff
}

func (lm *LogManager) logSyncPorc (commitIndex int, prevIndex int, prevTerm int, logEntries []LogEntries) (bool, int){
	// args.PrevIndex <= rf.logIndex 是考虑到 对方比我方日志更少
	logIndex := lm.getLastLogIndex()
	// 如若找到最后一条相同日志
	if prevIndex <= logIndex && prevTerm ==  lm.getLogTerm(prevIndex) {
		// 删除无用日志
		if prevIndex < logIndex {
			lm.logBuff = lm.logBuff[:prevIndex + 1]
		}
		// 追加日志
		if len(logEntries) > 0 {
			lm.logBuff = append(lm.logBuff, logEntries...)
		}
		// 更新日志提交索引
		if commitIndex > lm.getCommitIndex() {
			logIndex = lm.getLastLogIndex()
			lm.commitIndex = logIndex
			// 需要考虑匹配上的日志数量少于提交数量的情况
			if commitIndex <= logIndex {
				lm.commitIndex = commitIndex
			}
		}
		return true, len(lm.logBuff) - 1
	}
	if prevIndex > logIndex {
		return false, logIndex
	}
	return false, prevIndex - 1
}