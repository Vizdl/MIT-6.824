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

func (lm *LogManager) getLogBuffContext(begin int, end int) []LogEntries {
	return lm.logBuff[begin:end]
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
	//rf.persist()
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


func (lm *LogManager) logCutFormBegin (end int) {
	lm.logBuff = lm.logBuff[:end]
}

func (lm *LogManager) logCutToEnd (begin int) {
	lm.logBuff = lm.logBuff[begin:]
}

func (lm *LogManager) logCut (begin int, end int) {
	lm.logBuff = lm.logBuff[begin:end]
}