package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "sync"
import "fmt"
import "time"

/*
容错 : 
如何判断 worker 是否掉线？
	从给定任务开始计时,如若在特定时间内未完成任务,则认为超时。
如若任务需要执行的时间确实超过这个固定的限定?
	第一次限时为 x 秒,派发出去两次如若都未完成,则限定改为 x*2秒。以此类推。
如若已派发的任务其实还在进行,但是还需要一点时间,而这个时候按照判定将其视为失败。但之后该worker完成了,来提交任务怎么办?
	1) 避免时间差,假设 master 任务 100ms 超时, 而 worker 则认为 95ms 内没完成就是超时。 这样因时间上的精度问题而导致双方认知不一致的问题。
	然后设置发送超时时间,达成 处理时间 + 超时时间 + 定时器的最大误差 * 2 < 限定超时时间, 就可以确保 master worker双方达成一致。
*/


/*
定时任务go语言具体实现方案
	master : 
		每次派发出去任务,都做一个定时任务,且这个定时任务有两个管道,分别代表两种事件发生 : 任务成功与任务超时。
	worker : 

*/

/*
对于一个任务有三种状态 : 
1) 未派发
2) 已派发
3）已完成

对于整体任务有四种状态 : 
MasterMap : 正常初始化后就是 Map
MasterReduce : 所有 Map 成功完成后
MasterComplete : 所有 Reduce 成功完成后
MasterFailed : 异常初始化后 
*/


/*
什么时候 master 死亡? 
	当所有任务都结束的时候
什么时候 worker 死亡?
	获取任务很久但是超时的时候
什么时候 master 认定 任务失败?
	1) 任务长时间未完成
	2) 任务被提交的时候返回为失败。
*/

/*
事件接口设计 ：
	事件初始化 : go startTaskEvenetMonitor
	事件发生 : 
		超时事件 : 由定时器定时发生。
		完成事件 : 在提交任务处提交。
	参与的协程 : 
		1) 定时器协程
		2) rpc提交任务协程
		3) rpc申请任务协程
*/
const NOTASK = 0xffffffff // 2^32-1为没有任务,任务号不允许等于它
const MAXWUID = 0xffffffff // 最大为 2^32 - 1
/*
这些阶段要按照完成顺序来。
*/
type EMasterStatus int32
const (
    MasterMap EMasterStatus = iota       
    MasterReduce            
	MasterComplete
	MasterFailed
)
type EEventCode uint32
const (
    CompletedEvent = iota       
    TimeoutEvent    
)
/*
事件 : 
*/
type Event struct{
	WUID uint32
	Code EEventCode
	TaskId uint32
}
/*
服务器的状态随着 rpc调用 不断发生变更。
*/
type Master struct {
	// Your definitions here.
	nReduce int // 多线程读,初始化时才有写。 不需要锁保护。
	completedEvents[]chan struct{} /* 任务完成事件组,管道不需要锁保护,因为两个线程调用的是两段,并且是线程安全的。*/
	mu sync.Mutex				// 用来保存服务器状态的。


	/* 以下都是服务器的状态 */
	wuid uint32	/* worker 唯一id,每次都递增 */
	states EMasterStatus 
	unsent int 
	uncompleted int
	mapTask[]*TaskMessage
	runMapTask[]*TaskMessage /* 已分配出去的mapTask */
	reduceTask[]*TaskMessage
	runReduceTask[]*TaskMessage /* 已分配出去的reduceTask */
	workerMap map[uint32]uint32 /* key : wuid, value : taskid, 如若当前worker无有效任务,则value=NOTASK*/
	timeoutLimits []int64		/* 单位:纳秒,每次未完成,都将超时时间提升2数倍 */
	eventChan chan Event		/* 事件管道 */
}
/////////////////////////////////////////////=///////////////////////////////////// 静态lib函数 ////////////////////////////////////////////////////////////////////////////////////////
func Max(x, y int) int {
    if x < y {
        return y
    }
    return x
}

/////////////////////////////////////////////////////////////////////////////////////// PRIVATE ///////////////////////////////////////////////////////////////////////////////////////
/*
根据当前的状态,获取到对应的任务数组。
*/
func (m *Master) getCurrTaskSArrPairPtr()(*[]*TaskMessage, *[]*TaskMessage){
	if m.states == MasterMap {
		return &m.mapTask, &m.runMapTask
	}else if m.states == MasterReduce{
		return &m.reduceTask, &m.runReduceTask
	}else {
		fmt.Printf("错误的任务类型,当前master处于编号为 %d 的状态下\n",m.states)
		return nil, nil
	}
} 


func (m *Master) procEvents(){
	for true{
		event := <-m.eventChan // 从管道接收端取出事件
		m.eventExecuter(event) // 执行事件
	}
}



/*
私有函数 : 取消已经派送的任务。内部不加锁。但是函数外部要加锁。
*/
func (m *Master) cancelIssuedTask (taskId uint32){
	m.unsent++
	firstTask, firstRunTask := m.getCurrTaskSArrPairPtr()
	(*firstTask)[taskId] = (*firstRunTask)[taskId] 
	(*firstRunTask)[taskId] = nil
	if m.timeoutLimits[taskId] & (1 << 61) == 0{ // 防止溢出
		m.timeoutLimits[taskId] <<= 1	// 增加超时时长
	}
	return 
}

func (m *Master) finishIssuedTask (taskId uint32){
	m.uncompleted--
	if m.uncompleted == 0 { // 当前阶段任务全部完成了。
		if m.states == MasterMap {
			m.states = MasterReduce
			m.unsent = m.nReduce
			m.uncompleted = m.nReduce
		}else if m.states == MasterReduce {
			m.states = MasterComplete
			m.unsent = 0
			m.uncompleted = 0
			fmt.Printf("==================== 所有任务完成 ====================\n")
		}else {
			fmt.Printf("==================== finishIssuedTask发生错误 ====================\n")
		}
	}
}

/*
任务定时器 : 通过开启一个 goroutine 利用 select 来进行监听 '事件(管道)'来达成对事件的响应。
输入 : 任务号与任务超时时刻的时间戳。
*/
func (m *Master) taskTimer(wuid uint32, taskId uint32, timeout int64){
	// 设置定时器,经过实验,如果limit是负数就直接事件发生。
	limit:= time.Duration(timeout - time.Now().UnixNano()) * time.Nanosecond // 会有微量不可避免的偏差
	timeoutEvent := time.NewTimer(limit)
	<-timeoutEvent.C // 等待超时事件发生
	m.eventChan <- Event{
		TaskId : taskId,
		WUID : wuid,
		Code : TimeoutEvent,
	}
	timeoutEvent.Stop()
	return 
}

func (m *Master) eventExecuter(event Event){
	m.mu.Lock()
	defer m.mu.Unlock()
	// 查看事件是否还有效
	tid,exist := m.workerMap[event.WUID] 
	if !exist || tid != event.TaskId{
		return 
	}
	switch event.Code{
	case TimeoutEvent: // 超时
		m.cancelIssuedTask(event.TaskId)
		m.workerMap[event.WUID] = NOTASK
		break
	case CompletedEvent: // 完成
		m.finishIssuedTask(event.TaskId)
		m.workerMap[event.WUID] = NOTASK
		break
	}
}

/////////////////////////////////////////////////////////////////////////////////////// PUBLIC ///////////////////////////////////////////////////////////////////////////////////////
func (m *Master) RegisterWorker (registerTable *RegisterTable, registerResult *RegisterResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	registerResult.WUID = m.wuid
	m.workerMap[m.wuid] = NOTASK
	if m.wuid < MAXWUID{
		m.wuid++
	}else {
		fmt.Printf("wuid 已达到最大,无法再分配!")
		return nil
	}
	return nil
}


/*
GetTask : 提交申请书(Application),获取申请结果。
返回值 :
taskMessage : 如若申请失败为nil,申请者应该退出。否则应该是有效指针。
*/
func (m *Master) GetTask (application *Application, taskMessage *TaskMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	taskId,exist := m.workerMap[application.WUID]
	if !exist || taskId != NOTASK{
		fmt.Println(application.WUID," 的任务请求出错")
		return nil
	}
	if m.unsent <= 0 {
		temp := TaskMessage{
			TaskType : WaitTask,
			TaskId : uint32(0),
			File : "",
			Dir : "", 
			NReduce : m.nReduce,
		}
		*taskMessage = *(&temp)
	} else {
		firstTask ,firstRunTask := m.getCurrTaskSArrPairPtr()
		for i, task := range (*firstTask) {
			if task != nil {
				(*task).TimeStamp = time.Now().UnixNano() + m.timeoutLimits[i]
				m.workerMap[application.WUID] = uint32(i)
				*taskMessage = *task
				(*firstRunTask)[i] = task
				(*firstTask)[i] = nil
				// 大概估计 : 一次取余运算需要五个时钟周期(50ns)。
				go m.taskTimer(application.WUID, uint32(i), (*task).TimeStamp)
				break
			}
		}
		m.unsent--
	}
	return nil
}


/*
SubmitTask : 提交任务,告知master当前自己的任务状态(可能成功也可能失败)。
涉及到两种任务状态转换 :
已派发 -> 已完成
已派发 -> 未派发
*/
func (m *Master) SubmitTask(submitMessage *SubmitMessage, submitResult *SubmitResult)error{
	switch submitMessage.SubmitType{
	case 1 :
		fmt.Printf("submitMessage value :  %v\tBEGIN\n", submitMessage)
		fmt.Println("m.completedEvents[",submitMessage.TaskId,"]:", len(m.completedEvents[submitMessage.TaskId]))
		m.eventChan <- Event{
			TaskId : submitMessage.TaskId,
			WUID : submitMessage.WUID,
			Code : CompletedEvent,
		}
		fmt.Printf("submitMessage value :  %v\tEND\n", submitMessage)
		break
	default :
		fmt.Printf("SubmitTask : submitMessage.SubmitType error")
	}
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	newServer := rpc.NewServer()
    newServer.Register(m)
    l, e := net.Listen("tcp", "127.0.0.1:4444") 
	if e != nil {
		log.Fatal("listen error:", e)
	}
    go newServer.Accept(l)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := m.states == MasterComplete || m.states == MasterFailed
	return ret
}

func MakeMaster(files []string, nReduce int) *Master {
	if nReduce <= 0 {
		nReduce = 1
	}
	nMap := len(files)
	m := Master{
		wuid : 1,
		nReduce : nReduce,
		states : MasterMap,
		unsent : nMap,
		uncompleted : nMap,
		mapTask : make([]*TaskMessage, nMap),
		runMapTask : make([]*TaskMessage, nMap),
		reduceTask : make([]*TaskMessage, nReduce), 
		runReduceTask : make([]*TaskMessage, nReduce),
		completedEvents : make([]chan struct{}, Max(nMap, nReduce)),
		timeoutLimits : make([]int64, Max(nMap, nReduce)),
		workerMap : make(map[uint32]uint32),
		eventChan : make(chan Event, 1024),
	}
	for i := 0; i < len(m.completedEvents); i++{
		m.completedEvents[i] = make(chan struct{})
	}
	// 设置初始timeout
	for i := 0; i < len(m.timeoutLimits); i++ {
		m.timeoutLimits[i] = 100000000 // 100 000 000会有几个任务完不成。
	}
	
	// 核查文件系统是否存在files内文件
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			m.states = MasterFailed
			log.Fatalf("cannot open %v", filename)
		}
		file.Close()
	}

	// 设置 map 和 reduce 任务。
	taskId := uint32(0)
	for _, filename := range files {
		taskMessage := TaskMessage{
			TaskType : MapTask,
			TaskId : taskId, 
			File : filename, 
			Dir : ".", 
			NReduce : nReduce,
		}
		m.mapTask[taskId] = &taskMessage
		m.runMapTask[taskId] = nil
		taskId++
	}
	taskId = uint32(0)
	for i := 0; i < nReduce; i++{
		taskMessage := TaskMessage{
			TaskType : ReduceTask,
			TaskId : taskId,
			File : fmt.Sprintf("mr-out%d", taskId),
			Dir : ".", 
			NReduce : nReduce,
		}
		m.reduceTask[i] = &taskMessage
		m.runReduceTask[i] = nil
		taskId++
	}
	// 开启服务,等待连接。
	m.server()
	// 开启事件处理线程
	go m.procEvents()
	return &m
}
