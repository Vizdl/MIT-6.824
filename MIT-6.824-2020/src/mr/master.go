package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "sync"
import "fmt"
import "time"



const NOTASK = 0xffffffff  		/* 2^32-1为没有任务,任务号不允许等于它 */
const MAXWUID = 0xffffffff 		/* 最大为 2^32 - 1 */

/*
	对于整体任务有四种状态 : 
	MASTERMAP : 正常初始化后就是 MAP
	MASTERREDUCE : 所有 MAP 成功完成后
	MASTERCOMPLETE : 所有 REDUCE 成功完成后
	MASTERFAILED : 异常初始化后 
*/
type EMasterStatus int32
const (
    MasterMap EMasterStatus = iota       
    MasterReduce            
	MasterComplete
	MasterFailed
)


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
type EEventCode uint32
const (
    CompletedEvent = iota       
    TimeoutEvent    
)

type Event struct{
	WUID uint32
	Code EEventCode
	TaskId uint32
}

/*
服务器的状态随着 rpc调用 不断发生变更。

	对于一个任务有三种状态 : 
	1) 未派发
	2) 已派发
	3）已完成
	
*/
type Master struct {
	// Your definitions here.
	nReduce int						/* reduce任务数 */
	completedEvents[]chan struct{} 	/* 任务完成事件组,管道不需要锁保护,因为两个线程调用的是两段,并且是线程安全的。*/
	mu sync.Mutex					/* 用来保护服务器状态 */


	/* 以下都是服务器的状态 */
	wuid uint32						/* worker 唯一id,每次都递增 */
	states EMasterStatus 			/* 当前master阶段 */
	unsent int 						/* 当前阶段未分配出去的任务数量 */
	uncompleted int					/* 当前阶段未完成的任务数量 */
	mapTask[]*TaskMessage 			/* 未分配出去的mapTask */
	runMapTask[]*TaskMessage 		/* 已分配出去的mapTask */
	reduceTask[]*TaskMessage		/* 未分配出去的reduceTask */
	runReduceTask[]*TaskMessage 	/* 已分配出去的reduceTask */
	workerMap map[uint32]uint32 	/* key : wuid, value : taskid, 如若当前worker无有效任务,则value=NOTASK*/
	timeoutLimits []int64			/* 单位:纳秒,每次未完成,都将超时时间提升2数倍 */
	eventChan chan Event			/* 事件管道 */
	taskTimers []*time.Timer		/* 各个任务的定时器,用来关闭定时任务,只有在任务已分配出去时才有效。 */
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
函数功能 : 给当前服务器阶段的具体任务定时。当超出时间戳后,将自动启动超时任务。
输入 :
timeout : 超时时间戳。
taskId : 需定时的任务ID
wuid : 获取到任务号为taskId的worker的唯一ID。
*/
func (m *Master) taskTimer(wuid uint32, taskId uint32, timeout int64) *time.Timer{
	// 设置定时器,经过实验,如果limit是负数就直接事件发生。
	limit:= time.Duration(timeout - time.Now().UnixNano()) * time.Nanosecond // 会有微量不可避免的偏差
	return  time.AfterFunc(limit, func(){ // 在超时候自动调用
		m.eventChan <- Event{
			TaskId : taskId,
			WUID : wuid,
			Code : TimeoutEvent,
		}
	})
}


/*
函数功能 : 单次事件处理。
*/
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
		break
	case CompletedEvent: // 完成
		m.finishIssuedTask(event.TaskId)
		break
	}
	m.workerMap[event.WUID] = NOTASK
}

/////////////////////////////////////////////////////////////////////////////////////// PUBLIC ///////////////////////////////////////////////////////////////////////////////////////
/*
函数功能 : 注册worker服务器。
*/
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
函数功能 : 获取任务。
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
				m.taskTimers[i] = m.taskTimer(application.WUID, uint32(i), (*task).TimeStamp)
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
func (m *Master) SubmitTask(submitMessage *SubmitMessage, submitResult *SubmitResult) error {
	submitResult.IsSucceed = false
	switch submitMessage.SubmitType{
	case 1 :
		fmt.Printf("submitMessage value :  %v\tBEGIN\n", submitMessage)
		fmt.Println("m.completedEvents[",submitMessage.TaskId,"]:", len(m.completedEvents[submitMessage.TaskId]))

		m.mu.Lock()
		tid,exist := m.workerMap[submitMessage.WUID]
		// 为了避免关闭错了定时器。
		if exist && tid == submitMessage.TaskId && m.taskTimers[submitMessage.TaskId] != nil {
			submitResult.IsSucceed = m.taskTimers[submitMessage.TaskId].Stop()
		}
		m.mu.Unlock()

		if submitResult.IsSucceed {
			m.eventChan <- Event{
				TaskId : submitMessage.TaskId,
				WUID : submitMessage.WUID,
				Code : CompletedEvent,
			}
		}
		fmt.Printf("submitMessage value :  %v\tEND\n", submitMessage)
		break
	default :
		fmt.Printf("SubmitTask : submitMessage.SubmitType error")
	}
	return nil
}


/*
rpc服务
*/
func (m *Master) server() {
	newServer := rpc.NewServer()
    newServer.Register(m)
    l, e := net.Listen("tcp", "127.0.0.1:4444") 
	if e != nil {
		log.Fatal("listen error:", e)
	}
    go newServer.Accept(l)
}


/*
函数功能 : 判断所有任务是否完成。
*/
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := m.states == MasterComplete || m.states == MasterFailed
	return ret
}

/*
函数功能 : 创建master服务器,并初始化服务器状态。
*/
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
		taskTimers : make([]*time.Timer, Max(nMap, nReduce)),
	}
	for i := 0; i < len(m.completedEvents); i++{
		m.completedEvents[i] = make(chan struct{})
	}
	// 设置初始timeout
	for i := 0; i < len(m.timeoutLimits); i++ {
		m.timeoutLimits[i] = 100000000 
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
