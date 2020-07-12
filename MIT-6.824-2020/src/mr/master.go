package mr

// import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
// import "time"


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
这些阶段要按照完成顺序来。
*/
type MasterStatus int32
const (
    MasterMap MasterStatus = iota       
    MasterReduce            
	MasterComplete
	MasterFailed
)

type Master struct {
	// Your definitions here.
	nReduce int
	states MasterStatus
	unsent int /* 当前阶段未派出的任务数量 */
	uncompleted int /* 未完成的任务数量 */
	/*
	对任务进行管理的一个结构 : 
	要求 : 
	1) 找出一个状态为未分配的任务。
	2) 找出一个状态为已分配的任务。
	*/
	mapTask[]*TaskMessage
	runMapTask[]*TaskMessage /* 已分配出去的mapTask */
	reduceTask[]*TaskMessage
	runReduceTask[]*TaskMessage /* 已分配出去的reduceTask */
	// 当前master需要有锁来保护数据,因为存在竞态。
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
/*
GetTask : 提交申请书(Application),获取申请结果。
返回值 :
taskMessage : 如若申请失败为nil,申请者应该退出。否则应该是有效指针。
*/
func (m *Master) GetTask (application *Application, taskMessage *TaskMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 如若任务完全派出
	if m.unsent <= 0 {
		temp := TaskMessage{uint32(0), "111", "222", m.nReduce}
		*taskMessage = *(&temp)
		fmt.Println("让worker回去睡觉")
		fmt.Printf("taskMessage value :  %v\n", taskMessage)
	} else {
		// 找到第一个没有派发出去的任务。
		var firstTask *[]*TaskMessage
		var firstRunTask *[]*TaskMessage
		if m.states == MasterMap {
			firstTask = &m.mapTask
			firstRunTask = &m.runMapTask
			fmt.Printf("任务类型为 Map\n")
		}else {
			firstTask = &m.reduceTask
			firstRunTask = &m.runReduceTask
			fmt.Printf("任务类型为 Reduce\n")
		}
		for i, task := range (*firstTask) {
			if task != nil {
				fmt.Printf("第%d次任务分配,分配出去了任务 %d\n", m.unsent, i)
				*taskMessage = *task
				fmt.Printf("taskMessage value :  %v\n", taskMessage)
				(*firstRunTask)[i] = task
				// 睡眠然后继续
				// time.Sleep(10)
				(*firstTask)[i] = nil
				// *taskMessage = *(*firstRunTask)[i]
				break;
			}
		}
		m.unsent--
	}
	return nil;
}

/*
SubmitTask : 提交任务,告知master当前自己的任务状态(可能成功也可能失败)。
涉及到两种任务状态转换 :
已派发 -> 已完成
已派发 -> 未派发
*/
func (m *Master) SubmitTask(submitMessage *SubmitMessage, taskMessage *TaskMessage)error{
	m.mu.Lock()
	defer m.mu.Unlock()
	// taskType := submitMessage.TaskCode >> 30;
	taskId := (submitMessage.TaskCode << 2) >> 2
	fmt.Printf("submitMessage value :  %v\n", submitMessage)
	if submitMessage.SubmitType == 0 { // 已派发 -> 未派发
		m.unsent++
		// 将任务回归到未派发队列中。
		var firstTask *[]*TaskMessage
		var firstRunTask *[]*TaskMessage
		if m.states == MasterMap {
			firstTask = &m.mapTask
			firstRunTask = &m.runMapTask
			fmt.Printf("被提交的任务类型为 Map\n")
		}else {
			firstTask = &m.reduceTask
			firstRunTask = &m.runReduceTask
			fmt.Printf("被提交的任务类型为 Reduce\n")
		}
		fmt.Printf("任务 %d 失败了\n", taskId)
		(*firstTask)[taskId] = (*firstRunTask)[taskId] 
		(*firstRunTask)[taskId] = nil
	}else if submitMessage.SubmitType == 1 { // 已派发 -> 已完成
		m.uncompleted--
	}else {
	}

	// 状态变化
	if m.uncompleted == 0 { // 当前阶段任务全部完成了。
		if m.states == MasterMap {
			m.states = MasterReduce
			m.unsent = m.nReduce
			m.uncompleted = m.nReduce
		}else if m.states == MasterReduce {
			m.states = MasterComplete;
		}else {

		}
	}
	return nil;
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":3333")
	// os.Remove("mr-socket")
	// l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil) /* 创建goroutine去运行该函数 */
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	if nReduce <= 0 {
		nReduce = 1
	}
	
	m := Master{}
	m.unsent = len(files)
	m.uncompleted = len(files)
	// Your code here.
	// 核查文件系统是否存在files内文件
	for _, filename := range files {
		file, err := os.Open(filename)
		// fmt.Printf(filename + "\n")
		if err != nil {
			m.states = MasterFailed
			log.Fatalf("cannot open %v", filename)
		}
		file.Close()
	}
	m.states = MasterMap
	m.mapTask = make([]*TaskMessage, len(files))
	m.runMapTask = make([]*TaskMessage, len(files))
	m.reduceTask = make([]*TaskMessage, nReduce)
	m.runReduceTask = make([]*TaskMessage, nReduce)
	m.nReduce = nReduce

	// 设置 map 和 reduce 任务。
	taskId := uint32(0)
	for _, filename := range files {
		taskMessage := TaskMessage{(1 << 30) + taskId, filename, "./mid/", nReduce}
		m.mapTask[taskId] = &taskMessage
		m.runMapTask[taskId] = nil;
		taskId++
	}
	taskId = uint32(0)
	for i := 0; i < nReduce; i++{
		taskMessage := TaskMessage{(2 << 30) + taskId, fmt.Sprintf("mr-out-%d", taskId), "./mid/", nReduce}
		m.reduceTask[i] = &taskMessage
		m.runReduceTask[i] = nil;
		taskId++
	}
	// 开启服务,等待连接。
	m.server()
	return &m
}
