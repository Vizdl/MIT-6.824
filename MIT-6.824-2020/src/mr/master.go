package mr

// import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type MasterStatus int32
const (
    MasterFailed MasterStatus = iota
    MasterMap             
    MasterReduce            
    MasterComplete
)

type Master struct {
	// Your definitions here.
	nReduce int
	states MasterStatus
	taskCounter int /* 当前阶段任务剩余数 */
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

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1 // reply为返回值, args为传入参数
	return nil // 返回nil表示没有错误,返回字符串表示有错误。
}

/*
GetTask : 提交申请书(Application),获取申请结果。
返回值 :
taskMessage : 如若申请失败为nil,申请者应该退出。否则应该是有效指针。
*/
func (m *Master) GetTask(application *Application, taskMessage *TaskMessage)error{
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil;
}

/*
SubmitTask : 提交任务,告知master当前自己的任务状态(可能成功也可能失败)。
*/
func (m *Master) SubmitTask(application *Application, taskMessage *TaskMessage)error{
	return nil;
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
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

	// Your code here.
	// 核查文件系统是否存在files内文件
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			m.states = MasterFailed
			log.Fatalf("cannot open %v", filename)
		}
		file.Close()
	}
	m.states = MasterMap
	m.mapTask = make([]*TaskMessage, len(files))
	m.runMapTask = make([]*TaskMessage, len(files))
	m.reduceTask = make([]*TaskMessage, len(nReduce))
	m.runReduceTask = make([]*TaskMessage, len(nReduce))

	// 设置 map 和 reduce 任务。
	taskId := 0
	for _, filename := range files {
		taskMessage := TaskMessage{(1 << 31) + taskId, nil, nil, nReduce}
		m.mapTask[taskId++] = taskMessage
	}
	for i := 0; i < nReduce; i++{
		taskMessage := TaskMessage{(1 << 31) + taskId, nil, nil, nReduce}
		m.reduceTask[i] = taskMessage
	}
	// 开启服务,等待连接。
	m.server()
	return &m
}
