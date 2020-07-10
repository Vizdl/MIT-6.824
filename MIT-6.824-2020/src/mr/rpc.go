package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Application struct{
	
}

type TaskMessage struct{
	/* 
	最高两个bit表示类型 : 
		0 : wait
		1 : map
		2 : reduce
		3 : 未分配的任务号
	低30bit表示任务id
	*/
	taskCode int 
	file string /* 输入文件 */
	dir string /* 中间目录 */
	nReduce int 
}

