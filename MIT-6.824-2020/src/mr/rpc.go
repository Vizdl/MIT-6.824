package mr

//
// RPC definitions.
//

// Add your RPC definitions here.

type ETaskType int32
const (
    MapTask ETaskType = iota
    ReduceTask       
)


type ESubmitType uint32
const (
	Completed ESubmitType = iota
)
/*
注册表 : 如若注册失败,换一个ID继续注册。
*/
type RegisterTable struct{
}

type RegisterResult struct{
	WUID uint32 // Worker唯一ID
}


type Application struct{
	WUID uint32 // Worker唯一ID
}
type SubmitMessage struct{
	WUID uint32 // Worker唯一ID
	TaskType ETaskType
	TaskId uint32 
	SubmitType ESubmitType
}

type TaskMessage struct{
	TaskType ETaskType
	TaskId uint32 
	/*
	对于 map 来说 : file 是输入,dir是输出。
	对于 reduce 来说 : dir 是输入, file 是输出。
	*/
	File string 
	Dir string 
	NReduce int 
	/*
	任务被分配出去的时间戳,只有被分配时出去才有意义。
	*/
	TimeStamp int64
}


type SubmitResult struct{
	IsSucceed bool // 是否成功提交
}

