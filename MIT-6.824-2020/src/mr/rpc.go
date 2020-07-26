package mr

//
// RPC definitions.
//

// Add your RPC definitions here.

/*
注册表 : 如若注册失败,换一个ID继续注册。
*/
type RegisterTable struct{
	UID uint32 // Worker唯一ID
}
type Application struct{
	
}
type SubmitMessage struct{
	/* 
	最高两个bit表示类型 : 
		0 : wait
		1 : map
		2 : reduce
		3 : 未分配的任务号
	低30bit表示任务id
	*/
	TaskCode uint32 
	SubmitType uint32
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
	TaskCode uint32 
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

