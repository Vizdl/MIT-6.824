package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "time"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	// 循环请求任务
	application := Application{}
	taskMessage := TaskMessage{}
	for true {
		fmt.Printf("aaa")
		call("Master.GetTask", &application, &taskMessage) // 请求任务
		// if taskMessage == nil{ // 无任务
		// 	break; // 退出
		// }
		fmt.Printf("aaa")
		taskType := taskMessage.taskCode >> 30;
		taskId := (taskMessage.taskCode << 2) >> 2
		if taskType == 0 { // wait
			// 睡眠然后继续
			time.Sleep(10)
			continue
		}
		if taskType == 1 { // map
			// 创建输出文件表
			files := make([]*os.File, taskMessage.nReduce)
			for i := 0; i < taskMessage.nReduce; i++ {
				files[i], _ = os.Create(fmt.Sprintf("%s/mr%d-%d", taskMessage.dir, taskId, i))
			}
			// 打开输入文件
			file, err := os.Open(taskMessage.file)
			if err != nil {
				log.Fatalf("cannot open %v", taskMessage.file)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", taskMessage.file)
			}
			file.Close()
			kva := mapf(taskMessage.file, string(content))
			// 将kay-value分配写到不同文件内。
			for i := 0; i < len(kva); i++{
				fmt.Fprintf(files[ihash(kva[i].Key)], "%v %v\n", kva[i].Key, kva[i].Value)
			}
		}else if taskType == 2 { // reduce
			
		}
	}
}

//
// example function to show how to make an RPC call to the master.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

/*
send an RPC request to the master, wait for the response.
usually returns true.
returns false if something goes wrong.
函数功能 : 调用 master 上的函数。
input :
rpcname : 待调用函数名
args : 待调用函数传入参数
reply : 待调用函数传出参数
*/
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	/* 
	defer语句延迟执行一个函数，该函数被推迟到当包含它的程序返回时
	（包含它的函数 执行了return语句/运行到函数结尾自动返回/对应的goroutine panic）执行。 
	*/
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
