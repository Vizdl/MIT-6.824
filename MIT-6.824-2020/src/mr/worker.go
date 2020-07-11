package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "time"
import "encoding/json"
import "sort"
import "regexp"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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
		// fmt.Printf("aaa")
		call("Master.GetTask", &application, &taskMessage) // 请求任务
		// if taskMessage == nil{ // 无任务
		// 	break; // 退出
		// }
		// fmt.Printf("aaa")
		taskType := taskMessage.TaskCode >> 30;
		taskId := (taskMessage.TaskCode << 2) >> 2
		if taskType == 0 { // wait
			// 睡眠然后继续
		// fmt.Printf("aaa")
			time.Sleep(10)
			continue
		}
		if taskType == 1 { // map
			// 创建输出文件表
			encoders := make([]*json.Encoder, taskMessage.NReduce)
			for i := 0; i < taskMessage.NReduce; i++ {
				file, _ := os.Create(fmt.Sprintf("%s/mr-%d-%d", taskMessage.Dir, taskId, i))
				encoders[i] = json.NewEncoder(file)
				defer file.Close()
			}
			// 打开输入文件
			ofile, err := os.Open(taskMessage.File)
			if err != nil {
				log.Fatalf("cannot open %v", taskMessage.File)
			}
			content, err := ioutil.ReadAll(ofile)
			if err != nil {
				log.Fatalf("cannot read %v", taskMessage.File)
			}
			ofile.Close()
			kva := mapf(taskMessage.File, string(content))
			// 将kay-value分配写到不同文件内。
			for i := 0; i < len(kva); i++{
				// fmt.Fprintf(encoders[ihash(kva[i].Key)], "%v %v\n", kva[i].Key, kva[i].Value)
				encoders[ihash(kva[i].Key)].Encode(kva[i])
			}
		}else if taskType == 2 { // reduce
			intermediate := []KeyValue{}
			temp := []KeyValue{}
			// 创建输出文件
			ofile, _ := os.Create(fmt.Sprintf("mr-out-%d", taskId))
			// 打开中间目录下所有该reduce下的中间文件
			// dir, err := os.OpenFile(taskMessage.dir, os.O_RDWR, os.ModeDir)
			// if err != nil {
			// 	fmt.Println(err.Error())
			// 	return
			// }
			// 遍历目录下的文件,观察其文件名是否符合。
			dir_list, err := ioutil.ReadDir(taskMessage.Dir)
			if err != nil {
				fmt.Println(err)
				return
			}
			for _, v := range dir_list {
				match,_ := regexp.MatchString(fmt.Sprintf("mr-*-", taskId),v.Name()) 
				if match { // 如若匹配则添加到输入中
					// 打开这个文件
					file ,_ := os.Open(v.Name())
					decoder := json.NewDecoder(file)
   					err = decoder.Decode(&temp)
					intermediate = append(intermediate, temp...)
				}
				// 排序
				sort.Sort(ByKey(intermediate))
				// 分类然后reduce
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
			}
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
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":2345")
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
