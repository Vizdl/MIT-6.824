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
	for true {
		application := Application{}
		taskMessage := TaskMessage{}
		call("Master.GetTask", &application, &taskMessage) // 请求任务
		// time.Sleep(time.Second * 5)
		submitMessage := SubmitMessage{taskMessage.TaskCode,uint32(1)}
		fmt.Printf("taskMessage value :  %v\n", taskMessage)
		taskType := taskMessage.TaskCode >> 30;
		taskId := (taskMessage.TaskCode << 2) >> 2
		fmt.Printf("taskMessage.TaskCode : %x, taskType : %x, taskId ： %x\n",taskMessage.TaskCode,taskType, taskId)
		switch taskType {
		case 1:
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
			// 将kay-value分配到不同的数组中。
			intermediates := [][]KeyValue{}
			for i := 0; i < taskMessage.NReduce; i++{
				intermediates = append(intermediates, []KeyValue{})
			}
			for i := 0; i < len(kva); i++{
				// fmt.Fprintf(encoders[ihash(kva[i].Key)], "%v %v\n", kva[i].Key, kva[i].Value)
				idx := ihash(kva[i].Key) % taskMessage.NReduce
				intermediates[idx] = append(intermediates[idx], kva[i])
			}
			// 创建输出文件表
			for i := 0; i < taskMessage.NReduce; i++ {
				file, _ := os.Create(fmt.Sprintf("%s/mr-%d-%d", taskMessage.Dir, taskId, i))
				encoder := json.NewEncoder(file)
				defer file.Close()
				encoder.Encode(intermediates[i])
			}
			call("Master.SubmitTask", &submitMessage, &taskMessage)
			break
		case 2: // reduce
			intermediate := []KeyValue{}
			// 创建输出文件
			ofile, _ := os.Create(taskMessage.File)
			// 遍历目录下的文件,观察其文件名是否符合。
			dir_list, err := ioutil.ReadDir(taskMessage.Dir)
			if err != nil {
				fmt.Println(err)
				return
			}
			for _, v := range dir_list {
				match,_ := regexp.MatchString(fmt.Sprintf("mr-[1-9][0-9]*-%d$", taskId),v.Name()) 
				if match { // 如若匹配则添加到输入中
					var temp []KeyValue
					// 打开这个文件
					file ,_ := os.Open(taskMessage.Dir + "/" + v.Name())
					decoder := json.NewDecoder(file)
					err = decoder.Decode(&temp)
					if err != nil {
						fmt.Println("Decoder failed", err.Error())
					} 
					intermediate = append(intermediate, temp...)
					file.Close()
				}
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
			call("Master.SubmitTask", &submitMessage, &taskMessage)
			break
		default :// wait
			// 睡眠然后继续
			time.Sleep(time.Second)
			continue
		}
	}
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
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":9999")
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
