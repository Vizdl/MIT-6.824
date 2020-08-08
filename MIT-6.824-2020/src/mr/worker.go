package mr

import "fmt"
import "log"
import "net"
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

	// 注册成为对应master的worker
	registerTable := RegisterTable{}
	registerResult := RegisterResult{}
	// 向Master注册成为Worker
	if !call("Master.RegisterWorker", &registerTable, &registerResult, false, 0) {
		fmt.Println("call Master.RegisterWorker failed.程序退出。")
		return 
	} 
	wuid := registerResult.WUID
	// 循环请求任务
	for true {
		application := Application{
			WUID : wuid,
		}
		taskMessage := TaskMessage{}
		// 请求任务
		if !call("Master.GetTask", &application, &taskMessage, false, 0) {
			fmt.Println("call Master.GetTask failed.程序退出。")
			return 
		} 
		submitMessage := SubmitMessage{
			WUID : wuid,
			TaskType : taskMessage.TaskType,
			TaskId : taskMessage.TaskId,
			SubmitType : Completed,
		}
		submitResult := SubmitResult{}
		fmt.Printf("taskMessage : %v\n",taskMessage)
		switch taskMessage.TaskType {
		case MapTask:
			// 如若收到任务就超时了。
			if taskMessage.TimeStamp <= time.Now().UnixNano() {
				fmt.Println("map %d 收到任务就超时了。",taskMessage.TaskId)
				continue
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
			// 将kay-value分配到不同的数组中。
			intermediates := [][]KeyValue{}
			for i := 0; i < taskMessage.NReduce; i++{
				intermediates = append(intermediates, []KeyValue{})
			}
			for i := 0; i < len(kva); i++{
				idx := ihash(kva[i].Key) % taskMessage.NReduce
				intermediates[idx] = append(intermediates[idx], kva[i])
			}
			// 创建输出文件表
			uuu := 0
			for i := 0; i < taskMessage.NReduce; i++ {
				file, _ := os.Create(fmt.Sprintf("%s/mr-%d-%d", taskMessage.Dir, taskMessage.TaskId, i))
				encoder := json.NewEncoder(file)
				encoder.Encode(intermediates[i])
				uuu+= len(intermediates[i])
				file.Close()
			}
			break
		case ReduceTask:
			// 如若收到任务就超时了。
			if taskMessage.TimeStamp <= time.Now().UnixNano(){
				fmt.Println("reduce %d 收到任务就超时了。",taskMessage.TaskId)
				continue
			}
			intermediate := []KeyValue{}
			// 遍历目录下的文件,观察其文件名是否符合。
			dir_list, err := ioutil.ReadDir(taskMessage.Dir)
			if err != nil {
				fmt.Println(err)
				return
			}
			times := 0
			for _, v := range dir_list {
				match,_ := regexp.MatchString(fmt.Sprintf("mr-([1-9][0-9]*|0)-%d$", taskMessage.TaskId),v.Name())
				if match {
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
					times++
				}
			}

			// 排序
			sort.Sort(ByKey(intermediate))
			// 分类然后reduce
			// 创建输出文件
			ofile, _ := os.Create(taskMessage.File)
			defer ofile.Close()
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
			break
		default :
			panic("worker.go/worker : 未定义的任务类型")
		}
		// 提交任务失败,表示master可能宕机了,但这个时候不退出。而是等再次申请任务时退出。
		if !call("Master.SubmitTask", &submitMessage, &submitResult, true, taskMessage.TimeStamp - 10000000) { // 提前十毫秒。
			fmt.Printf("call Master.SubmitTask failed, taskMessage : %v\n",taskMessage)
			continue
		}
	}
}

func call(rpcname string, args interface{}, reply interface{}, startTimeout bool, timeout int64) bool {
	limit := time.Duration(timeout - time.Now().UnixNano()) * time.Nanosecond
	if !startTimeout{
		limit = 0
	}
	// time.Sleep(time.Second)
	conn, err := net.DialTimeout("tcp", "127.0.0.1:4444", limit)
	if err != nil {
		fmt.Println("TCP连接超时")
		return false // 任务超时
	}
	defer conn.Close()

	if startTimeout {
    	conn.SetReadDeadline(time.Unix(0, timeout)) // 第一个参数为秒,第二个为纳秒
	}
	/*
	调用超时的原理超时之后就不重发了？但是之前发的可能已到达。
	*/
    client := rpc.NewClient(conn)
	defer client.Close()
	
	err = client.Call(rpcname, args, reply)
	if err != nil { // 任务超时
		fmt.Println("RCP超时")
		return false
	}
	return true
}
