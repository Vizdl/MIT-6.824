package kvraft

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string // "Put" or "Append"
}
/*
KVServer : 将数据传达到 raft协议层,如若返回到applyCh则表明提交成功。
如若返回到下一个序号的数据都没有返回,则表示提交失败。
*/
type KVServer struct {
	mu      sync.Mutex
	writemu sync.Mutex
	me      int			// 当前服务器的序号
	rf      *raft.Raft	// 这个k/v服务器的raft服务器。
	applyCh chan raft.ApplyMsg // 接收数据的地方
	dead    int32 		// set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	datas map[string]string
	leaderChancel chan raft.ApplyMsg // 接收数据的地方
}

/*
首先判断当前服务器是否是领导者,如若是则接受请求,如若不是则告诉请求者真正的领导者是谁。
从map中取得数据并且返回。
*/
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	log.Printf("第 %d 个 k/v Raft 服务器 收到 Get 指令 : %+v ", kv.me, args)
	kv.mu.Lock()
	if kv.rf.RaftStatus() == kv.me {
		log.Printf("第 %d 个 k/v Raft 服务器 收到 Get 指令, 并认为自己是领导者 ", kv.me)
		reply.Err = OK
		reply.Value = kv.datas[args.Key]
	}else {
		reply.Err = ErrWrongLeader
	}
	kv.mu.Unlock()
	log.Printf("第 %d 个 k/v Raft 服务器 收到 Get 指令 : %+v, 返回结果为 : %+v ", kv.me, args, reply)
}
/*
首先判断当前服务器是否是领导者,如若是则接受请求,如若不是则告诉请求者真正的领导者是谁。
修改map中的数据,要修改先要将数据给raft,然后看管道中传出的数据。
*/
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op {
		Op: args.Op,
		Key: args.Key,
		Value: args.Value,
	}
	log.Printf("第 %d 个 k/v Raft 服务器 收到 PutAppend 指令 : %+v", kv.me, args)
	/* 如若该 raft 是 Leader 则会自动执行同步,等待同步结果出来就可以知道是否提交成功了 */
	log.Printf("准备抢占kv.writemu")
	kv.mu.Lock()
	log.Printf("成功抢占kv.writemu")
	log.Printf("准备利用raft同步请求")
	_, currLeader, ok := kv.rf.Start(command)
	log.Printf("成功利用raft同步请求")
	reply.Leader = currLeader
	/* 堵塞等待消息,如若得到则 */
	if ok {
		log.Printf("当前server是领导者")
		reply.Err = OK
		log.Printf("准备应用消息")
		message := <-kv.leaderChancel // message :=  读取消息等待
		op := (message.Command).(Op)
		if op.Op == "Put"{
			kv.datas[op.Key] = op.Value
		}else {
			kv.datas[op.Key] += op.Value
		}
		log.Printf("成功应用消息")
	}else {
		log.Printf("当前server不是领导者")
		reply.Err = ErrWrongLeader
	}
	log.Printf("准备释放kv.writemu")
	kv.mu.Unlock()
	log.Printf("成功释放kv.writemu")

	log.Printf("第 %d 个 k/v Raft 服务器 收到 PutAppend 指令 : %+v, 返回结果为 : %+v ", kv.me, args, reply)
}

/* 读取管道中的消息 */
func (kv *KVServer) ReadApplyCh (){
	for true {
		log.Printf("第 %d 台服务器ReadApplyCh : ready read applyCh", kv.me)
		message := <-kv.applyCh
		log.Printf("第 %d 台服务器ReadApplyCh : read applyCh ok", kv.me)
		log.Printf("第 %d 台服务器kv.rf.RaftStatus() == kv.me begin", kv.me)
		if kv.rf.RaftStatus() == kv.me {
			log.Printf("第 %d 台服务器将应用消息同步到 leaderChancel内了", kv.me)
			kv.leaderChancel <- message
		}
		log.Printf("第 %d 台服务器kv.rf.RaftStatus() == kv.me end", kv.me)
	}
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
/*
当KVServer实例不调用Kill()时，测试人员调用Kill()
需要一次。
为了您的方便，我们提供
设置rf.dead的代码(不需要锁)，
和一个用于测试rf.dead的killed()方法
长时间运行的循环。
您也可以添加自己的
杀死()代码。
你没有被要求做任何事
关于这个，但可能方便(例如)
抑制来自Kill()实例的调试输出。
*/
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
/*
servers[] : 包含了将通过Raft合作形成容错密钥/值服务的服务器的集合的端口。
me : 当前服务器在 servers[]中的角标.
这个 k/v 服务器应该存储快照通过底层的raft来实现。
应该调用 persister.SaveStateAndSnapshot() 去原子地保存这个raft服务器状态使用快照的方式。
这个 k/v 服务器应该快照化当raft保存的状态超过了 maxraftstate 个字节,为了允许 raft 去自动回收 它的Log。
如果 maxraftstate 是 -1,你不需要去快照化。
StartKVServer() 必需要快速地返回, 所以它应该为了所有需要长时间运行的工作开启一个协程.
*/
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.datas = make(map[string]string)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.leaderChancel = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // 创建raft服务器

	// You may need initialization code here.
	go kv.ReadApplyCh()
	return kv
}
