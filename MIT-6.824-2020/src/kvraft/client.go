package kvraft

import (
	"labrpc"
	"log"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

/*
随机函数 : 获取int64范围内的随机整数
*/
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}


/*
客户端与服务器是多对多的关系,并不是一一对应。
servers []*labrpc.ClientEnd : k/v server
*/
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers : servers,
	}

	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
/*
1) 先找到真正的领导者
	i) 随机找一个raft服务器,如若是领导者则会返回提交结果,否则会告诉领导者是谁。
2) 向其发送请求
*/
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	/* 随机选取一个服务器 */
	log.Println("Get 开始")
	ok := true
	server := 0
	args := GetArgs{ Key: key }
	reply := GetReply{ Err : ErrWrongLeader }
	for reply.Err == ErrWrongLeader || !ok {
		server = int(nrand()) % (len(ck.servers))
		log.Printf("\nGet 向第 %d 台服务器进行 尝试中",server)
		ok = ck.servers[server].Call("KVServer.Get", &args, &reply)
		if (reply.Err == ErrWrongLeader && reply.Leader == -1) || !ok {
			time.Sleep(100000)
		}
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
/*
1) 先找到真正的领导者
	i) 随机找一个raft服务器,如若是领导者则会返回提交结果,否则会告诉领导者是谁。
2) 向其发送请求
*/
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
	/* 随机选取一个服务器 */
	log.Println("PutAppend 开始")
	ok := true
	server := 0
	args := PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
	}
	reply := PutAppendReply{Err: ErrWrongLeader}
	for reply.Err == ErrWrongLeader || !ok {
		server = int(nrand()) % (len(ck.servers))
		log.Printf("\nPutAppend 向第 %d 台服务器进行 尝试中",server)
		ok = ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if (reply.Err == ErrWrongLeader && reply.Leader == -1) || !ok {
			time.Sleep(100000)
		}
	}
	log.Printf("client call ok, args : %+v, reply : %+v",args,reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
