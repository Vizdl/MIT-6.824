package main 

import "time"
import "fmt"

func main (){
	fmt.Printf("时间戳类型 : %T\n",time.Now().Unix()); // int64
	fmt.Printf("纳秒时间戳类型 : %T\n",time.Now().UnixNano());
	// time.Sleep(time.Nsec * 6)
	t := time.Unix(100, 0) // 探测这个函数的返回值,输入值已知,第一个参数为
	fmt.Printf("time.Unix(100, 0) : %T\n",t); // time.Time
}