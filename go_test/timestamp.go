package main 

import "time"
import "fmt"

func main (){
	fmt.Printf("时间戳类型 : %T\n",time.Now().Unix()); // int64
	fmt.Printf("纳秒时间戳类型 : %T\n",time.Now().UnixNano());
	time.Sleep(time.Nsec * 6)
}