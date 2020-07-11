package main

/*
start the master process, which is implemented
in ../mr/master.go

go run mrmaster.go pg*.txt : 以 pg*.txt作为参数传入到当前程序内。
*/

import "mr"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	// fmt.Printf("os.Args type:%T\n", os.Args)
	// fmt.Printf("os.Args len:%d\n", len(os.Args))
	// fmt.Println(os.Args);
	// fmt.Println(os.Args[2]);
	m := mr.MakeMaster(os.Args[1:], 10) // 调用 master.go 中 MakeMaster 函数。设置有10个 reduce task 
	for m.Done() == false {
		fmt.Println("m.Done() == false");
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
