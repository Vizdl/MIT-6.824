// package main

// import (
//     "fmt"
//     "time"
// )

// func main() {
//     /*
//         用sleep实现定时器
//     */
//     fmt.Println(time.Now())
//     time.Sleep(time.Second)
//     fmt.Println(time.Now())
//     /*
//         用timer实现定时器
//     */
//     timer := time.NewTimer(time.Second)
//     fmt.Println(<-timer.C)
//     /*
//         用after实现定时器
//     */
//     fmt.Println(<-time.After(time.Second))

// }

package main
 
import (
 "fmt"
 "time"
)
 
 
func main() {
	//closeChannel()
	c := make(chan int) // 创建一个无缓冲管道
	timeout := time.After(time.Second * 2) // 返回一个时间戳的管道,在特定时间会向里面写入事件
	t1 := time.NewTimer(time.Second * 3) // 效果相同 只执行一次
	var i int
	go func() {
		for {
			select { // 这不是go中的switch,而是多路IO复用中的select的改写版。
			case <-c:
				fmt.Println("channel sign")
				return
			case <-t1.C: // 代码段2
				fmt.Println("3s定时任务")
			case <-timeout: // 代码段1
				i++
				fmt.Println(i, "2s定时输出")
			case <-time.After(time.Second * 4): // 代码段3
				fmt.Println("4s timeout。。。。") 
			default:    // 代码段4
				fmt.Println("default")
				time.Sleep(time.Second * 1)
			}
		}
	}()
	time.Sleep(time.Second * 6)
	close(c) // 关闭管道c,被c察觉到了。
	time.Sleep(time.Second * 2)
	fmt.Println("main退出")
}
