package main

import (
    "fmt"
    "time"
)

func main() {
    /*
        用sleep实现定时器
    */
    fmt.Println(time.Now())
    time.Sleep(time.Second)
    fmt.Println(time.Now())
    /*
        用timer实现定时器
    */
    timer := time.NewTimer(time.Second)
    fmt.Println(<-timer.C)
    /*
        用after实现定时器
    */
    fmt.Println(<-time.After(time.Second))

}