package main
 
import (
 "fmt"
)



func main (){
	m := make(map[int]int)
	v,ok := m[1]
	fmt.Printf("v : %d\n", v)
	fmt.Println(ok)
}