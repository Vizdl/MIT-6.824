package main
 
import (
 "fmt"
)
/*
结论 : defer不是作用域{}失效执行,而是在返回的下一句。
*/
func test (){
	fmt.Printf("aaa\n")
}
func t (){
	fmt.Printf("ccc\n")
}
func main() {
	if (true){
		defer test()
	}
	t()
}
