package main
import "fmt"
func main() {
	s := "*.go"
	strs := s[0:]
	fmt.Println(s)
	fmt.Println(len(s))
	fmt.Println(strs[0])
	fmt.Println(strs[1])
}
