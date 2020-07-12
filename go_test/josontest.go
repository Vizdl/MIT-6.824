package main

import (
    "encoding/json"
    "fmt"
    "os"
)

type PersonInfo struct {
    Name    string
    age     int32
    Sex     bool
    Hobbies []string
}

func main() {
    writeFile()
    readFile()
}

func readFile() {

    filePtr, err := os.Open("person_info.json")
    if err != nil {
        fmt.Println("Open file failed [Err:%s]", err.Error())
        return
    }
    defer filePtr.Close()

    var person []PersonInfo

    // 创建json解码器
    decoder := json.NewDecoder(filePtr)
    err = decoder.Decode(&person)
    fmt.Printf("json读入type:%T\n", person)
    if err != nil {
        fmt.Println("Decoder failed", err.Error())

    } else {
        fmt.Println("Decoder success")
        fmt.Println(person)
    }
}
func writeFile() {
    personInfo := []PersonInfo{{"David", 30, true, []string{"跑步", "读书", "看电影"}}, {"Lee", 27, false, []string{"工作", "读书", "看电影"}}}

    // 创建文件
    filePtr, err := os.Create("person_info.json")
    if err != nil {
        fmt.Println("Create file failed", err.Error())
        return
    }
    defer filePtr.Close()

    // 创建Json编码器
    encoder := json.NewEncoder(filePtr)
    fmt.Printf("encoder type:%T\n", encoder)
    err = encoder.Encode(personInfo)
    
    fmt.Printf("json读入type:%T\n", personInfo)
    if err != nil {
        fmt.Println("Encoder failed", err.Error())

    } else {
        fmt.Println("Encoder success")
    }
   // 带JSON缩进格式写文件　　//data, err := json.MarshalIndent(personInfo, "", "  ")   //if err != nil {   // fmt.Println("Encoder failed", err.Error())   //   //} else {   // fmt.Println("Encoder success")   //}   //   //filePtr.Write(data)
}