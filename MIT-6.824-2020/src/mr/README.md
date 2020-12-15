在当前文件下,文件解释:

marset.go : master的实现,主要负责分配和调度任务

worker.go : worker实现,主要负责根据不同的插件提供的map/reduce去处理master提供的文件路径(可以是分布式文件系统的,也可以是本地文件系统的)对应的文件。


rpc.go : rpc通信所用的结构体


mapreduce开发日志.txt : 我个人对MapReduce开发的思考与总结以及后期计划。

关于如何使用该 mapreduce计算框架(该路径下都是示例程序建议查看 wc.go) :
https://github.com/Vizdl/MIT-6.824/tree/master/MIT-6.824-2020/src/mrapps

写完mapreduce函数后,先进入 src/main/文件夹
cd src/main
构建 wc.go
go build -buildmode=plugin ../mrapps/wc.go
删除以往main文件夹下输出
rm mr-out*
在linux使用该指令运行 master(依然在main文件夹下)
go run mrmaster.go pg-*.txt
运行单个worker(使用几次指令,运行几个worker)
go run mrworker.go wc.so

当前仍未写一个脚本整合这些指令,之后可能会写一个。

关于go环境安装,mapreduce运行,详细请寻 : http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html