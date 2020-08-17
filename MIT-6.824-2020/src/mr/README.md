在当前文件下,文件解释:

marset.go : master的实现,主要负责分配和调度任务

worker.go : worker实现,主要负责根据不同的插件提供的map/reduce去处理master提供的文件路径(可以是分布式文件系统的,也可以是本地文件系统的)对应的文件。


rpc.go : rpc通信所用的结构体


mapreduce开发日志.txt : 我个人对MapReduce开发的思考与总结以及后期计划。