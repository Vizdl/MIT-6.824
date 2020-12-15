在当前文件下,文件解释:

    raft.go : 
        raft一致性协议实现主体,几乎所有关键部分都在这一个单元内。
    persister.go : 
        持久化,raft服务器需要永久保存的数据放入到这里
        (在这里为了简化就没有放在磁盘内,但后期肯定是要放在磁盘内做持久化)。
    debug.go/release.go : 
        将raft日志输出的模块单独放在这两个文件内,
        使用标签来进行条件编译。不同时候输出不同日志。
    config.go : 
        MIT提供了一个利用管道实现的虚拟的网络,
        使得每个raft服务器可以直接通过虚拟的rpc调用其他raft服务器提供的rpc服务。
        该文件实现对模拟网络的配置。
    test_test.go : 
        测试模块。利用config.go提供的网络配置接口,
        来断开/重连 raft服务器,使raft服务器宕机/重启。
        以测试 raft.go 实现的容错机制,
        以及利用testing来实现对 raft 功能效率的测试。
    util.go : 
        目前未知。
    开发日志.txt :
        我在实现raft所作的思考与总结以及后期计划,
        在开发阶段可能有些乱,后期会进行整理。


关于raft的资料文档 :   
    
    raft lab文档(描述了如何启动与测试raft服务器以及每个阶段的任务与资料) :
         http://nil.csail.mit.edu/6.824/2020/labs/lab-raft.html

关于测试 :

    当前由于并没有部署真实的网络环境,
    而是采用了MIT提供的由管道实现的虚拟网络的rpc接口(便于测试)。
    故而目前只能通过 go testing 来进行测试。
    
    指令执行路径 : 就在raft文件夹也就是当前文件夹下
    带日志测试指令 : go test -run 2A -tags "debug"
    无日志测试指令 : go test -run 2A