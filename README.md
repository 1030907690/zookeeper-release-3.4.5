# zookeeper-release-3.4.5
zookeeper-release-3.4.5

1.下载ant-eclipse-1.0.bin.tar.bz2失败，将源码build.xml中的
get src="http://downloads.sourceforge.net/project/ant-eclipse/ant-eclipse/1.0/ant-eclipse-1.0.bin.tar.bz2" 替换成如下地址
get src="http://ufpr.dl.sourceforge.net/project/ant-eclipse/ant-eclipse/1.0/ant-eclipse-1.0.bin.tar.bz2"



# 改造成了maven项目  可直接运行 src/main/java/org/apache/zookeeper/server/quorum/QuorumPeerMain ,注意需要建data目录,conf里面目前配置的是集群环境

### zookeeper是什么
- zookeeper是一个数据库
- zookeeper是一个拥有文件系统的数据库
- zookeeper是一个解决了数据一致性问题的分布式数据库
- zookeeper是一个具有发布和订阅功能的分布式数据库(watch机制)


### CAP理论
- consistency 一致性
- available 可用性
- partition tolerance 分区容错性

### ZAB协议
- 领导选举
- 过半机制
- 预提交，收到ack，提交(两阶段提交)


### 领导选举发生的时机
- 集群启动
- leader挂掉
- ，不能对外提供服务了(领导者选举)follower挂掉后leader发现已经没有过半数的数量

