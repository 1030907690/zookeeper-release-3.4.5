# zookeeper-release-3.4.5
zookeeper-release-3.4.5

1.下载ant-eclipse-1.0.bin.tar.bz2失败，将源码build.xml中的
get src="http://downloads.sourceforge.net/project/ant-eclipse/ant-eclipse/1.0/ant-eclipse-1.0.bin.tar.bz2" 替换成如下地址
get src="http://ufpr.dl.sourceforge.net/project/ant-eclipse/ant-eclipse/1.0/ant-eclipse-1.0.bin.tar.bz2"



# 改造成了maven项目  可直接运行 src/main/java/org/apache/zookeeper/server/quorum/QuorumPeerMain ,注意需要建data目录,conf里面目前配置的是集群环境