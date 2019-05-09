/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum;

import java.io.File;
import java.io.IOException;

import javax.management.JMException;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
//https://blog.reactor.top/2018/03/15/Zookeeper%E6%BA%90%E7%A0%81-%E6%80%BB%E4%BD%93%E6%B5%81%E7%A8%8B%E6%A6%82%E8%A7%88/
public class QuorumPeerMain {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    protected QuorumPeer quorumPeer;

    /**
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile
     */
    //1.Zookeeper启动类是QuorumPeerMain，并将配置文件通过args参数方式传入
    public static void main(String[] args) {
        String configPathPrefix = "F:\\work\\zookeeper-release-3.4.5\\conf\\";
        //为了方便debug 增加cfg路径 2019年5月6日11:47:08
        if (args.length < 1) {
            args = new String[1];
            args[0] = configPathPrefix + "zoo_sample.cfg";

            //自定义log4j.properties的加载位置 2019年5月6日11:58:07
            PropertyConfigurator.configure(configPathPrefix + "log4j.properties");

        }



        QuorumPeerMain main = new QuorumPeerMain();
        try {
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            System.exit(2);
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            System.exit(2);
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            System.exit(1);
        }
        LOG.info("Exiting normally");
        System.exit(0);
    }

    //启动并且运行
    protected void initializeAndRun(String[] args)
            throws ConfigException, IOException {
        //2.然后将传入的配置文件进行解析获取到QuorumPeerConfig配置类
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            //解析配置文件
            config.parse(args[0]);
        }

        // Start and schedule the the purge task
        /*3、然后启动DatadirCleanupManager线程，由于Zookeeper的任何一个变更操作(增、删、改)都将在transaction log中进行记录，
        因为内存中的数据掉电后会丢失，必须写入到硬盘上的transaction log中；当写操作达到一定量或者一定时间间隔后，会对内存中的数据进行一次快照并写入到硬盘上的snap log中，
        主要为了缩短启动时加载数据的时间从而加快系统启动，另一方面避免transaction log日志数量过度膨胀。随着运行时间的增长生成的transaction log和snapshot将越来越多，
        所以要定期清理，DatadirCleanupManager就是启动一个TimeTask定时任务用于清理DataDir中的snapshot及对应的transaction log*/
        /*
        DatadirCleanupManager主要有两个参数：
            snapRetainCount:清理后保留的snapshot的个数,对应配置:autopurge.snapRetainCount,大于等于3,默认3
            purgeInterval:清理任务TimeTask执行周期,即几个小时清理一次,对应配置:autopurge.purgeInterval,单位:小时
        * */
        //数据目录清理管理器
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(config
                .getDataDir(), config.getDataLogDir(), config
                .getSnapRetainCount(), config.getPurgeInterval());
        purgeMgr.start();

        /***
         4、根据配置中的servers数量判断是集群环境还是单机环境，如果单机环境以standalone模式运行直接调用ZooKeeperServerMain.main()方法，
         这里就不细说，生产环境下主要是利用Zookeeper的集群环境，下面也主要是分析Zookeeper的集群环境下运行流程
         * */
        if (args.length == 1 && config.servers.size() > 0) {
            //根据配置运行
            runFromConfig(config);
        } else {
            LOG.warn("Either no config or no quorum defined in config, running "
                    + " in standalone mode");
            // there is only server in the quorum -- run as standalone
            //单机环境运行
            ZooKeeperServerMain.main(args);
        }
    }


    public void runFromConfig(QuorumPeerConfig config) throws IOException {
        try {
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        LOG.info("Starting quorum peer");
        try {
            /*
             5、创建ServerCnxnFactory实例，ServerCnxnFactory从名字就可以看出其是一个工厂类，负责管理ServerCnxn，
             ServerCnxn这个类代表了一个客户端与一个server的连接，每个客户端连接过来都会被封装成一个ServerCnxn实例用来维护了服务器与客户端之间的Socket通道。
             首先要有监听端口，客户端连接才能过来，ServerCnxnFactory.configure()方法的核心就是启动监听端口供客户端连接进来，端口号由配置文件中clientPort属性进行配置，
             默认是2181
            * */
            ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
            cnxnFactory.configure(config.getClientPortAddress(),
                    config.getMaxClientCnxns());

            /*
            6、初始化QuorumPeer，Quorum在Zookeeper中代表集群中大多数节点的意思，即一半以上节点，Peer是端、节点的意思，
            Zookeeper集群中一半以上的节点其实就可以代表整个集群的状态，QuorumPeer就是管理维护的整个集群的一个核心类，
            这一步主要是创建一个QuorumPeer实例，并进行各种初始化工作，大致代码如下
            * */
            quorumPeer = new QuorumPeer(); //创建QuorumPeer实例
            quorumPeer.setClientPortAddress(config.getClientPortAddress());
            quorumPeer.setTxnFactory(new FileTxnSnapLog(  //FileTxnSnapLog主要用于snap和transaction log的IO工具类
                    new File(config.getDataLogDir()),
                    new File(config.getDataDir())));
            // 设置参与竞选的所有服务器
            quorumPeer.setQuorumPeers(config.getServers());
            quorumPeer.setElectionType(config.getElectionAlg()); //选举类型，用于确定选举算法
            quorumPeer.setMyid(config.getServerId()); //myid用于区分不同端
            // 设置心跳时间
            quorumPeer.setTickTime(config.getTickTime());
            // 设置最小Session过期时间，默认是2*ticket
            quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
            // 设置最大Session过期时间，默认是20*ticket
            quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            // 设置允许follower同步和连接到leader的时间总量，以ticket为单位
            quorumPeer.setInitLimit(config.getInitLimit());
            // 设置follower与leader之间同步的时间量
            quorumPeer.setSyncLimit(config.getSyncLimit());
            // 设置集群数量验证器，默认为半数原则
            quorumPeer.setQuorumVerifier(config.getQuorumVerifier());
            // 设置工厂，默认是NIO工厂
            quorumPeer.setCnxnFactory(cnxnFactory); //ServerCnxnFactory客户端请求管理工厂类
            quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));  //ZKDatabase维护ZK在内存中的数据结构
            quorumPeer.setLearnerType(config.getPeerType());

            /*
            ​ 7、QuorumPeer初始化完成后执行
            * */
            quorumPeer.start();
            quorumPeer.join();
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Quorum Peer interrupted", e);
        }
    }
}
