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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.common.AtomicFileOutputStream;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.jmx.ZKMBeanInfo;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.flexible.QuorumMaj;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the quorum protocol. There are three states this server
 * can be in:
 * <ol>
 * <li>Leader election - each server will elect a leader (proposing itself as a
 * leader initially).</li>
 * <li>Follower - the server will synchronize with the leader and replicate any
 * transactions.</li>
 * <li>Leader - the server will process requests and forward them to followers.
 * A majority of followers must log the request before it can be accepted.
 * </ol>
 *
 * This class will setup a datagram socket that will always respond with its
 * view of the current leader. The response will take the form of:
 *
 * <pre>
 * int xid;
 *
 * long myid;
 *
 * long leader_id;
 *
 * long leader_zxid;
 * </pre>
 *
 * The request for the current leader will consist solely of an xid: int xid;
 */
public class QuorumPeer extends Thread implements QuorumStats.Provider {
    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeer.class);

    QuorumBean jmxQuorumBean;
    LocalPeerBean jmxLocalPeerBean;
    LeaderElectionBean jmxLeaderElectionBean;
    QuorumCnxManager qcm;

    /* ZKDatabase is a top level member of quorumpeer 
     * which will be used in all the zookeeperservers
     * instantiated later. Also, it is created once on 
     * bootup and only thrown away in case of a truncate
     * message from the leader
     */
    private ZKDatabase zkDb;

    public static class QuorumServer {
        public QuorumServer(long id, InetSocketAddress addr,
                            InetSocketAddress electionAddr) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = electionAddr;
        }

        public QuorumServer(long id, InetSocketAddress addr) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = null;
        }

        public QuorumServer(long id, InetSocketAddress addr,
                            InetSocketAddress electionAddr, LearnerType type) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = electionAddr;
            this.type = type;
        }

        public InetSocketAddress addr;

        public InetSocketAddress electionAddr;

        public long id;

        public LearnerType type = LearnerType.PARTICIPANT;
    }

    public enum ServerState {
        LOOKING, FOLLOWING, LEADING, OBSERVING;
    }

    /*
     * A peer can either be participating, which implies that it is willing to
     * both vote in instances of consensus and to elect or become a Leader, or
     * it may be observing in which case it isn't.
     * 
     * We need this distinction to decide which ServerState to move to when 
     * conditions change (e.g. which state to become after LOOKING). 
     */
    public enum LearnerType {
        PARTICIPANT, OBSERVER;
    }
    
    /*
     * To enable observers to have no identifier, we need a generic identifier
     * at least for QuorumCnxManager. We use the following constant to as the
     * value of such a generic identifier. 
     */

    static final long OBSERVER_ID = Long.MAX_VALUE;

    /*
     * Record leader election time
     */
    public long start_fle, end_fle;

    /*
     * Default value of peer is participant
     */
    private LearnerType learnerType = LearnerType.PARTICIPANT;

    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * Sets the LearnerType both in the QuorumPeer and in the peerMap
     */
    public void setLearnerType(LearnerType p) {
        learnerType = p;
        if (quorumPeers.containsKey(this.myid)) {
            this.quorumPeers.get(myid).type = p;
        } else {
            LOG.error("Setting LearnerType to " + p + " but " + myid
                    + " not in QuorumPeers. ");
        }

    }

    /**
     * The servers that make up the cluster
     */
    protected Map<Long, QuorumServer> quorumPeers;

    public int getQuorumSize() {
        return getVotingView().size();
    }

    /**
     * QuorumVerifier implementation; default (majority). 
     */

    private QuorumVerifier quorumConfig;

    /**
     * My id
     */
    private long myid;


    /**
     * get the id of this quorum peer.
     */
    public long getId() {
        return myid;
    }

    /**
     * This is who I think the leader currently is.
     */
    volatile private Vote currentVote;

    public synchronized Vote getCurrentVote() {
        return currentVote;
    }

    public synchronized void setCurrentVote(Vote v) {
        currentVote = v;
    }

    volatile boolean running = true;

    /**
     * The number of milliseconds of each tick
     */
    protected int tickTime;

    /**
     * Minimum number of milliseconds to allow for session timeout.
     * A value of -1 indicates unset, use default.
     */
    protected int minSessionTimeout = -1;

    /**
     * Maximum number of milliseconds to allow for session timeout.
     * A value of -1 indicates unset, use default.
     */
    protected int maxSessionTimeout = -1;

    /**
     * The number of ticks that the initial synchronization phase can take
     */
    protected int initLimit;

    /**
     * The number of ticks that can pass between sending a request and getting
     * an acknowledgment
     */
    protected int syncLimit;

    /**
     * The current tick
     */
    protected int tick;

    /**
     * @deprecated As of release 3.4.0, this class has been deprecated, since
     * it is used with one of the udp-based versions of leader election, which
     * we are also deprecating. 
     *
     * This class simply responds to requests for the current leader of this
     * node.
     * <p>
     * The request contains just an xid generated by the requestor.
     * <p>
     * The response has the xid, the id of this server, the id of the leader,
     * and the zxid of the leader.
     *
     *
     */
    @Deprecated
    class ResponderThread extends Thread {
        ResponderThread() {
            super("ResponderThread");
        }

        volatile boolean running = true;

        @Override
        public void run() {
            try {
                byte b[] = new byte[36];
                ByteBuffer responseBuffer = ByteBuffer.wrap(b);
                DatagramPacket packet = new DatagramPacket(b, b.length);
                while (running) {
                    udpSocket.receive(packet);
                    if (packet.getLength() != 4) {
                        LOG.warn("Got more than just an xid! Len = "
                                + packet.getLength());
                    } else {
                        responseBuffer.clear();
                        responseBuffer.getInt(); // Skip the xid
                        responseBuffer.putLong(myid);
                        Vote current = getCurrentVote();
                        switch (getPeerState()) {
                            case LOOKING:
                                responseBuffer.putLong(current.getId());
                                responseBuffer.putLong(current.getZxid());
                                break;
                            case LEADING:
                                responseBuffer.putLong(myid);
                                try {
                                    long proposed;
                                    synchronized (leader) {
                                        proposed = leader.lastProposed;
                                    }
                                    responseBuffer.putLong(proposed);
                                } catch (NullPointerException npe) {
                                    // This can happen in state transitions,
                                    // just ignore the request
                                }
                                break;
                            case FOLLOWING:
                                responseBuffer.putLong(current.getId());
                                try {
                                    responseBuffer.putLong(follower.getZxid());
                                } catch (NullPointerException npe) {
                                    // This can happen in state transitions,
                                    // just ignore the request
                                }
                                break;
                            case OBSERVING:
                                // Do nothing, Observers keep themselves to
                                // themselves.
                                break;
                        }
                        packet.setData(b);
                        udpSocket.send(packet);
                    }
                    packet.setLength(b.length);
                }
            } catch (RuntimeException e) {
                LOG.warn("Unexpected runtime exception in ResponderThread", e);
            } catch (IOException e) {
                LOG.warn("Unexpected IO exception in ResponderThread", e);
            } finally {
                LOG.warn("QuorumPeer responder thread exited");
            }
        }
    }

    private ServerState state = ServerState.LOOKING;

    public synchronized void setPeerState(ServerState newState) {
        state = newState;
    }

    public synchronized ServerState getPeerState() {
        return state;
    }

    DatagramSocket udpSocket;

    private InetSocketAddress myQuorumAddr;

    public InetSocketAddress getQuorumAddress() {
        return myQuorumAddr;
    }

    private int electionType;

    Election electionAlg;

    ServerCnxnFactory cnxnFactory;
    private FileTxnSnapLog logFactory = null;

    private final QuorumStats quorumStats;

    public QuorumPeer() {
        super("QuorumPeer");
        quorumStats = new QuorumStats(this);
    }


    /**
     * For backward compatibility purposes, we instantiate QuorumMaj by default.
     */

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir,
                      File dataLogDir, int electionType,
                      long myid, int tickTime, int initLimit, int syncLimit,
                      ServerCnxnFactory cnxnFactory) throws IOException {
        this(quorumPeers, dataDir, dataLogDir, electionType, myid, tickTime,
                initLimit, syncLimit, cnxnFactory,
                new QuorumMaj(countParticipants(quorumPeers)));
    }

    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File dataDir,
                      File dataLogDir, int electionType,
                      long myid, int tickTime, int initLimit, int syncLimit,
                      ServerCnxnFactory cnxnFactory,
                      QuorumVerifier quorumConfig) throws IOException {
        this();
        this.cnxnFactory = cnxnFactory;
        this.quorumPeers = quorumPeers;
        this.electionType = electionType;
        this.myid = myid;
        this.tickTime = tickTime;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
        this.logFactory = new FileTxnSnapLog(dataLogDir, dataDir);
        this.zkDb = new ZKDatabase(this.logFactory);
        if (quorumConfig == null)
            this.quorumConfig = new QuorumMaj(countParticipants(quorumPeers));
        else this.quorumConfig = quorumConfig;
    }

    QuorumStats quorumStats() {
        return quorumStats;
    }

    /***
     start方法实现的业务主要包含四个方面：

     ​ 1、loadDataBase：涉及到的核心类是ZKDatabase，并借助于FileTxnSnapLog工具类将snap和transaction log反序列化到内存中，最终构建出内存数据结构DataTree

     ​ 2、cnxnFactory.start：之前介绍过ServerCnxnFactory作用，ServerCnxnFactory本身也可以作为一个线程，其run方法实现的大致逻辑是：
     构建reactor模型的EventLoop，Selector每隔1秒执行一次select方法来处理IO请求，并分发到对应的代表该客户端的ServerCnxn中并利用doIO进行处理。其核心代码我简化如下
     * */
    @Override
    public synchronized void start() {
        loadDataBase(); //从事务日志目录dataLogDir和数据快照目录dataDir中恢复出DataTree数据
        cnxnFactory.start();  //开启对客户端的连接端口,启动ServerCnxnFactory主线程
        startLeaderElection(); //创建出选举算法
        //QuorumPeer本身也是一个线程，其继承了Thread类，这里就是启动QuorumPeer线程，就是执行QuorumPeer.run方法
        super.start(); //启动QuorumPeer线程，在该线程中进行服务器状态的检查
    }

    public static void main(String[] args) {
        new QuorumPeer().test();
    }

    public void test() {
        super.start();
    }

    private void loadDataBase() {
        try {
            zkDb.loadDataBase();

            // load the epochs
            long lastProcessedZxid = zkDb.getDataTree().lastProcessedZxid;
            long epochOfZxid = ZxidUtils.getEpochFromZxid(lastProcessedZxid);
            try {
                currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
            } catch (FileNotFoundException e) {
                // pick a reasonable epoch number
                // this should only happen once when moving to a
                // new code version
                currentEpoch = epochOfZxid;
                LOG.info(CURRENT_EPOCH_FILENAME
                                + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation",
                        currentEpoch);
                writeLongToFile(CURRENT_EPOCH_FILENAME, currentEpoch);
            }
            if (epochOfZxid > currentEpoch) {
                throw new IOException("The current epoch, " + ZxidUtils.zxidToString(currentEpoch) + ", is older than the last zxid, " + lastProcessedZxid);
            }
            try {
                acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
            } catch (FileNotFoundException e) {
                // pick a reasonable epoch number
                // this should only happen once when moving to a
                // new code version
                acceptedEpoch = epochOfZxid;
                LOG.info(ACCEPTED_EPOCH_FILENAME
                                + " not found! Creating with a reasonable default of {}. This should only happen when you are upgrading your installation",
                        acceptedEpoch);
                writeLongToFile(CURRENT_EPOCH_FILENAME, acceptedEpoch);
            }
            if (acceptedEpoch < currentEpoch) {
                throw new IOException("The current epoch, " + ZxidUtils.zxidToString(currentEpoch) + " is less than the accepted epoch, " + ZxidUtils.zxidToString(acceptedEpoch));
            }
        } catch (IOException ie) {
            LOG.error("Unable to load database on disk", ie);
            throw new RuntimeException("Unable to run quorum server ", ie);
        }
    }

    ResponderThread responder;

    synchronized public void stopLeaderElection() {
        responder.running = false;
        responder.interrupt();
    }

    /*
    ​ 3、startLeaderElection()：这个主要是初始化一些Leader选举工作，这部分的关键代码在QuorumPeer.createElectionAlgorithm，大致如下：
    * */
    synchronized public void startLeaderElection() {
        try {
            currentVote = new Vote(myid, getLastLoggedZxid(), getCurrentEpoch());
        } catch (IOException e) {
            RuntimeException re = new RuntimeException(e.getMessage());
            re.setStackTrace(e.getStackTrace());
            throw re;
        }
        for (QuorumServer p : getView().values()) {
            if (p.id == myid) {
                myQuorumAddr = p.addr;
                break;
            }
        }
        if (myQuorumAddr == null) {
            throw new RuntimeException("My id " + myid + " not in the peer list");
        }
        if (electionType == 0) {
            try {
                udpSocket = new DatagramSocket(myQuorumAddr.getPort());
                responder = new ResponderThread();
                responder.start();
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
        this.electionAlg = createElectionAlgorithm(electionType);
    }

    /**
     * Count the number of nodes in the map that could be followers.
     * @param peers
     * @return The number of followers in the map
     */
    protected static int countParticipants(Map<Long, QuorumServer> peers) {
        int count = 0;
        for (QuorumServer q : peers.values()) {
            if (q.type == LearnerType.PARTICIPANT) {
                count++;
            }
        }
        return count;
    }

    /**
     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File snapDir,
                      File logDir, int clientPort, int electionAlg,
                      long myid, int tickTime, int initLimit, int syncLimit)
            throws IOException {
        this(quorumPeers, snapDir, logDir, electionAlg,
                myid, tickTime, initLimit, syncLimit,
                ServerCnxnFactory.createFactory(new InetSocketAddress(clientPort), -1),
                new QuorumMaj(countParticipants(quorumPeers)));
    }

    /**
     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(Map<Long, QuorumServer> quorumPeers, File snapDir,
                      File logDir, int clientPort, int electionAlg,
                      long myid, int tickTime, int initLimit, int syncLimit,
                      QuorumVerifier quorumConfig)
            throws IOException {
        this(quorumPeers, snapDir, logDir, electionAlg,
                myid, tickTime, initLimit, syncLimit,
                ServerCnxnFactory.createFactory(new InetSocketAddress(clientPort), -1),
                quorumConfig);
    }

    /**
     * returns the highest zxid that this host has seen
     *
     * @return the highest zxid for this host
     */
    public long getLastLoggedZxid() {
        if (!zkDb.isInitialized()) {
            loadDataBase();
        }
        return zkDb.getDataTreeLastProcessedZxid();
    }

    public Follower follower;
    public Leader leader;
    public Observer observer;

    protected Follower makeFollower(FileTxnSnapLog logFactory) throws IOException {
        return new Follower(this, new FollowerZooKeeperServer(logFactory,
                this, new ZooKeeperServer.BasicDataTreeBuilder(), this.zkDb));
    }

    protected Leader makeLeader(FileTxnSnapLog logFactory) throws IOException {
        return new Leader(this, new LeaderZooKeeperServer(logFactory,
                this, new ZooKeeperServer.BasicDataTreeBuilder(), this.zkDb));
    }

    protected Observer makeObserver(FileTxnSnapLog logFactory) throws IOException {
        return new Observer(this, new ObserverZooKeeperServer(logFactory,
                this, new ZooKeeperServer.BasicDataTreeBuilder(), this.zkDb));
    }

    protected Election createElectionAlgorithm(int electionAlgorithm) {
        Election le = null;

        //TODO: use a factory rather than a switch
        switch (electionAlgorithm) {
            case 0:
                le = new LeaderElection(this);
                break;
            case 1:
                le = new AuthFastLeaderElection(this);
                break;
            case 2:
                le = new AuthFastLeaderElection(this, true);
                break;
            case 3:
                qcm = new QuorumCnxManager(this); //创建一个QuorumCnxManager实例
                QuorumCnxManager.Listener listener = qcm.listener;
                if (listener != null) {
                    listener.start(); //Listener是一个线程，这里启动Listener线程，主要启动选举监听端口并处理连接进来的Socket
                    le = new FastLeaderElection(this, qcm); //选举算法使用FastLeaderElection，存在好几种算法实现，但是其它集中算法实现都已经慢慢废弃
                } else {
                    LOG.error("Null listener when initializing cnx manager");
                }
                break;
            default:
                assert false;
        }
        return le;
    }

    protected Election makeLEStrategy() {
        LOG.debug("Initializing leader election protocol...");
        if (getElectionType() == 0) {
            electionAlg = new LeaderElection(this);
        }
        return electionAlg;
    }

    synchronized protected void setLeader(Leader newLeader) {
        leader = newLeader;
    }

    synchronized protected void setFollower(Follower newFollower) {
        follower = newFollower;
    }

    synchronized protected void setObserver(Observer newObserver) {
        observer = newObserver;
    }

    synchronized public ZooKeeperServer getActiveServer() {
        if (leader != null)
            return leader.zk;
        else if (follower != null)
            return follower.zk;
        else if (observer != null)
            return observer.zk;
        return null;
    }

    /*
    QuorumPeer线程进入到一个无限循环模式，不停的通过getPeerState方法获取当前节点状态，然后执行相应的分支逻辑。大致流程可以简单描述如下：

​ a.首先系统刚启动时serverState默认是LOOKING，表示需要进行Leader选举，这时进入Leader选举状态中，会调用FastLeaderElection.lookForLeader方法，
    lookForLeader方法内部也包含了一个循环逻辑，直到选举出Leader才会跳出lookForLeader方法，如果选举出的Leader就是本节点，则将serverState=LEADING赋值，
    否则设置成FOLLOWING或OBSERVING

​ b.然后，QuorumPeer.run进行下一轮次循环，通过getPeerState获取当前serverState状态，如果是LEADING，则表示当前节点当选为LEADER，则进入Leader角色分支流程，
   执行作为一个Leader该干的任务；如果是FOLLOWING或OBSERVING，则进入Follower或Observer角色，并执行其相应的任务。注意：进入分支路程会一直阻塞在其分支中，
   直到角色转变才会重新进行下一轮次循环，比如Follower监控到无法与Leader保持通信了，会将serverState赋值为LOOKING，跳出分支并进行下一轮次循环，这时就会进入LOOKING分支中重新进行Leader选举
    * */
    @Override
    public void run() {
        setName("QuorumPeer" + "[myid=" + getId() + "]" +
                cnxnFactory.getLocalAddress());

        LOG.debug("Starting quorum peer");
        try {
            jmxQuorumBean = new QuorumBean(this);
            MBeanRegistry.getInstance().register(jmxQuorumBean, null);
            for (QuorumServer s : getView().values()) {
                ZKMBeanInfo p;
                if (getId() == s.id) {
                    p = jmxLocalPeerBean = new LocalPeerBean(this);
                    try {
                        MBeanRegistry.getInstance().register(p, jmxQuorumBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                        jmxLocalPeerBean = null;
                    }
                } else {
                    p = new RemotePeerBean(s);
                    try {
                        MBeanRegistry.getInstance().register(p, jmxQuorumBean);
                    } catch (Exception e) {
                        LOG.warn("Failed to register with JMX", e);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxQuorumBean = null;
        }

        try {
            /*
             * Main loop
             */
            //获取serverState状态后进入不同分支，当分支退出后继续下次循环
            while (running) {

                /***
                 * 节点确定了自己的角色后，就会进入自己的角色分支：对于Leader而言创建Leader实例并调用其lead()函数，对于Follower而言创建Follower实例并调用其followLeader()函数，
                 * 对于Observer而言创建Observer实例并调用其observeLeader()函数。在这三个函数中，服务器会进行相关的初始化并完成最终的启动。
                 *
                 * 对于Follower和Observer而言，主要的初始化工作是要建立与Leader的连接并同步epoch信息，最后完成与Leader的数据同步。而Leader会启动LearnerCnxAcceptor线程，
                 * 该线程会接受来自Follower和Observer（统称为Learner）的连接请求并为每个连接创建一个LearnerHandler线程，该线程会负责包括数据同步在内的与learner的一切通信。
                 *
                 *
                 * Learn(Follower或Observer)节点会主动向Leader发起连接，Zookeeper就会进入集群同步阶段，集群同步主要完成集群中各节点状态信息和数据信息的一致。
                 * 选出新的Leader后的流程大致分为：计算epoch、统一epoch、同步数据、广播模式等四个阶段。其中其前三个阶段：计算epoch、统一epoch、
                 * 同步数据就是这一节主要介绍的集群同步阶段的主要内容，这三个阶段主要完成新Leader与集群中的节点完成同步工作，处于这个阶段的zk集群还没有真正做好对外提供服务的能力，
                 * 可以看着是新leader上任后进行的内部沟通、前期准备工作等，只有等这三个阶段全部完成，新leader才会真正的成为leader，这时zk集群会恢复正常可运行状态并对外提供服务。
                 * */
                switch (getPeerState()) {
                    case LOOKING:
                        //当前节点进入Leader选举状态，执行选举分支流程
                        LOG.info("LOOKING");
                        //只读模式已启用
                        if (Boolean.getBoolean("readonlymode.enabled")) {
                            LOG.info("Attempting to start ReadOnlyZooKeeperServer");

                            // Create read-only server but don't start it immediately
                            final ReadOnlyZooKeeperServer roZk = new ReadOnlyZooKeeperServer(
                                    logFactory, this,
                                    new ZooKeeperServer.BasicDataTreeBuilder(),
                                    this.zkDb);

                            // Instead of starting roZk immediately, wait some grace
                            // period before we decide we're partitioned.
                            //
                            // Thread is used here because otherwise it would require
                            // changes in each of election strategy classes which is
                            // unnecessary code coupling.
                            Thread roZkMgr = new Thread() {
                                @Override
                                public void run() {
                                    try {
                                        // lower-bound grace period to 2 secs
                                        sleep(Math.max(2000, tickTime));
                                        if (ServerState.LOOKING.equals(getPeerState())) {
                                            roZk.startup();
                                        }
                                    } catch (InterruptedException e) {
                                        LOG.info("Interrupted while attempting to start ReadOnlyZooKeeperServer, not started");
                                    } catch (Exception e) {
                                        LOG.error("FAILED to start ReadOnlyZooKeeperServer", e);
                                    }
                                }
                            };
                            try {
                                roZkMgr.start();
                                setCurrentVote(makeLEStrategy().lookForLeader());
                            } catch (Exception e) {
                                LOG.warn("Unexpected exception", e);
                                setPeerState(ServerState.LOOKING);
                            } finally {
                                // If the thread is in the the grace period, interrupt
                                // to come out of waiting.
                                roZkMgr.interrupt();
                                roZk.shutdown();
                            }
                        } else {
                            try {
                                //选举
                                setCurrentVote(makeLEStrategy().lookForLeader()); //调用FastLeaderElection.lookForLeader()
                            } catch (Exception e) {
                                LOG.warn("Unexpected exception", e);
                                setPeerState(ServerState.LOOKING);
                            }
                        }
                        break;
                    case OBSERVING:
                        //当前节点成为Observer角色时执行分支流程
                        try {
                            LOG.info("OBSERVING");
                            setObserver(makeObserver(logFactory));
                            observer.observeLeader();
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            observer.shutdown();
                            setObserver(null);
                            setPeerState(ServerState.LOOKING);
                        }
                        break;
                    case FOLLOWING:
                        //当前节点成为Follower角色时执行分支流程
                        try {
                            LOG.info("FOLLOWING");
                            setFollower(makeFollower(logFactory));
                            follower.followLeader();
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            follower.shutdown();
                            setFollower(null);
                            setPeerState(ServerState.LOOKING);
                        }
                        break;
                    case LEADING:
                        //当前节点成为Leader角色时执行分支流程
                        LOG.info("LEADING");
                        try {
                            //被选举为Leader角色的节点，会创建一个Leader实例，然后执行Leader.lead()进入到Leader角色的任务分支中
                            setLeader(makeLeader(logFactory));
                            leader.lead();
                            setLeader(null);
                        } catch (Exception e) {
                            LOG.warn("Unexpected exception", e);
                        } finally {
                            if (leader != null) {
                                leader.shutdown("Forcing shutdown");
                                setLeader(null);
                            }
                            setPeerState(ServerState.LOOKING);
                        }
                        break;
                }
            }
        } finally {
            LOG.warn("QuorumPeer main thread exited");
            try {
                MBeanRegistry.getInstance().unregisterAll();
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            jmxQuorumBean = null;
            jmxLocalPeerBean = null;
        }
    }

    public void shutdown() {
        running = false;
        if (leader != null) {
            leader.shutdown("quorum Peer shutdown");
        }
        if (follower != null) {
            follower.shutdown();
        }
        cnxnFactory.shutdown();
        if (udpSocket != null) {
            udpSocket.close();
        }

        if (getElectionAlg() != null) {
            this.interrupt();
            getElectionAlg().shutdown();
        }
        try {
            zkDb.close();
        } catch (IOException ie) {
            LOG.warn("Error closing logs ", ie);
        }
    }

    /**
     * A 'view' is a node's current opinion of the membership of the entire
     * ensemble.    
     */
    public Map<Long, QuorumPeer.QuorumServer> getView() {
        return Collections.unmodifiableMap(this.quorumPeers);
    }

    /**
     * Observers are not contained in this view, only nodes with 
     * PeerType=PARTICIPANT.     
     */
    public Map<Long, QuorumPeer.QuorumServer> getVotingView() {
        Map<Long, QuorumPeer.QuorumServer> ret =
                new HashMap<Long, QuorumPeer.QuorumServer>();
        Map<Long, QuorumPeer.QuorumServer> view = getView();
        for (QuorumServer server : view.values()) {
            if (server.type == LearnerType.PARTICIPANT) {
                ret.put(server.id, server);
            }
        }
        return ret;
    }

    /**
     * Returns only observers, no followers.
     */
    public Map<Long, QuorumPeer.QuorumServer> getObservingView() {
        Map<Long, QuorumPeer.QuorumServer> ret =
                new HashMap<Long, QuorumPeer.QuorumServer>();
        Map<Long, QuorumPeer.QuorumServer> view = getView();
        for (QuorumServer server : view.values()) {
            if (server.type == LearnerType.OBSERVER) {
                ret.put(server.id, server);
            }
        }
        return ret;
    }

    /**
     * Check if a node is in the current view. With static membership, the
     * result of this check will never change; only when dynamic membership
     * is introduced will this be more useful.
     */
    public boolean viewContains(Long sid) {
        return this.quorumPeers.containsKey(sid);
    }

    /**
     * Only used by QuorumStats at the moment
     */
    @Override
    public String[] getQuorumPeers() {
        List<String> l = new ArrayList<String>();
        synchronized (this) {
            if (leader != null) {
                for (LearnerHandler fh : leader.getLearners()) {
                    if (fh.getSocket() != null) {
                        String s = fh.getSocket().getRemoteSocketAddress().toString();
                        if (leader.isLearnerSynced(fh))
                            s += "*";
                        l.add(s);
                    }
                }
            } else if (follower != null) {
                l.add(follower.sock.getRemoteSocketAddress().toString());
            }
        }
        return l.toArray(new String[0]);
    }

    public String getServerState() {
        switch (getPeerState()) {
            case LOOKING:
                return QuorumStats.Provider.LOOKING_STATE;
            case LEADING:
                return QuorumStats.Provider.LEADING_STATE;
            case FOLLOWING:
                return QuorumStats.Provider.FOLLOWING_STATE;
            case OBSERVING:
                return QuorumStats.Provider.OBSERVING_STATE;
        }
        return QuorumStats.Provider.UNKNOWN_STATE;
    }


    /**
     * get the id of this quorum peer.
     */
    public long getMyid() {
        return myid;
    }

    /**
     * set the id of this quorum peer.
     */
    public void setMyid(long myid) {
        this.myid = myid;
    }

    /**
     * Get the number of milliseconds of each tick
     */
    public int getTickTime() {
        return tickTime;
    }

    /**
     * Set the number of milliseconds of each tick
     */
    public void setTickTime(int tickTime) {
        LOG.info("tickTime set to " + tickTime);
        this.tickTime = tickTime;
    }

    /** Maximum number of connections allowed from particular host (ip) */
    public int getMaxClientCnxnsPerHost() {
        ServerCnxnFactory fac = getCnxnFactory();
        if (fac == null) {
            return -1;
        }
        return fac.getMaxClientCnxnsPerHost();
    }

    /** minimum session timeout in milliseconds */
    public int getMinSessionTimeout() {
        return minSessionTimeout == -1 ? tickTime * 2 : minSessionTimeout;
    }

    /** minimum session timeout in milliseconds */
    public void setMinSessionTimeout(int min) {
        LOG.info("minSessionTimeout set to " + min);
        this.minSessionTimeout = min;
    }

    /** maximum session timeout in milliseconds */
    public int getMaxSessionTimeout() {
        return maxSessionTimeout == -1 ? tickTime * 20 : maxSessionTimeout;
    }

    /** minimum session timeout in milliseconds */
    public void setMaxSessionTimeout(int max) {
        LOG.info("maxSessionTimeout set to " + max);
        this.maxSessionTimeout = max;
    }

    /**
     * Get the number of ticks that the initial synchronization phase can take
     */
    public int getInitLimit() {
        return initLimit;
    }

    /**
     * Set the number of ticks that the initial synchronization phase can take
     */
    public void setInitLimit(int initLimit) {
        LOG.info("initLimit set to " + initLimit);
        this.initLimit = initLimit;
    }

    /**
     * Get the current tick
     */
    public int getTick() {
        return tick;
    }

    /**
     * Return QuorumVerifier object
     */

    public QuorumVerifier getQuorumVerifier() {
        return quorumConfig;

    }

    public void setQuorumVerifier(QuorumVerifier quorumConfig) {
        this.quorumConfig = quorumConfig;
    }

    /**
     * Get an instance of LeaderElection
     */

    public Election getElectionAlg() {
        return electionAlg;
    }

    /**
     * Get the synclimit
     */
    public int getSyncLimit() {
        return syncLimit;
    }

    /**
     * Set the synclimit
     */
    public void setSyncLimit(int syncLimit) {
        this.syncLimit = syncLimit;
    }

    /**
     * Gets the election type
     */
    public int getElectionType() {
        return electionType;
    }

    /**
     * Sets the election type
     */
    public void setElectionType(int electionType) {
        this.electionType = electionType;
    }

    public ServerCnxnFactory getCnxnFactory() {
        return cnxnFactory;
    }

    public void setCnxnFactory(ServerCnxnFactory cnxnFactory) {
        this.cnxnFactory = cnxnFactory;
    }

    public void setQuorumPeers(Map<Long, QuorumServer> quorumPeers) {
        this.quorumPeers = quorumPeers;
    }

    public int getClientPort() {
        return cnxnFactory.getLocalPort();
    }

    public void setClientPortAddress(InetSocketAddress addr) {
    }

    public void setTxnFactory(FileTxnSnapLog factory) {
        this.logFactory = factory;
    }

    public FileTxnSnapLog getTxnFactory() {
        return this.logFactory;
    }

    /**
     * set zk database for this node
     * @param database
     */
    public void setZKDatabase(ZKDatabase database) {
        this.zkDb = database;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * get reference to QuorumCnxManager
     */
    public QuorumCnxManager getQuorumCnxManager() {
        return qcm;
    }

    private long readLongFromFile(String name) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = "";
        try {
            line = br.readLine();
            return Long.parseLong(line);
        } catch (NumberFormatException e) {
            throw new IOException("Found " + line + " in " + file);
        } finally {
            br.close();
        }
    }

    private long acceptedEpoch = -1;
    private long currentEpoch = -1;

    public static final String CURRENT_EPOCH_FILENAME = "currentEpoch";

    public static final String ACCEPTED_EPOCH_FILENAME = "acceptedEpoch";

    /**
     * Write a long value to disk atomically. Either succeeds or an exception
     * is thrown.
     * @param name file name to write the long to
     * @param value the long value to write to the named file
     * @throws IOException if the file cannot be written atomically
     */
    private void writeLongToFile(String name, long value) throws IOException {
        File file = new File(logFactory.getSnapDir(), name);
        AtomicFileOutputStream out = new AtomicFileOutputStream(file);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        boolean aborted = false;
        try {
            bw.write(Long.toString(value));
            bw.flush();

            out.flush();
        } catch (IOException e) {
            LOG.error("Failed to write new file " + file, e);
            // worst case here the tmp file/resources(fd) are not cleaned up
            //   and the caller will be notified (IOException)
            aborted = true;
            out.abort();
            throw e;
        } finally {
            if (!aborted) {
                // if the close operation (rename) fails we'll get notified.
                // worst case the tmp file may still exist
                out.close();
            }
        }
    }

    public long getCurrentEpoch() throws IOException {
        if (currentEpoch == -1) {
            currentEpoch = readLongFromFile(CURRENT_EPOCH_FILENAME);
        }
        return currentEpoch;
    }

    public long getAcceptedEpoch() throws IOException {
        if (acceptedEpoch == -1) {
            acceptedEpoch = readLongFromFile(ACCEPTED_EPOCH_FILENAME);
        }
        return acceptedEpoch;
    }

    public void setCurrentEpoch(long e) throws IOException {
        currentEpoch = e;
        writeLongToFile(CURRENT_EPOCH_FILENAME, e);

    }

    public void setAcceptedEpoch(long e) throws IOException {
        acceptedEpoch = e;
        writeLongToFile(ACCEPTED_EPOCH_FILENAME, e);
    }
}
