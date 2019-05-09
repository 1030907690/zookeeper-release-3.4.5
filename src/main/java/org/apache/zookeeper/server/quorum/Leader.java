/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has the control logic for the Leader.
 */
public class Leader {
    private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
    
    static final private boolean nodelay = System.getProperty("leader.nodelay", "true").equals("true");
    static {
        LOG.info("TCP NoDelay set to: " + nodelay);
    }

    static public class Proposal {
        public QuorumPacket packet;

        public HashSet<Long> ackSet = new HashSet<Long>();

        public Request request;

        @Override
        public String toString() {
            return packet.getType() + ", " + packet.getZxid() + ", " + request;
        }
    }

    final LeaderZooKeeperServer zk;

    final QuorumPeer self;

    // the follower acceptor thread
    LearnerCnxAcceptor cnxAcceptor;
    
    // list of all the followers
    private final HashSet<LearnerHandler> learners =
        new HashSet<LearnerHandler>();

    /**
     * Returns a copy of the current learner snapshot
     */
    public List<LearnerHandler> getLearners() {
        synchronized (learners) {
            return new ArrayList<LearnerHandler>(learners);
        }
    }

    // list of followers that are ready to follow (i.e synced with the leader)
    private final HashSet<LearnerHandler> forwardingFollowers =
        new HashSet<LearnerHandler>();
    
    /**
     * Returns a copy of the current forwarding follower snapshot
     */
    public List<LearnerHandler> getForwardingFollowers() {
        synchronized (forwardingFollowers) {
            return new ArrayList<LearnerHandler>(forwardingFollowers);
        }
    }

    private void addForwardingFollower(LearnerHandler lh) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.add(lh);
        }
    }

    private final HashSet<LearnerHandler> observingLearners =
        new HashSet<LearnerHandler>();
        
    /**
     * Returns a copy of the current observer snapshot
     */
    public List<LearnerHandler> getObservingLearners() {
        synchronized (observingLearners) {
            return new ArrayList<LearnerHandler>(observingLearners);
        }
    }

    private void addObserverLearnerHandler(LearnerHandler lh) {
        synchronized (observingLearners) {
            observingLearners.add(lh);
        }
    }

    // Pending sync requests. Must access under 'this' lock.
    private final HashMap<Long,List<LearnerSyncRequest>> pendingSyncs =
        new HashMap<Long,List<LearnerSyncRequest>>();
    
    synchronized public int getNumPendingSyncs() {
        return pendingSyncs.size();
    }

    //Follower counter
    final AtomicLong followerCounter = new AtomicLong(-1);

    /**
     * Adds peer to the leader.
     * 
     * @param learner
     *                instance of learner handle
     */
    void addLearnerHandler(LearnerHandler learner) {
        synchronized (learners) {
            learners.add(learner);
        }
    }

    /**
     * Remove the learner from the learner list
     * 
     * @param peer
     */
    void removeLearnerHandler(LearnerHandler peer) {
        synchronized (forwardingFollowers) {
            forwardingFollowers.remove(peer);            
        }        
        synchronized (learners) {
            learners.remove(peer);
        }
        synchronized (observingLearners) {
            observingLearners.remove(peer);
        }
    }

    boolean isLearnerSynced(LearnerHandler peer){
        synchronized (forwardingFollowers) {
            return forwardingFollowers.contains(peer);
        }        
    }
    
    ServerSocket ss;

    Leader(QuorumPeer self,LeaderZooKeeperServer zk) throws IOException {
        this.self = self;
        try {
            ss = new ServerSocket();
            ss.setReuseAddress(true);
            ss.bind(new InetSocketAddress(self.getQuorumAddress().getPort()));
        } catch (BindException e) {
            LOG.error("Couldn't bind to port "
                    + self.getQuorumAddress().getPort(), e);
            throw e;
        }
        this.zk=zk;
    }

    /**
     * This message is for follower to expect diff
     */
    final static int DIFF = 13;
    
    /**
     * This is for follower to truncate its logs 
     */
    final static int TRUNC = 14;
    
    /**
     * This is for follower to download the snapshots
     */
    final static int SNAP = 15;
    
    /**
     * This tells the leader that the connecting peer is actually an observer
     */
    final static int OBSERVERINFO = 16;
    
    /**
     * This message type is sent by the leader to indicate it's zxid and if
     * needed, its database.
     */
    final static int NEWLEADER = 10;

    /**
     * This message type is sent by a follower to pass the last zxid. This is here
     * for backward compatibility purposes.
     */
    final static int FOLLOWERINFO = 11;

    /**
     * This message type is sent by the leader to indicate that the follower is
     * now uptodate andt can start responding to clients.
     */
    final static int UPTODATE = 12;

    /**
     * This message is the first that a follower receives from the leader.
     * It has the protocol version and the epoch of the leader.
     */
    public static final int LEADERINFO = 17;

    /**
     * This message is used by the follow to ack a proposed epoch.
     */
    public static final int ACKEPOCH = 18;
    
    /**
     * This message type is sent to a leader to request and mutation operation.
     * The payload will consist of a request header followed by a request.
     */
    final static int REQUEST = 1;

    /**
     * This message type is sent by a leader to propose a mutation.
     */
    public final static int PROPOSAL = 2;

    /**
     * This message type is sent by a follower after it has synced a proposal.
     */
    final static int ACK = 3;

    /**
     * This message type is sent by a leader to commit a proposal and cause
     * followers to start serving the corresponding data.
     */
    final static int COMMIT = 4;

    /**
     * This message type is enchanged between follower and leader (initiated by
     * follower) to determine liveliness.
     */
    final static int PING = 5;

    /**
     * This message type is to validate a session that should be active.
     */
    final static int REVALIDATE = 6;

    /**
     * This message is a reply to a synchronize command flushing the pipe
     * between the leader and the follower.
     */
    final static int SYNC = 7;
        
    /**
     * This message type informs observers of a committed proposal.
     */
    final static int INFORM = 8;

    ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();

    ConcurrentLinkedQueue<Proposal> toBeApplied = new ConcurrentLinkedQueue<Proposal>();

    Proposal newLeaderProposal = new Proposal();
    
    class LearnerCnxAcceptor extends Thread{
        private volatile boolean stop = false;
        
        @Override
        public void run() {
            try {
                while (!stop) {
                    try{
                        Socket s = ss.accept();
                        // start with the initLimit, once the ack is processed
                        // in LearnerHandler switch to the syncLimit
                        s.setSoTimeout(self.tickTime * self.initLimit);
                        s.setTcpNoDelay(nodelay);
                        LearnerHandler fh = new LearnerHandler(s, Leader.this);
                        fh.start();
                    } catch (SocketException e) {
                        if (stop) {
                            LOG.info("exception while shutting down acceptor: "
                                    + e);

                            // When Leader.shutdown() calls ss.close(),
                            // the call to accept throws an exception.
                            // We catch and set stop to true.
                            stop = true;
                        } else {
                            throw e;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Exception while accepting follower", e);
            }
        }
        
        public void halt() {
            stop = true;
        }
    }

    StateSummary leaderStateSummary;
    
    long epoch = -1;
    boolean waitingForNewEpoch = true;
    volatile boolean readyToStart = false;
    
    /**
     * This method is main function that is called to lead
     *
     * 此方法是调用lead的主要功能
     * @throws IOException
     * @throws InterruptedException
     */

    /*
    Leader分支大致可以分为5个阶段：启动LearnerCnxAcceptor线程、计算newEpoch、广播newEpoch、数据同步和集群状态监测。
    * */

    /*
    Leader.lead()方法控制着Leader角色节点的主体流程，其实现较为简单，大致模式都是通过阻塞方法阻塞当前线程，直到该阶段完成Leader线程才会被唤醒继续执行下一个阶段；
    而每个阶段实现的具体细节及大量的网络IO操作等都在LearnerHandler中实现。比如计算newEpoch，Leader中只会判断newEpoch计算完成没，没有计算完成就会进入阻塞状态挂起当前Leader线程，
    直到集群中一半以上的节点同步了epoch信息后newEpoch正式产生才会唤醒Leader线程继续向下执行；而计算newEpoch会涉及到Leader去收集集群中大部分Learner服务器的epoch信息，
    会涉及到大量的网络IO通信等内容，这些细节部分都在LearnerHandler中实现。

    涉及到网络IO就会存在Server和Client，这里的Server就是Leader，Client就是Learner(Follower和Observer统称Learner)，对于Server端，主要关注Leader和LearnerHandler这两个类，
    而对于Client端，根据角色分类主要关注Follower或Observer这两个类。
    * */
    void lead() throws IOException, InterruptedException {
        self.end_fle = System.currentTimeMillis();
        LOG.info("LEADING - LEADER ELECTION TOOK - " +
              (self.end_fle - self.start_fle));
        self.start_fle = 0;
        self.end_fle = 0;

        zk.registerJMX(new LeaderBean(this, zk), self.jmxLocalPeerBean);

        try {
            self.tick = 0;
            zk.loadData();
            
            leaderStateSummary = new StateSummary(self.getCurrentEpoch(), zk.getLastProcessedZxid());

            // Start thread that waits for connection requests from 
            // new followers.

            //启动LearnerCnxAcceptor线程
            /*
            Leader首先会启动一个LearnerCnxAcceptor线程，这个线程做的工作就非常简单了，就是不停的循环accept接收Learner端的网络请求(这里的监听端口就是上面说的同步监听端口，
            而不是选举端口)，Leader选举结束后被分配为Follower或Observer角色的节点会主动向Leader发起连接，Leader端接收到一个网络连接就会封装成一个LearnerHandler线程。
            Leader类可以看成一个总管，和每个Learner服务器的交互任务都会被分派给LearnerHandler这个助手完成，当Leader检测到一个任务被一半以上的LearnerHandler处理完成，
            即认为该阶段结束，进入下一个阶段。
            * */
            cnxAcceptor = new LearnerCnxAcceptor();
            cnxAcceptor.start();
            
            readyToStart = true;
            //计算epoch
            /***
             epoch在Zookeeper中是一个很重要的概念，前面也介绍过了：epoch就相当于Leader的身份编号，就如同身份证编号一样，每次选举产生一个新Leader时
             ，都会为该Leader重新计算出一个新epoch。epoch被设计成一个递增值

             epoch作用：可以防止旧Leader活过来后继续广播之前旧提议造成状态不一致问题，只有当前Leader的提议才会被Follower处理。
             Zookeeper集群所有的事务请求操作都要提交由Leader服务器完成，Leader服务器将事务请求转成一个提议(Proposal)并分配一个事务ID(zxid)后广播给Learner，
             zxid就是由epoch和counter(递增)组成，当存在旧leader向follower发送命令的时候，follower发现zxid所在的epoch比当前的小，则直接拒绝，防止出现不一致性。
             * */
            /**
             1.getEpochToPropose():计算新epoch，该方法会阻塞，直到集群中过半的Follower节点同步完epoch并计算出一个新的epoch才会继续向下执行
             */
            long epoch = getEpochToPropose(self.getId(), self.getAcceptedEpoch());
            
            zk.setZxid(ZxidUtils.makeZxid(epoch, 0));
            
            synchronized(this){
                lastProposed = zk.getZxid();
            }
            
            newLeaderProposal.packet = new QuorumPacket(NEWLEADER, zk.getZxid(),
                    null, null);


            if ((newLeaderProposal.packet.getZxid() & 0xffffffffL) != 0) {
                LOG.info("NEWLEADER proposal has Zxid of "
                        + Long.toHexString(newLeaderProposal.packet.getZxid()));
            }
            outstandingProposals.put(newLeaderProposal.packet.getZxid(), newLeaderProposal);
            newLeaderProposal.ackSet.add(self.getId());

            /*
            newEpoch计算完成后，该值只有Leader知道，现在需要将newEpoch广播到集群中所有的服务器节点上，让他们都更新下新Leader的epoch信息，
            这样他们在处理请求时会根据epoch判断该请求是不是当前新Leader发出的，可以防止旧Leader活过来后继续广播之前旧提议造成状态不一致问题，
            只有当前Leader的提议才会被Follower处理。
            * */
            /**
             * 2.waitForEpochAck():将新epoch广播给集群中所有Learner节点，该方法会阻塞，直到新epoch广播到集群中，并收到一半以上节点ack反馈时才会继续向下执行
             */

            /**
             * 3.新epoch计算完成并广播同步到集群中后，由于选举策略决定，新选出的Leader一定是数据最新的节点，其它节点要和Leader之间进行数据同步，同步大致流程：
             1、根据Learner节点的lastZxid和Leader节点的lastZxid对比，确定同步方式DIFF、SNAP、TRUNC以及DIFF+TRUNC
             2、数据同步Leader发送数据包大致分为三类：先发送同步方式数据包(DIFF+SNAP+TRUNC)、然后发送同步Proposal+Committed数据包、
             最后发送NewLeader数据包表示Leader已经将同步数据发送完成
             3、Learner接收到同步方式后进行相关的准备工作，然后根据Proposal+Committed进行同步，最后收到NewLeader表示同步数据包发送完毕了，需要对NewLeader数据包进行回复Ack数据包
             4、waitForNewLeaderAck()方法功能就是进入阻塞直到收到过半Follower节点回复的ack(包括自身)才会继续向下执行
             */

            /*
            1、Leader和Learner间同步可以分为四种模式：SNAP(全量同步)、DIFF(差异同步)、TRUNC(回滚同步)、TRUNC+DIFF(回滚+差异同步)

            2、具体采用何种方式同步，关键由如下几个参数判断确定：
                peerLastZxid：该Learner服务器最后处理的ZXID
                minCommittedLog：Leader服务器提议缓存队列committedLog中的最小ZXID
                maxCommittedLog：Leader服务器提议缓存队列committedLog中的最大ZXID

                a、peerLastZxid就是上面介绍的ACKEPOCH带过来的Learner端最大事务ID，
                b、Leader端会在zk服务器上维护一个committedLog集合，该集合保存了最近事务请求的议案Proposal，默认保存最近500条；minCommittedLog就是这个集合中最小的zxid，而maxCommittedLog就是这个集合中最大的zxid

            下面来分析下各种情况的逻辑：
                a、全量同步（SNAP同步）
                   场景1：peerLastZxid < minCommittedLog
                   场景2：committedLog集合为空，且Leader和Learner的lastZxid不相等

                   先来看下场景1：由于peerLastZxid < minCommittedLog，说明Learner和Leader之间的数据差异较大，即使将committedLog集合中全部的议案同步个Learner也无法保持他们之间的数据一直，因此必须采用全量同步方式
                   再来看看场景2：由于committedLog集合为空表示没有可用于进行差异化同步的议案，但是Leader和Learner的lastZxid又不相等，说明他们之前的数据不是一致的，因此也只能采取全量同步方式

                    所谓全量同步就是Leader服务器将本机上的全量内存数据构建出一个镜像快照snapshot，通过网络IO同步给Learner，Learner节点获取到Leader的镜像快照数据即可用于覆盖当前内存数据，这样就保证同步后Learner和Leader之间数据的一致。

                b、差异化同步（DIFF同步）
                   场景：minCommittedLog <= peerLastZxid <= maxCommittedLog

                   这种场景说明Learner和Leader之间数据存在数据差异，且差异的数据不大，可以进行差异化同步，即找出committedLog集合中在Learner端没有执行的议案，然后将这些议案发送过去，让Learner重新执行下这个议案，同时在每个议案后面都会被追加一个COMMIT议案进行提交(zk中的议案Proposal就相当于Oracle中的redo或MySql中binlog，通过将这些日志逐条重新执行一次可以完成数据的恢复)，最后达到Leader和Learner间数据一致

                 c、回滚+差异化同步(TRUNC+DIFF)
                   上面分析minCommittedLog <= peerLastZxid <= maxCommittedLog情况，会进行差异化同步，但是这里会存在一类特殊情况，即：Learner存在Leader上不存在的数据，这里需要先对Learner进行回滚，然后再进行DIFF操作

                   这种场景是如何出现的呢？
                   1、假设有A、B、C三台机器，假如某一时刻A是Leader服务器，其epoch=1，其上已经存在的事务ID包括：0x10000001和0x10000002，然后客户端发送一个事务请求q1，其事务ID为：0x10000003，A将事务请求q1写入到本地事务日志文件后，准备将其广播出去，但还未广播出去时A服务器奔溃导致zk集群重新进入Leader选举状态
                   2、这时B和C进行选举，假设C被选为新的Leader后，其epoch变更为2，接收到客户端新的事务请求q2，其事务ID为：0x20000001
                   3、这时之前奔溃得到A节点活过来了，得知当前Leader为B节点，节点A就会连接节点B进行数据同步，这时B节点作为Leader其维护的committedLog中包括的事务ID有：0x10000001、0x10000002和0x20000001，而获取到节点A的peerLastZxid为
            0x10000003，其符合minCommittedLog <= peerLastZxid <= maxCommittedLog，但是0x10000003在Leader上并不存在，这时就需要将节点A上的0x10000003回滚掉，然后将0x20000001发送给A进行差异化同步，即可完成数据一致性同步

                    还会存在另一种场景同样会导致这种情况出现：
                    1、假设A、B、C、D、E五台服务器，假如某一时刻A是Leader服务器，其epoch=1，现在集群中已存在的事务有2个：0x10000001和0x10000002，然后客户端发送一个事务请求q1，其事务ID为：0x10000003，A将事务请求q1写入到本地事务日志文件后，就会将其广播出去，如果这时只广播给了节点B后就挂了，然后进入选举状态
                    2、虽然节点B的lastZxid最大会被优先选为Leader，但是可能节点B的网络较差最终C、D和E三台节点都选举E节点作为新Leader，3大于集群(5)的一半，选举是有效的，这时依然会存在上述的情况

                d、回滚同步（TRUNC同步）
                    场景：peerLastZxid大于maxCommittedLog。
                    这种场景其实就是上述先回滚再差异化同步的简化模式，Learner只需要回滚到ZXID值为maxCommitedLog即可和Leader保持数据一致

            3、同步大致流程：
                1、首先确定同步方式，然后将同步方式并附带一些额外的参数封装成一个数据包发送给Learner节点，让Learner知道接下来如何进行数据同步，以及Learner可能会进行一些前期的准备工作，比如TRUNC+DIFF方式则需要先回滚一些数据，SNAP方式则需要先清理掉内存数据等等
                 2、然后发送Proposal+Commit或Snapshot镜像数据进行同步工作
                 3、最后Leader会向Learner发送NEWLEADER数据包，代表Leader已经将所有需要同步的数据包发送完毕了，Learner收到NEWLEADER数据包会回复一个ACK数据包
                 4、当收到过半回复的ACK数据包后，表示Leader端进行数据同步完成，Leader最后会向Learner发送UPTODATE数据包，告知Learner集群同步任务已经完成，Learner也会退出同步流程

             详细代码逻辑可以参见：LearnerHandler、Leader、Follower、Observer等这几个类。
            * */
            waitForEpochAck(self.getId(), leaderStateSummary);
            self.setCurrentEpoch(epoch);

            // We have to get at least a majority of servers in sync with
            // us. We do this by waiting for the NEWLEADER packet to get
            // acknowledged
            while (!self.getQuorumVerifier().containsQuorum(newLeaderProposal.ackSet)){
            //while (newLeaderProposal.ackCount <= self.quorumPeers.size() / 2) {
                if (self.tick > self.initLimit) {
                    // Followers aren't syncing fast enough,
                    // renounce leadership!
                    StringBuilder ackToString = new StringBuilder();
                    for(Long id : newLeaderProposal.ackSet)
                        ackToString.append(id + ": ");
                    
                    shutdown("Waiting for a quorum of followers, only synced with: " + ackToString);
                    HashSet<Long> followerSet = new HashSet<Long>();

                    for(LearnerHandler f : getLearners()) {
                        followerSet.add(f.getSid());
                    }

                    if (self.getQuorumVerifier().containsQuorum(followerSet)) {
                    //if (followers.size() >= self.quorumPeers.size() / 2) {
                        LOG.warn("Enough followers present. "+
                                "Perhaps the initTicks need to be increased.");
                    }
                    return;
                }
                Thread.sleep(self.tickTime);
                self.tick++;
            }
            
            /**
             * WARNING: do not use this for anything other than QA testing
             * on a real cluster. Specifically to enable verification that quorum
             * can handle the lower 32bit roll-over issue identified in
             * ZOOKEEPER-1277. Without this option it would take a very long
             * time (on order of a month say) to see the 4 billion writes
             * necessary to cause the roll-over to occur.
             * 
             * This field allows you to override the zxid of the server. Typically
             * you'll want to set it to something like 0xfffffff0 and then
             * start the quorum, run some operations and see the re-election.
             */
            String initialZxid = System.getProperty("zookeeper.testingonly.initialZxid");
            if (initialZxid != null) {
                long zxid = Long.parseLong(initialZxid);
                zk.setZxid((zk.getZxid() & 0xffffffff00000000L) | zxid);
            }

            if (!System.getProperty("zookeeper.leaderServes", "yes").equals("no")) {
                self.cnxnFactory.setZooKeeperServer(zk);
            }
            // Everything is a go, simply start counting the ticks
            // WARNING: I couldn't find any wait statement on a synchronized
            // block that would be notified by this notifyAll() call, so
            // I commented it out
            //synchronized (this) {
            //    notifyAll();
            //}
            // We ping twice a tick, so we only update the tick every other
            // iteration
            boolean tickSkip = true;
    
            while (true) {
                Thread.sleep(self.tickTime / 2);
                if (!tickSkip) {
                    self.tick++;
                }
                HashSet<Long> syncedSet = new HashSet<Long>();

                // lock on the followers when we use it.
                syncedSet.add(self.getId());

                for (LearnerHandler f : getLearners()) {
                    // Synced set is used to check we have a supporting quorum, so only
                    // PARTICIPANT, not OBSERVER, learners should be used
                    if (f.synced() && f.getLearnerType() == LearnerType.PARTICIPANT) {
                        syncedSet.add(f.getSid());
                    }
                    f.ping();
                }

              if (!tickSkip && !self.getQuorumVerifier().containsQuorum(syncedSet)) {
                //if (!tickSkip && syncedCount < self.quorumPeers.size() / 2) {
                    // Lost quorum, shutdown
                  // TODO: message is wrong unless majority quorums used
                    shutdown("Only " + syncedSet.size() + " followers, need "
                            + (self.getVotingView().size() / 2));
                    // make sure the order is the same!
                    // the leader goes to looking
                    return;
              } 
              tickSkip = !tickSkip;
            }
        } finally {
            zk.unregisterJMX(this);
        }
    }

    boolean isShutdown;

    /**
     * Close down all the LearnerHandlers
     */
    void shutdown(String reason) {
        LOG.info("Shutting down");

        if (isShutdown) {
            return;
        }
        
        LOG.info("Shutdown called",
                new Exception("shutdown Leader! reason: " + reason));

        if (cnxAcceptor != null) {
            cnxAcceptor.halt();
        }
        
        // NIO should not accept conenctions
        self.cnxnFactory.setZooKeeperServer(null);
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during close",e);
        }
        // clear all the connections
        self.cnxnFactory.closeAll();
        // shutdown the previous zk
        if (zk != null) {
            zk.shutdown();
        }
        synchronized (learners) {
            for (Iterator<LearnerHandler> it = learners.iterator(); it
                    .hasNext();) {
                LearnerHandler f = it.next();
                it.remove();
                f.shutdown();
            }
        }
        isShutdown = true;
    }

    /**
     * Keep a count of acks that are received by the leader for a particular
     * proposal
     * 
     * @param zxid
     *                the zxid of the proposal sent out
     * @param followerAddr
     */
    synchronized public void processAck(long sid, long zxid, SocketAddress followerAddr) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Ack zxid: 0x{}", Long.toHexString(zxid));
            for (Proposal p : outstandingProposals.values()) {
                long packetZxid = p.packet.getZxid();
                LOG.trace("outstanding proposal: 0x{}",
                        Long.toHexString(packetZxid));
            }
            LOG.trace("outstanding proposals all");
        }
        
        if (outstandingProposals.size() == 0) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("outstanding is 0");
            }
            return;
        }
        if (lastCommitted >= zxid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("proposal has already been committed, pzxid: 0x{} zxid: 0x{}",
                        Long.toHexString(lastCommitted), Long.toHexString(zxid));
            }
            // The proposal has already been committed
            return;
        }
        Proposal p = outstandingProposals.get(zxid);
        if (p == null) {
            LOG.warn("Trying to commit future proposal: zxid 0x{} from {}",
                    Long.toHexString(zxid), followerAddr);
            return;
        }
        
        p.ackSet.add(sid);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Count for zxid: 0x{} is {}",
                    Long.toHexString(zxid), p.ackSet.size());
        }
        if (self.getQuorumVerifier().containsQuorum(p.ackSet)){             
            if (zxid != lastCommitted+1) {
                LOG.warn("Commiting zxid 0x{} from {} not first!",
                        Long.toHexString(zxid), followerAddr);
                LOG.warn("First is 0x{}", Long.toHexString(lastCommitted + 1));
            }
            outstandingProposals.remove(zxid);
            if (p.request != null) {
                toBeApplied.add(p);
            }
            // We don't commit the new leader proposal
            if ((zxid & 0xffffffffL) != 0) {
                if (p.request == null) {
                    LOG.warn("Going to commmit null request for proposal: {}", p);
                }
                commit(zxid);
                inform(p);
                zk.commitProcessor.commit(p.request);
                if(pendingSyncs.containsKey(zxid)){
                    for(LearnerSyncRequest r: pendingSyncs.remove(zxid)) {
                        sendSync(r);
                    }
                }
                return;
            } else {
                lastCommitted = zxid;
                LOG.info("Have quorum of supporters; starting up and setting last processed zxid: 0x{}",
                        Long.toHexString(zk.getZxid()));
                zk.startup();
                zk.getZKDatabase().setlastProcessedZxid(zk.getZxid());
            }
        }
    }

    static class ToBeAppliedRequestProcessor implements RequestProcessor {
        private RequestProcessor next;

        private ConcurrentLinkedQueue<Proposal> toBeApplied;

        /**
         * This request processor simply maintains the toBeApplied list. For
         * this to work next must be a FinalRequestProcessor and
         * FinalRequestProcessor.processRequest MUST process the request
         * synchronously!
         * 
         * @param next
         *                a reference to the FinalRequestProcessor
         */
        ToBeAppliedRequestProcessor(RequestProcessor next,
                ConcurrentLinkedQueue<Proposal> toBeApplied) {
            if (!(next instanceof FinalRequestProcessor)) {
                throw new RuntimeException(ToBeAppliedRequestProcessor.class
                        .getName()
                        + " must be connected to "
                        + FinalRequestProcessor.class.getName()
                        + " not "
                        + next.getClass().getName());
            }
            this.toBeApplied = toBeApplied;
            this.next = next;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#processRequest(org.apache.zookeeper.server.Request)
         */
        public void processRequest(Request request) throws RequestProcessorException {
            // request.addRQRec(">tobe");
            next.processRequest(request);
            Proposal p = toBeApplied.peek();
            if (p != null && p.request != null
                    && p.request.zxid == request.zxid) {
                toBeApplied.remove();
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.zookeeper.server.RequestProcessor#shutdown()
         */
        public void shutdown() {
            LOG.info("Shutting down");
            next.shutdown();
        }
    }

    /**
     * send a packet to all the followers ready to follow
     * 
     * @param qp
     *                the packet to be sent
     */
    void sendPacket(QuorumPacket qp) {
        synchronized (forwardingFollowers) {
            for (LearnerHandler f : forwardingFollowers) {                
                f.queuePacket(qp);
            }
        }
    }
    
    /**
     * send a packet to all observers     
     */
    void sendObserverPacket(QuorumPacket qp) {        
        for (LearnerHandler f : getObservingLearners()) {
            f.queuePacket(qp);
        }
    }

    long lastCommitted = -1;

    /**
     * Create a commit packet and send it to all the members of the quorum
     * 
     * @param zxid
     */
    public void commit(long zxid) {
        synchronized(this){
            lastCommitted = zxid;
        }
        QuorumPacket qp = new QuorumPacket(Leader.COMMIT, zxid, null, null);
        sendPacket(qp);
    }
    
    /**
     * Create an inform packet and send it to all observers.
     * @param zxid
     * @param proposal
     */
    public void inform(Proposal proposal) {   
        QuorumPacket qp = new QuorumPacket(Leader.INFORM, proposal.request.zxid, 
                                            proposal.packet.getData(), null);
        sendObserverPacket(qp);
    }

    long lastProposed;

    
    /**
     * Returns the current epoch of the leader.
     * 
     * @return
     */
    public long getEpoch(){
        return ZxidUtils.getEpochFromZxid(lastProposed);
    }
    
    @SuppressWarnings("serial")
    public static class XidRolloverException extends Exception {
        public XidRolloverException(String message) {
            super(message);
        }
    }

    /**
     * create a proposal and send it out to all the members
     * 
     * @param request
     * @return the proposal that is queued to send to all the members
     */
    public Proposal propose(Request request) throws XidRolloverException {
        /**
         * Address the rollover issue. All lower 32bits set indicate a new leader
         * election. Force a re-election instead. See ZOOKEEPER-1277
         */
        if ((request.zxid & 0xffffffffL) == 0xffffffffL) {
            String msg =
                    "zxid lower 32 bits have rolled over, forcing re-election, and therefore new epoch start";
            shutdown(msg);
            throw new XidRolloverException(msg);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
        try {
            request.hdr.serialize(boa, "hdr");
            if (request.txn != null) {
                request.txn.serialize(boa, "txn");
            }
            baos.close();
        } catch (IOException e) {
            LOG.warn("This really should be impossible", e);
        }
        QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid, 
                baos.toByteArray(), null);
        
        Proposal p = new Proposal();
        p.packet = pp;
        p.request = request;
        synchronized (this) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proposing:: " + request);
            }

            lastProposed = p.packet.getZxid();
            outstandingProposals.put(lastProposed, p);
            sendPacket(pp);
        }
        return p;
    }
            
    /**
     * Process sync requests
     * 
     * @param r the request
     */
    
    synchronized public void processSync(LearnerSyncRequest r){
        if(outstandingProposals.isEmpty()){
            sendSync(r);
        } else {
            List<LearnerSyncRequest> l = pendingSyncs.get(lastProposed);
            if (l == null) {
                l = new ArrayList<LearnerSyncRequest>();
            }
            l.add(r);
            pendingSyncs.put(lastProposed, l);
        }
    }
        
    /**
     * Sends a sync message to the appropriate server
     * 
     * @param f
     * @param r
     */
            
    public void sendSync(LearnerSyncRequest r){
        QuorumPacket qp = new QuorumPacket(Leader.SYNC, 0, null, null);
        r.fh.queuePacket(qp);
    }
                
    /**
     * lets the leader know that a follower is capable of following and is done
     * syncing
     * 
     * @param handler handler of the follower
     * @return last proposed zxid
     */
    synchronized public long startForwarding(LearnerHandler handler,
            long lastSeenZxid) {
        // Queue up any outstanding requests enabling the receipt of
        // new requests
        if (lastProposed > lastSeenZxid) {
            for (Proposal p : toBeApplied) {
                if (p.packet.getZxid() <= lastSeenZxid) {
                    continue;
                }
                handler.queuePacket(p.packet);
                // Since the proposal has been committed we need to send the
                // commit message also
                QuorumPacket qp = new QuorumPacket(Leader.COMMIT, p.packet
                        .getZxid(), null, null);
                handler.queuePacket(qp);
            }
            List<Long>zxids = new ArrayList<Long>(outstandingProposals.keySet());
            Collections.sort(zxids);
            for (Long zxid: zxids) {
                if (zxid <= lastSeenZxid) {
                    continue;
                }
                handler.queuePacket(outstandingProposals.get(zxid).packet);
            }
        }
        if (handler.getLearnerType() == LearnerType.PARTICIPANT) {
            addForwardingFollower(handler);
        } else {
            addObserverLearnerHandler(handler);
        }
                
        return lastProposed;
    }

    private HashSet<Long> connectingFollowers = new HashSet<Long>();
    public long getEpochToPropose(long sid, long lastAcceptedEpoch) throws InterruptedException, IOException {
        synchronized(connectingFollowers) {
            if (!waitingForNewEpoch) {
                return epoch;
            }
            if (lastAcceptedEpoch >= epoch) {
                epoch = lastAcceptedEpoch+1;
            }
            connectingFollowers.add(sid);
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (connectingFollowers.contains(self.getId()) && 
                                            verifier.containsQuorum(connectingFollowers)) {
                waitingForNewEpoch = false;
                self.setAcceptedEpoch(epoch);
                connectingFollowers.notifyAll();
            } else {
                long start = System.currentTimeMillis();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(waitingForNewEpoch && cur < end) {
                    connectingFollowers.wait(end - cur);
                    cur = System.currentTimeMillis();
                }
                if (waitingForNewEpoch) {
                    throw new InterruptedException("Timeout while waiting for epoch from quorum");        
                }
            }
            return epoch;
        }
    }

    private HashSet<Long> electingFollowers = new HashSet<Long>();
    private boolean electionFinished = false;
    public void waitForEpochAck(long id, StateSummary ss) throws IOException, InterruptedException {
        synchronized(electingFollowers) {
            if (electionFinished) {
                return;
            }
            if (ss.getCurrentEpoch() != -1) {
                if (ss.isMoreRecentThan(leaderStateSummary)) {
                    throw new IOException("Follower is ahead of the leader, leader summary: " 
                                                    + leaderStateSummary.getCurrentEpoch()
                                                    + " (current epoch), "
                                                    + leaderStateSummary.getLastZxid()
                                                    + " (last zxid)");
                }
                electingFollowers.add(id);
            }
            QuorumVerifier verifier = self.getQuorumVerifier();
            if (electingFollowers.contains(self.getId()) && verifier.containsQuorum(electingFollowers)) {
                electionFinished = true;
                electingFollowers.notifyAll();
            } else {                
                long start = System.currentTimeMillis();
                long cur = start;
                long end = start + self.getInitLimit()*self.getTickTime();
                while(!electionFinished && cur < end) {
                    electingFollowers.wait(end - cur);
                    cur = System.currentTimeMillis();
                }
                if (!electionFinished) {
                    throw new InterruptedException("Timeout while waiting for epoch to be acked by quorum");
                }
            }
        }
    }

    /**
     * Get string representation of a given packet type
     * @param packetType
     * @return string representing the packet type
     */
    public static String getPacketType(int packetType) {
        switch (packetType) {
        case DIFF:
            return "DIFF";
        case TRUNC:
            return "TRUNC";
        case SNAP:
            return "SNAP";
        case OBSERVERINFO:
            return "OBSERVERINFO";
        case NEWLEADER:
            return "NEWLEADER";
        case FOLLOWERINFO:
            return "FOLLOWERINFO";
        case UPTODATE:
            return "UPTODATE";
        case LEADERINFO:
            return "LEADERINFO";
        case ACKEPOCH:
            return "ACKEPOCH";
        case REQUEST:
            return "REQUEST";
        case PROPOSAL:
            return "PROPOSAL";
        case ACK:
            return "ACK";
        case COMMIT:
            return "COMMIT";
        case PING:
            return "PING";
        case REVALIDATE:
            return "REVALIDATE";
        case SYNC:
            return "SYNC";
        case INFORM:
            return "INFORM";
        default:
            return "UNKNOWN";
        }
    }
}
