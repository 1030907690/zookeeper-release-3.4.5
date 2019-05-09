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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm is push-based
 * as with the other UDP implementations.
 * <p>
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 */


public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once it believes that it has reached the end of
     * leader election.
     */
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive
     * notification checks. This impacts the amount of time to get
     * the system up again after long partitions. Currently 60 seconds.
     */

    final static int maxNotificationInterval = 60000;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     */

    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of another
     * peer with higher zxid or same zxid and higher server id
     */

    static public class Notification {
        /*
         * Proposed leader
         */
        long leader;

        /*
         * zxid of the proposed leader
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * current state of sender
         */
        QuorumPeer.ServerState state;

        /*
         * Address of sender
         */
        long sid;

        /*
         * epoch of the proposed leader
         */
        long peerEpoch;
    }

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
               long leader,
               long zxid,
               long electionEpoch,
               ServerState state,
               long sid,
               long peerEpoch) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;

        /*
         * Leader epoch
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    private class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver implements Runnable {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                this.stop = false;
                this.manager = manager;
            }

            @Override
            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try {
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if (response == null) {
                            continue;
                        }

                        /*
                         * If it is from an observer, respond right away.
                         * Note that the following predicate assumes that
                         * if a server is not a follower, then it must be
                         * an observer. If we ever have any other type of
                         * learner in the future, we'll have to change the
                         * way we check for observers.
                         */
                        if (!self.getVotingView().containsKey(response.sid)) {
                            Vote current = self.getCurrentVote();
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock,
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = "
                                        + self.getId());
                            }

                            /*
                             * We check for 28 bytes for backward compatibility
                             */
                            if (response.buffer.capacity() < 28) {
                                LOG.error("Got a short response: "
                                        + response.buffer.capacity());
                                continue;
                            }
                            boolean backCompatibility = (response.buffer.capacity() == 28);
                            response.buffer.clear();

                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (response.buffer.getInt()) {
                                case 0:
                                    ackstate = QuorumPeer.ServerState.LOOKING;
                                    break;
                                case 1:
                                    ackstate = QuorumPeer.ServerState.FOLLOWING;
                                    break;
                                case 2:
                                    ackstate = QuorumPeer.ServerState.LEADING;
                                    break;
                                case 3:
                                    ackstate = QuorumPeer.ServerState.OBSERVING;
                                    break;
                            }

                            // Instantiate Notification and set its attributes
                            Notification n = new Notification();
                            n.leader = response.buffer.getLong();
                            n.zxid = response.buffer.getLong();
                            n.electionEpoch = response.buffer.getLong();
                            n.state = ackstate;
                            n.sid = response.sid;
                            if (!backCompatibility) {
                                n.peerEpoch = response.buffer.getLong();
                            } else {
                                if (LOG.isInfoEnabled()) {
                                    LOG.info("Backward compatibility mode, server id=" + n.sid);
                                }
                                n.peerEpoch = ZxidUtils.getEpochFromZxid(n.zxid);
                            }

                            /*
                             * Print notification info
                             */
                            if (LOG.isInfoEnabled()) {
                                printNotification(n);
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if (self.getPeerState() == QuorumPeer.ServerState.LOOKING) {
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if ((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock)) {
                                    Vote v = getVote();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock,
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if (ackstate == QuorumPeer.ServerState.LOOKING) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Sending new notification. My id =  " +
                                                self.getId() + " recipient=" +
                                                response.sid + " zxid=0x" +
                                                Long.toHexString(current.getZxid()) +
                                                " leader=" + current.getId());
                                    }
                                    ToSend notmsg = new ToSend(
                                            ToSend.mType.notification,
                                            current.getId(),
                                            current.getZxid(),
                                            logicalclock,
                                            self.getPeerState(),
                                            response.sid,
                                            current.getPeerEpoch());
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }


        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        /***
         WorkerSender线程会一直轮询提取sendqueue中的数据，当提取到ToSend数据后，会获取到集群中所有参与Leader选举节点(除Observer节点外的节点)的sid，
         如果sid即为本机节点，则转成Notification直接放入到recvqueue中，因为本机不再需要走网络IO；否则放入到queueSendMap中，key是要发送给哪个服务器节点的sid，
         ByteBuffer即为ToSend的内容，queueSendMap维护的着当前节点要发送的网络数据信息，由于发送到同一个sid服务器可能存在多条数据，所以queueSendMap的value是一个queue类型；
         * */
        class WorkerSender implements Runnable {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager) {
                this.stop = false;
                this.manager = manager;
            }

            @Override
            public void run() {
                while (!stop) {
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if (m == null) {
                            continue;
                        }

                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m message to send
             */
            private void process(ToSend m) {
                byte requestBytes[] = new byte[36];
                ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

                /*
                 * Building notification packet to send
                 */

                requestBuffer.clear();
                requestBuffer.putInt(m.state.ordinal());
                requestBuffer.putLong(m.leader);
                requestBuffer.putLong(m.zxid);
                requestBuffer.putLong(m.electionEpoch);
                requestBuffer.putLong(m.peerEpoch);

                manager.toSend(m.sid, requestBuffer);

            }
        }

        /**
         * Test if both send and receive queues are empty.
         */
        public boolean queueEmpty() {
            return (sendqueue.isEmpty() || recvqueue.isEmpty());
        }


        WorkerSender ws;
        WorkerReceiver wr;

        /**
         * Constructor of class Messenger.
         *
         * @param manager Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            Thread t = new Thread(this.ws,
                    "WorkerSender[myid=" + self.getId() + "]");
            t.setDaemon(true);
            //启动发送消息的线程
            t.start();

            this.wr = new WorkerReceiver(manager);

            t = new Thread(this.wr,
                    "WorkerReceiver[myid=" + self.getId() + "]");
            t.setDaemon(true);
            //启动接收消息线程
            t.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt() {
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    QuorumPeer self;
    Messenger messenger;
    volatile long logicalclock; /* Election instance */
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock() {
        return logicalclock;
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager) {
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self    QuorumPeer that created this object
     * @param manager Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    private void leaveInstance(Vote v) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("About to leave FLE instance: leader="
                    + v.getId() + ", zxid=0x" +
                    Long.toHexString(v.getZxid()) + ", my id=" + self.getId()
                    + ", my state=" + self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager() {
        return manager;
    }

    volatile boolean stop;

    @Override
    public void shutdown() {
        stop = true;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }


    /**
     * Send notifications to all peers upon a change in our vote
     */
    private void sendNotifications() {
        for (QuorumServer server : self.getVotingView().values()) {
            long sid = server.id;

            ToSend notmsg = new ToSend(ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock,
                    QuorumPeer.ServerState.LOOKING,
                    sid,
                    proposedEpoch);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x" +
                        Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock) +
                        " (n.round), " + sid + " (recipient), " + self.getId() +
                        " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            sendqueue.offer(notmsg);
        }
    }


    private void printNotification(Notification n) {

        LOG.info("通知: " + n.leader + " (n.leader), 0x"
                + Long.toHexString(n.zxid) + " (n.zxid), 0x"
                + Long.toHexString(n.electionEpoch) + " (n.round), " + n.state
                + " (n.state), " + n.sid + " (n.sid), 0x"
                + Long.toHexString(n.peerEpoch) + " (n.peerEPoch), "
                + self.getPeerState() + " (my state)");

        LOG.info("Notification: " + n.leader + " (n.leader), 0x"
                + Long.toHexString(n.zxid) + " (n.zxid), 0x"
                + Long.toHexString(n.electionEpoch) + " (n.round), " + n.state
                + " (n.state), " + n.sid + " (n.sid), 0x"
                + Long.toHexString(n.peerEpoch) + " (n.peerEPoch), "
                + self.getPeerState() + " (my state)");
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     * <p>
     * <p>
     * 检查一对（服务器ID，zxid）是否成功
     * 目前投票。
     *
     * @param id   Server identifier
     * @param zxid Last zxid observed by the issuer of this vote
     */
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        if (self.getQuorumVerifier().getWeight(newId) == 0) {
            return false;
        }
        
        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */

        /**
         * 对端投票胜出返回true情况：
         *      1、对端peerEpoch > 当前peerEpoch
         *      2、对端peerEpoch == 当前peerEpoch时，对端zxid > 当前zxid
         *      3、对端peerEpoch == 当前peerEpoch，对端zxid == 当前zxid时，对端serverID > 当前serverID
         */
        return ((newEpoch > curEpoch) ||
                ((newEpoch == curEpoch) &&
                        ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));

        /*
        下面简单说下这个PK逻辑原理(胜出一方代表更有希望成为Leader)：
            ​ 1、首先比较epoch，哪个epoch哪个胜出，前面介绍过epoch代表了Leader的轮次，是一个递增的，epoch越大就意味着数据越新，
            Leader数据越新则可以减少后续数据同步的效率，当然应该优先选为Leader；
            ​ 2、然后才是比较zxid，由于zxid=epoch+counter，第一步已经把epoch比较过了，其实这步骤只是相当于比较counter大小，
            counter越大则代表数据越新，优先选为Leader。注：其实第1和第2可以合并到一起，直接比较zxid即可，因为zxid=epoch+counter，第1比较显的有些多余
            ​ 3、如果前两个指标都没法比较出来，只能通过sid来确定，zxid相等说明两个服务器的数据是一致的，所以选哪个当Leader其实没有区别，这里就随机选择一个sid大的当Leader
        * */
    }

    /**
     * Termination predicate. Given a set of votes, determines if
     * have sufficient to declare the end of the election round.
     *
     * @param votes Set of votes
     * @param l     Identifier of the vote received last
     * @param zxid  zxid of the the vote received last
     */
    private boolean termPredicate(
            HashMap<Long, Vote> votes,
            Vote vote) {

        HashSet<Long> set = new HashSet<Long>();

        /*
         * First make the views consistent. Sometimes peers will have
         * different zxids for a server depending on timing.
         */
        for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())) {
                set.add(entry.getKey());
            }
        }

        return self.getQuorumVerifier().containsQuorum(set);
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes         set of votes
     * @param leader        leader id
     * @param electionEpoch epoch id
     */
    private boolean checkLeader(
            HashMap<Long, Vote> votes,
            long leader,
            long electionEpoch) {

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if (leader != self.getId()) {
            if (votes.get(leader) == null) {
                predicate = false;
            } else if (votes.get(leader).getState() != ServerState.LEADING) {
                predicate = false;
            }
        } else if (logicalclock != electionEpoch) {
            predicate = false;
        }

        return predicate;
    }

    synchronized void updateProposal(long leader, long zxid, long epoch) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x"
                    + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                    + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized Vote getVote() {
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        } else {
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch() {
        if (self.getLearnerType() == LearnerType.PARTICIPANT) {
            try {
                return self.getCurrentEpoch();
            } catch (IOException e) {
                RuntimeException re = new RuntimeException(e.getMessage());
                re.setStackTrace(e.getStackTrace());
                throw re;
            }
        } else {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     * <p>
     * 开始新一轮领导人选举。 每当我们的QuorumPeer
     * 将其状态更改为LOOKING，此方法被调用，并且它
     * 向所有其他同行发送通知。
     */


    /*
     Leader选举策略入口方法为：FastLeaderElection.lookForLeader()方法。当QuorumPeer.serverState变成LOOKING时，该方法会被调用，表示执行新一轮Leader选举。
     下面来看下lookForLeader方法的大致实现逻辑：
    ​ 1、更新自己期望投票信息，即自己期望选哪个服务器作为Leader(用sid代替期望服务器节点)以及该服务器zxid、epoch等信息，第一次投票默认都是投自己当选Leader，
    然后调用sendNotifications方法广播该投票到集群中所有可以参与投票服务器，广播涉及到网络IO流程前面已讲解
    * */
    @Override
    public Vote lookForLeader() throws InterruptedException {
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
            self.start_fle = System.currentTimeMillis();
        }
        try {
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();

            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();

            int notTimeout = finalizeWait;

            synchronized (this) {
                //给自己投票
                //logicalclock是一个AtomicLong类型，默认是0，执行incrementAndGet累加操作变成1
                //logicalclock维护electionEpoch，即选举轮次，在进行投票结果赛选的时候需要保证大家在一个投票轮次
                logicalclock++;
                //updateProposal()方法有三个参数：a.期望投票给哪个服务器(sid)、b.该服务器的zxid、c.该服务器的epoch，在后面会看到这三个参数是选举Leader时的核心指标
                //getInitId()用于获取当前myid
                //getInitLastLoggedZxid()提取lastProcessedZxid值，lastProcessedZxid是最后一次commit的事务请求的zxid
                //getPeerEpoch()：获取epoch值，每个leader任期内都要有一个epoch代表该Leader轮次，同时把该epoch同步到集群送的所有其它节点，
                // 并会被保存到本地硬盘dataLogDir目录下currentEpoch文件中，这里的getPeerEpoch()就是获取最近一次Leader的epoch，如果是第一次部署启动则默认从0开始
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info("New election. My id =  " + self.getId() +
                    ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            //将投票信息发送给集群中的每个服务器
            /**
             * 1、将proposedLeader、proposedZxid、electionEpoch、peerEpoch、sid(要发送给哪个节点的sid)等信息封装为一个ToSend对象，
             * 并放入到LinkedBlockingQueue<ToSend> sendqueue队列中，注意遍历集群中所有参与投票节点的sid，为每个sid封装成一个ToSend
             * 2、WorkerSender线程将会从sendqueue队列中获取要发送消息根据sid发送给集群中指定的节点
             */
            sendNotifications(); // 发送给集群中所有可参与投票节点，注意也包括自身节点

            /*
             * Loop in which we exchange notifications until we find a leader
             * 循环，在此循环中，我们交换通知，直到找到领导
             */
            //循环，如果是竞选状态一直到选举出结果

            //然后就开始等待其它服务器发送给自己的投票信息，接收投票涉及的网络IO流程看前面”网络IO”一节介绍
            while ((self.getPeerState() == ServerState.LOOKING) &&
                    (!stop)) {
                /*
                 * Remove next notification from queue, times out after 2 times
                 * the termination time
                 *
                 *从队列中删除下一个通知，2次后超时
             *终止时间
                 */
                //从recvqueue队列中取Notification
                Notification n = recvqueue.poll(notTimeout,
                        TimeUnit.MILLISECONDS);

                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 *
                 *如果收不到足够的通知，会发送更多通知。
          *否则处理新通知。
                 */
                //没有收到投票信息
                if (n == null) {
                    if (manager.haveDelivered()) {
                        sendNotifications();
                    } else {
                        manager.connectAll();
                    }

                    /*
                     * Exponential backoff
                     */
                    int tmpTimeOut = notTimeout * 2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval ?
                            tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                }
                //收到投票信息
                else if (self.getVotingView().containsKey(n.sid)) {
                    /*
                     * Only proceed if the vote comes from a replica in the
                     * voting view.
                     *
                     *只有在投票来自中的复制品时才进行
                  *投票观点。
                     */

                    /*
                    将接收到投票的state进行判断确定执行哪个分支逻辑：
                    ​ a.如果是FOLLOWING或LEADING，则说明对端已选举出Leader，这时只需要验证下这个Leader是否有效即可，有效则代表选举结束，否则继续接收投票信息
                    ​ b.OBSERVING：忽略该投票信息，因为Observer不能参与投票
                    ​ c.LOOKING：则表示对端也还处于Leader选举状态，
                    * */
                    switch (n.state) {
                        case LOOKING:
                            //LOOKING分支逻辑
                            // If notification > current, replace and send messages out

                        /*首先对之前提到的选举轮次electionEpoch进行判断，这里分为三种情况:
                             a.只有对方发过来的投票的electionEpoch和当前节点相等表示是同一轮投票，即投票有效，然后调用totalOrderPredicate()对投票进行PK，返回true代表对端胜出，则表示第一次投票是错误的(第一次都是投给自己)，更新自己投票期望对端为Leader，然后调用sendNotifications()将自己最新的投票广播出去。返回false则代表自己胜出，第一次投票没有问题，就不用管
                            ​ b.如果对端发过来的electionEpoch大于自己，则表明重置自己的electionEpoch，然后清空之前获取到的所有投票recvset，因为之前获取的投票轮次落后于当前则代表之前的投票已经无效了，然后调用totalOrderPredicate()将当前期望的投票和对端投票进行PK，用胜出者更新当前期望投票，然后调用sendNotifications()将自己期望头破广播出去。注意：这里不管哪一方胜出，都需要广播出去，而不是步骤a中己方胜出不需要广播，这是因为由于electionEpoch落后导致之前发出的所有投票都是无效的，所以这里需要重新发送
                            ​ c.如果对端发过来的electionEpoch小于自己，则表示对方投票无效，直接忽略不进行处理
                         */

                            // 判断投票是否过时，如果过时就清除之前已经接收到的信息
                            if (n.electionEpoch > logicalclock) {
                            /*
                            如果对端发过来的electionEpoch大于自己，则表明重置自己的electionEpoch，然后清空之前获取到的所有投票recvset，
                            因为之前获取的投票轮次落后于当前则代表之前的投票已经无效了，然后调用totalOrderPredicate()将当前期望的投票和对端投票进行PK，
                            用胜出者更新当前期望投票，然后调用sendNotifications()将自己期望投票广播出去。注意：这里不管哪一方胜出，都需要广播出去，
                            而不是步骤a中己方胜出不需要广播，这是因为由于electionEpoch落后导致之前发出的所有投票都是无效的，所以这里需要重新发送
                            * */
                                logicalclock = n.electionEpoch;
                                recvset.clear();
                                //更新投票信息
                                if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                        getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                    updateProposal(n.leader, n.zxid, n.peerEpoch);
                                } else {
                                    updateProposal(getInitId(),
                                            getInitLastLoggedZxid(),
                                            getPeerEpoch());
                                }
                                //发送投票信息
                                sendNotifications();
                            } else if (n.electionEpoch < logicalclock) {
                                //如果对端发过来的electionEpoch小于自己，则表示对方投票无效，直接忽略不进行处理
                                //忽略
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
                                            + Long.toHexString(n.electionEpoch)
                                            + ", logicalclock=0x" + Long.toHexString(logicalclock));
                                }
                                break;
                            } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                    proposedLeader, proposedZxid, proposedEpoch)) {
                            /*
                            只有对方发过来的投票的electionEpoch和当前节点相等表示是同一轮投票，即投票有效，然后调用totalOrderPredicate()对投票进行PK，返回true代表对端胜出，
                            则表示第一次投票是错误的(第一次都是投给自己)，更新自己投票期望对端为Leader，然后调用sendNotifications()将自己最新的投票广播出去。返回false则代表自己胜出，
                            第一次投票没有问题，就不用管
                            * */

                                //更新投票信息
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                                sendNotifications();
                            }

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Adding vote: from=" + n.sid +
                                        ", proposed leader=" + n.leader +
                                        ", proposed zxid=0x" + Long.toHexString(n.zxid) +
                                        ", proposed election epoch=0x" + Long.toHexString(n.electionEpoch));
                            }

                            //将接收到的有效投票放入到recvset中，该集合保存接收到的所有投票数据
                            recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                            //判断是否投票结束
                        /*termPredicate判断是否已经选举出Leader，其原理很简单：前面已经通过PK修正了自己期望选举的Leader投票，termPredicate要做的就是从所有投票recvset中赛选出期望选举的Leader投票，
                        然后看票数是否大于集群一半以上，超过则表示选举结束，否则则表明当前投票不足以选出Leader，需要继续接收投票*/
                            if (termPredicate(recvset,
                                    new Vote(proposedLeader, proposedZxid,
                                            logicalclock, proposedEpoch))) {

                                // Verify if there is any change in the proposed leader

                                //这里保险起见，将recvqueue接收到还没处理的投票继续进行验证下，确定当前选出的Leader是否有效
                                while ((n = recvqueue.poll(finalizeWait,
                                        TimeUnit.MILLISECONDS)) != null) {
                                    if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                            proposedLeader, proposedZxid, proposedEpoch)) {
                                        //如果进入这个if中，则表明当前选出的Leader和对端PK被PK掉了，即当前选出的Leader是无效的
                                        //将recvqueue提取的数据还放回去，然后跳出继续下一次从recvqueue提取数据进行投票处理逻辑
                                        recvqueue.put(n);
                                        break;
                                    }
                                }

                            /*
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             *
                             *一旦我们没有阅读任何新内容，这个谓词就是真的
                             *来自接待队列的相关消息
                             */
                                if (n == null) {  //执行到这里则表明当前已经选出Leader，并且验证后也是有效的
                                    //判断选出的Leader的sid是否就是自己，是则将peerState设置成LEADING，否则则设置成FOLLOWING或OBSERVING
                                    self.setPeerState((proposedLeader == self.getId()) ?
                                            ServerState.LEADING : learningState());

                                    Vote endVote = new Vote(proposedLeader,
                                            proposedZxid, proposedEpoch);
                                    leaveInstance(endVote); //将recvqueue清空，因为接受到的数据已经没有作用，所以要清空
                                    return endVote;


                                    /*
                                    至此，Leader的选举流程已经全部完成，Leader的选举流程会在系统刚启动时或Leader挂掉后，系统进入选举阶段，选举出来的Leader最终能否真正成为Leader，
                                    还需要进行数据恢复阶段考验，只有保证集群中的数据状态一致后，才算度过实习期真正成为Leader并对外提供服务。
                                    数据如何进行数据恢复保证集群状态一致性再后续博文中再继续进行分析。
                                    * */
                                }
                            }
                            break;
                        case OBSERVING:
                            //OBSERVING分支逻辑
                            //忽略
                            LOG.debug("Notification from observer: " + n.sid);
                            break;
                        case FOLLOWING:
                            //FOLLOWING分支逻辑
                        case LEADING:
                            //LEADING分支逻辑
                        /*
                         * Consider all notifications from the same epoch
                         * together.
                         */
                            //如果是同一轮投票
                            if (n.electionEpoch == logicalclock) {
                                recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));
                                //判断是否投票结束
                                if (termPredicate(recvset, new Vote(n.leader,
                                        n.zxid, n.electionEpoch, n.peerEpoch, n.state))
                                        && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                    self.setPeerState((n.leader == self.getId()) ?
                                            ServerState.LEADING : learningState());

                                    Vote endVote = new Vote(n.leader, n.zxid, n.peerEpoch);
                                    leaveInstance(endVote);
                                    return endVote;
                                }
                            }

                            /**
                             * Before joining an established ensemble, verify that
                             * a majority are following the same leader.
                             */
                            //记录投票已经完成
                            outofelection.put(n.sid, new Vote(n.leader, n.zxid,
                                    n.electionEpoch, n.peerEpoch, n.state));
                            if (termPredicate(outofelection, new Vote(n.leader,
                                    n.zxid, n.electionEpoch, n.peerEpoch, n.state))
                                    && checkLeader(outofelection, n.leader, n.electionEpoch)) {
                                synchronized (this) {
                                    logicalclock = n.electionEpoch;
                                    self.setPeerState((n.leader == self.getId()) ?
                                            ServerState.LEADING : learningState());
                                }
                                Vote endVote = new Vote(n.leader, n.zxid, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                            break;
                        default:
                            //忽略
                            LOG.warn("Notification state unrecoginized: " + n.state
                                    + " (n.state), " + n.sid + " (n.sid)");
                            break;
                    }
                } else {
                    LOG.warn("Ignoring notification from non-cluster member " + n.sid);
                }
            }
            return null;
        } finally {
            try {
                if (self.jmxLeaderElectionBean != null) {
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
        }
    }
}
