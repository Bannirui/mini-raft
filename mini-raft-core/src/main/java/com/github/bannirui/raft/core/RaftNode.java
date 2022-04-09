package com.github.bannirui.raft.core;

import cn.hutool.core.collection.CollUtil;
import com.baidu.brpc.client.RpcCallback;
import com.github.bannirui.raft.bean.enums.RaftNodeState;
import com.github.bannirui.raft.bean.constant.FilePathConstant;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.common.bean.RaftNodeOption;
import com.github.bannirui.raft.common.util.RaftConfigurationUtil;
import com.github.bannirui.raft.core.storeage.SegmentLog;
import com.github.bannirui.raft.core.storeage.Snapshot;
import com.google.protobuf.ByteString;
import com.googlecode.protobuf.format.JsonFormat;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>raft节点</p>
 * <p>功能实现<ul>
 *     <li>保存raft节点的核心数据<ul>
 *         <li>节点状态信息</li>
 *         <li>日志信息</li>
 *         <li>snapshot</li>
 *     </ul></li>
 *     <li>raft节点向其他节点发起rpc请求相关的方法</li>
 *     <li>raft节点定时器<ul>
 *         <li>主节点心跳定时器</li>
 *         <li>发起选举定时器</li>
 *     </ul></li>
 * </ul></p>
 * @since 2022/4/4
 * @author dingrui
 */
@Slf4j
@Data
@Component
public class RaftNode
{

    /**
     * raft节点配置
     */
    @Autowired
    RaftNodeOption raftNodeOption;

    /**
     * 状态机
     */
    private StateMachine stateMachine;

    /**
     * 集群信息 所有的节点
     */
    private RaftProto.Configuration configuration;

    /**
     * 集群中除本机节点的其他节点
     * key=serverId
     * value=节点封装信息
     */
    private Map<Integer, Peer> peerMap = new ConcurrentHashMap<>();

    /**
     * 本机节点
     */
    private RaftProto.Server localServer;

    private SegmentLog raftLog;

    private Snapshot snapshot;

    /**
     * 节点初始化时默认状态是follower
     */
    private RaftNodeState state = RaftNodeState.FOLLOWER;

    /**
     * 服务器最后一次知道的任期号
     * 初始化为0 持续递增
     */
    private long curTerm;

    /**
     * 在当前获得选票的候选人id
     * 对于每个节点 自己作为投票人将选票投给了谁
     */
    private int voteFor;

    /**
     * 集群中leader节点的serverId
     * 如果当前节点是leader 就维护自己的serverId
     * 如果当前节点是follower 就维护leader的serverId
     */
    private int leaderId;

    /**
     * 已知的最大的已经被提交的日志索引值
     */
    private long commitIndex;

    /**
     * 最后被应用到状态机的日志索引值
     * 初始化为0 持续递增
     */
    private volatile long lastAppliedIndex;

    private Lock lock = new ReentrantLock();
    private Condition commitIndexCondition = this.lock.newCondition();
    private Condition catchUpCondition = this.lock.newCondition();

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    /**
     * <p>raft节点属性赋值初始化及启动</p>
     * @since 2022/4/9
     * @author dingrui
     * @param servers 集群所有服务器
     * @param localServer 本机服务器
     * @return void
     */
    public void init(List<RaftProto.Server> servers, RaftProto.Server localServer, StateMachine stateMachine)
    {
        this.stateMachine = stateMachine;
        // 集群
        RaftProto.Configuration.Builder configurationBuilder = RaftProto.Configuration.newBuilder();
        for (RaftProto.Server server : servers)
        {
            if (Objects.isNull(server)) continue;
            configurationBuilder.addServers(server);
        }
        this.configuration = configurationBuilder.build();
        // 本地服务器
        this.localServer = localServer;
        // 日志数据
        this.raftLog = new SegmentLog(raftNodeOption.getDataDir(), raftNodeOption.getMaxSegmentFileSize());
        this.snapshot = new Snapshot(raftNodeOption.getDataDir());
        this.snapshot.reload();

        this.curTerm = this.raftLog.getMetaData().getCurrentTerm();
        this.voteFor = this.raftLog.getMetaData().getVotedFor();
        this.commitIndex = Math.max(this.snapshot.getMetaData().getLastIncludedIndex(), this.raftLog.getMetaData().getCommitIndex());
        if (this.snapshot.getMetaData().getLastIncludedIndex() > 0 && this.raftLog.getFirstLogIndex() <= this.snapshot.getMetaData().getLastIncludedIndex())
            this.raftLog.truncatePrefix(this.snapshot.getMetaData().getLastIncludedIndex() + 1);
        RaftProto.Configuration configuration = this.snapshot.getMetaData().getConfiguration();
        if (configuration.getServersCount() > 0) this.configuration = configuration;
        // ./data/snapshot/data
        String snapshotDataDir = this.snapshot.getSnapshotDir() + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_DATA;
        // 节点启动时 将节点目录下的snapshot数据加载到状态机并逐条进行读取 方便客户端的读取请求
        stateMachine.readSnapshot(snapshotDataDir);
        for (long index = this.snapshot.getMetaData().getLastIncludedIndex() + 1; index <= this.commitIndex; index++)
        {
            RaftProto.LogEntry entry = this.raftLog.getEntry(index);
            if (Objects.equals(entry.getType(), RaftProto.EntryType.DATA))
                stateMachine.apply(entry.getData().toByteArray());
            else if (Objects.equals(entry.getType(), RaftProto.EntryType.CONFIGURATION)) this.applyConfiguration(entry);
        }
        this.lastAppliedIndex = this.commitIndex;
        // 启动节点
        this.start();
    }

    /**
     * <p>服务器节点启动或者重启时进行初始化</p>
     * @since 2022/4/9
     * @author dingrui
     * @return void
     */
    private void start()
    {
        List<RaftProto.Server> serversList = this.configuration.getServersList();
        if (CollUtil.isEmpty(serversList)) return;
        for (RaftProto.Server server : serversList)
        {
            if (Objects.isNull(server)) continue;
            int serverId = server.getServerId();
            // 除本机之外的节点信息进行封装放到map中
            if (Objects.equals(serverId, this.localServer.getServerId()) || this.peerMap.containsKey(serverId))
                continue;
            Peer peer = new Peer(server);
            peer.setNextIndex(this.raftLog.getLastLogIndex() + 1);
            this.peerMap.put(server.getServerId(), peer);
        }
        // 线程池初始化
        this.executorService = new ThreadPoolExecutor(this.raftNodeOption.getRaftConsensusThreadNum(), this.raftNodeOption.getRaftConsensusThreadNum(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        this.scheduledExecutorService = Executors.newScheduledThreadPool(2);
        // 定时任务进行snapshot
        this.scheduledExecutorService.scheduleWithFixedDelay(this::takeSnapshot, this.raftNodeOption.getSnapshotPeriodS(), this.raftNodeOption.getSnapshotPeriodS(), TimeUnit.SECONDS);
        // 选举定时器
        this.resetElectionTimer();
    }

    /**
     * <p>集群中leader节点收到来自客户端的数据后<ul>
     *     <li>leader节点写日志</li>
     *     <li>日志信息同步给follower节点</li>
     * </ul></p>
     * @since 2022/4/8
     * @author dingrui
     * @param data 日志信息
     * @param entryType 标识leader节点要同步日志的是配置还是数据
     * @return boolean
     */
    public boolean replicate(byte[] data, RaftProto.EntryType entryType)
    {
        if (log.isInfoEnabled())
            log.info("节点{} 收到日志数据同步请求 日志类型={} 日志内容={}", this.getLocalServer().getServerId(), entryType, data);
        this.lock.lock();
        long newLastLogIndex = 0L;
        try
        {
            // 只有leader才能接收到来自客户端的请求 不是leader直接返回
            if (!Objects.equals(this.state, RaftNodeState.LEADER))
            {
                if (log.isInfoEnabled())
                    log.info("节点{} 当前节点状态state={} 不是leader 无权处理日志同步请求", this.getLocalServer().getServerId(), this.state);
                return false;
            }
            // 日志
            RaftProto.LogEntry logEntry = RaftProto.LogEntry.newBuilder().setTerm(this.curTerm).setType(entryType).setData(ByteString.copyFrom(data)).build();
            // leader写日志
            newLastLogIndex = this.raftLog.append(logEntry);
            if (log.isInfoEnabled())
                log.info("节点{} leader节点写日志成功 新的logIndex={} 准备同步给集群follower", this.localServer.getServerId(), newLastLogIndex);
            // leader将日志同步给follower
            this.peerMap.values().forEach(peer -> {
                if (log.isInfoEnabled())
                    log.info("节点{}为leader 向follower{}发送日志同步请求的rpc", this.localServer.getServerId(), peer.getServer().getServerId());
                if (Objects.nonNull(peer)) this.executorService.submit(() -> this.appendEntries(peer));
            });
            // 异步请求 leader写成功后就可以返回响应了
            if (this.raftNodeOption.isAsyncWrite()) return true;
            long startTime = System.currentTimeMillis();
            while (this.lastAppliedIndex < newLastLogIndex)
            {
                // 超时等待
                if (System.currentTimeMillis() - startTime >= this.raftNodeOption.getMaxAwaitTimeoutMS()) break;
                this.commitIndexCondition.await(this.raftNodeOption.getMaxAwaitTimeoutMS(), TimeUnit.MILLISECONDS);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            this.lock.unlock();
        }
        if (log.isInfoEnabled())
            log.info("节点{}是leader 同步完数据后 lastAppliedIndex={} newLastLogIndex={}", this.localServer.getServerId(), this.lastAppliedIndex, newLastLogIndex);
        if (this.lastAppliedIndex < newLastLogIndex) return false;
        return true;
    }

    /**
     * <p>leader向follower同步日志数据 {@link RaftProto.AppendEntriesRequest#getEntriesList()} )}<ul>
     *     <li>日志为空时代表是心跳包</li>
     *     <li>日志不为空表示的正常需要同步的日志</li>
     * </ul></p>
     * @since 2022/4/8
     * @author dingrui
     * @param peer 目标服务器(接收方)
     * @return void
     */
    public void appendEntries(Peer peer)
    {
        RaftProto.AppendEntriesRequest.Builder requestBuilder = RaftProto.AppendEntriesRequest.newBuilder();
        long prevLogIndex, numEntries;
        boolean isNeedInstallSnapshot = false;
        this.lock.lock();
        try
        {
            // peer需要快照
            long firstLogIndex = this.raftLog.getFirstLogIndex();
            if (peer.getNextIndex() < firstLogIndex) isNeedInstallSnapshot = true;
        }
        finally
        {
            this.lock.unlock();
        }
        if (log.isInfoEnabled()) log.info("节点{}是否需要快照={}", peer.getServer().getServerId(), isNeedInstallSnapshot);
        if (isNeedInstallSnapshot && !this.installSnapshot(peer))
        {
            if (log.isInfoEnabled()) log.info("节点{}为leader 需要进行快照 但是执行快照失败", this.localServer.getServerId());
            return;
        }
        long lastSnapshotIndex, lastSnapshotTerm;
        this.snapshot.getLock().lock();
        try
        {
            lastSnapshotIndex = this.snapshot.getMetaData().getLastIncludedIndex();
            lastSnapshotTerm = this.snapshot.getMetaData().getLastIncludedTerm();
        }
        finally
        {
            this.snapshot.getLock().unlock();
        }
        this.lock.lock();
        try
        {
            long firstLogIndex = this.raftLog.getFirstLogIndex();
            if (peer.getNextIndex() < firstLogIndex) return;
            prevLogIndex = peer.getNextIndex() - 1;
            long prevLogTerm;
            if (Objects.equals(prevLogIndex, 0L)) prevLogTerm = 0L;
            else if (Objects.equals(prevLogIndex, lastSnapshotIndex)) prevLogTerm = lastSnapshotTerm;
            else prevLogTerm = this.raftLog.getEntryTerm(prevLogIndex);
            // leader的id
            requestBuilder.setServerId(this.localServer.getServerId());
            // leader的任期
            requestBuilder.setTerm(this.curTerm);
            // 新日志紧随之前的索引
            requestBuilder.setPrevLogTerm(prevLogTerm);
            // 新日志紧随之前的日志任期
            requestBuilder.setPrevLogIndex(prevLogIndex);
            // 准备要存储的日志
            numEntries = this.packEntries(peer.getNextIndex(), requestBuilder);
            // leader已经提交的索引值
            requestBuilder.setCommitIndex(Math.min(this.commitIndex, prevLogIndex + numEntries));
        }
        finally
        {
            this.lock.unlock();
        }
        RaftProto.AppendEntriesRequest request = requestBuilder.build();
        // rpc同步方法
        RaftProto.AppendEntriesResponse response = peer.getConsensusService().appendEntries(request);
        this.lock.lock();
        if (log.isInfoEnabled() && CollUtil.isNotEmpty(request.getEntriesList()))
            log.info("{}为leader 向follower发送的写日志请求为{} 收到follower的响应为{}}", this.localServer.getServerId(), new JsonFormat().printToString(request), new JsonFormat().printToString(response));
        try
        {
            // leader没收到follower响应 主观判定follower为下线
            if (Objects.isNull(response))
            {
                if (!RaftConfigurationUtil.containsServer(this.configuration, peer.getServer().getServerId()))
                {
                    if (log.isInfoEnabled())
                        log.info("follower={} 已经不在集群列表中 下线该服务器 停掉对应的rpc服务", peer.getServer().getServerId());
                    // 服务器下线 将其暴露的rpc服务停掉
                    this.peerMap.remove(peer.getServer().getServerId());
                    peer.getRpcClient().stop();
                }
                return;
            }
            // follower的任期比leader还大 将leader降级为follower
            if (response.getTerm() > this.curTerm)
            {
                if (log.isInfoEnabled())
                {
                    log.info("{}为follower的任期号{}大于{}为leader的任期号{} 将leader降级为follower", peer.getServer().getServerId(), response.getTerm(), this.localServer.getServerId(), this.curTerm);
                }
                this.stepDown(response.getTerm());
            }
            else
            {
                // follower日志延迟于leader follower没跟上leader同步
                if (!Objects.equals(response.getResCode(), RaftProto.ResCode.SUCCESS))
                    peer.setNextIndex(response.getLastLogIndex() + 1);
                else
                {
                    long matchIndex = prevLogIndex + numEntries;
                    peer.setMatchIndex(matchIndex);
                    peer.setNextIndex(matchIndex + 1);
                    if (RaftConfigurationUtil.containsServer(this.configuration, peer.getServer().getServerId()))
                        this.advanceCommitIndex();
                    else
                    {
                        // follower和leader的日志差距在阈值之内 可以容忍
                        if (this.raftLog.getLastLogIndex() - peer.getMatchIndex() <= this.raftNodeOption.getCatchupMargin())
                        {
                            peer.setCatchUp(true);
                            this.catchUpCondition.signalAll();
                        }
                    }
                }
            }
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * <p>给定一个任期号 集群间互相通信的节点 leader的任期最大 这个方法贯穿在rpc调用中 收到rpc请求后都会调用一下该方法<ul>
     *     <li>如果当前节点是leader 正常情况下自己的任期最大 直接返回</li>
     *     <li>如果当前节点是leader 发现有比自己任期更大的 就将leader降级为follower</li>
     *     <li>如果当前节点是follower 那么请求只会来自leader</li>
     *     <li>一点节点不再是leader也就不再拥有发送心跳包的权利</li>
     * </ul></p>
     * @since 2022/4/8
     * @author dingrui
     * @param term 给定任期号
     * @return void
     */
    public void stepDown(long term)
    {
        // 参数校验
        if (this.curTerm > term) return;
        // follower
        if (this.curTerm < term)
        {
            this.curTerm = term;
            this.leaderId = 0;
            this.voteFor = 0;
            this.raftLog.updateMetaData(this.curTerm, this.voteFor, null, null);
        }
        // 节点角色(有可能是leader降级为follower)
        this.state = RaftNodeState.FOLLOWER;
        // 只有leader才有资格发心跳 leader如果被降级为了follower 取消其心跳发送资格
        if (Objects.nonNull(this.heartbeatScheduledFuture) && !this.heartbeatScheduledFuture.isDone())
            this.heartbeatScheduledFuture.cancel(true);
        // 选举定时器
        this.resetElectionTimer();
    }

    public void takeSnapshot()
    {
        // 加载快照过程中
        if (this.snapshot.getIsInstallSnapshot().get()) return;
        // cas锁
        this.snapshot.getIsTakeSnapshot().compareAndSet(false, true);
        try
        {
            // 已经应用到状态机的日志索引
            long localLastAppliedIndex;
            long lastAppliedTerm = 0L;
            RaftProto.Configuration.Builder localConfiguration = RaftProto.Configuration.newBuilder();
            this.lock.lock();
            try
            {
                if (this.raftLog.getTotalSize() < this.raftNodeOption.getSnapshotMinLogSize()) return;
                if (this.lastAppliedIndex <= this.snapshot.getMetaData().getLastIncludedIndex()) return;
                localLastAppliedIndex = this.lastAppliedIndex;
                if (this.lastAppliedIndex >= this.raftLog.getFirstLogIndex() && this.lastAppliedIndex <= this.raftLog.getLastLogIndex())
                    lastAppliedTerm = this.raftLog.getEntryTerm(this.lastAppliedIndex);
                localConfiguration.mergeFrom(this.configuration);
            }
            finally
            {
                this.lock.unlock();
            }
            boolean success = false;
            this.snapshot.getLock().lock();
            try
            {
                if (log.isInfoEnabled()) log.info("节点{} 进行快照装载", this.localServer.getServerId());
                // ./data/snapshot/.tmp
                String tmpSnapshotDir = this.snapshot.getSnapshotDir() + FilePathConstant.Snapthot.Dir.SNAPSHOT_TMP;
                this.snapshot.updateMetaData(tmpSnapshotDir, localLastAppliedIndex, lastAppliedTerm, localConfiguration.build());
                // ./data/snapshot/.tmp/data
                String tmpSnapshotDataDir = tmpSnapshotDir + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_TMP_DATA;
                // 将状态机中该数据快照到文件
                this.stateMachine.writeSnapshot(tmpSnapshotDataDir);
                try
                {
                    // ./data/snapshot
                    File snapshotDirFile = new File(this.snapshot.getSnapshotDir());
                    if (snapshotDirFile.exists()) FileUtils.deleteDirectory(snapshotDirFile);
                    FileUtils.moveDirectory(new File(tmpSnapshotDir), new File(this.snapshot.getSnapshotDir()));
                    if (log.isInfoEnabled()) log.info("节点{} 加载快照结束", this.localServer.getServerId());
                    success = true;
                }
                catch (Exception ignored)
                {
                }
            }
            finally
            {
                this.snapshot.getLock().unlock();
            }

            if (!success) return;
            // 重新加载snapshot
            long lastSnapshotIndex = 0L;
            this.snapshot.getLock().lock();
            try
            {
                this.snapshot.reload();
                lastSnapshotIndex = this.snapshot.getMetaData().getLastIncludedIndex();
            }
            finally
            {
                this.snapshot.getLock().unlock();
            }

            this.lock.lock();
            try
            {
                if (lastSnapshotIndex > 0 && this.raftLog.getFirstLogIndex() <= lastSnapshotIndex)
                    this.raftLog.truncateSuffix(lastSnapshotIndex + 1);
            }
            finally
            {
                this.lock.unlock();
            }
        }
        finally
        {
            // cas锁
            this.snapshot.getIsTakeSnapshot().compareAndSet(true, false);
        }
    }

    public void applyConfiguration(RaftProto.LogEntry entry)
    {
        RaftProto.Configuration newCfg = null;
        try
        {
            newCfg = RaftProto.Configuration.parseFrom(entry.getData().toByteArray());
        }
        catch (Exception ignored)
        {
        }
        if (Objects.isNull(newCfg)) return;
        this.configuration = newCfg;
        newCfg.getServersList().forEach(server -> {
            if (!this.peerMap.containsKey(server.getServerId()) && !Objects.equals(server.getServerId(), this.localServer.getServerId()))
            {
                Peer peer = new Peer(server);
                peer.setNextIndex(this.raftLog.getLastLogIndex() + 1);
                this.peerMap.put(server.getServerId(), peer);
            }
        });
    }

    public long getLastLogTerm()
    {
        long lastLogIndex = this.raftLog.getLastLogIndex();
        if (lastLogIndex >= this.raftLog.getFirstLogIndex()) return this.raftLog.getEntryTerm(lastLogIndex);
        // log为空 lastLogIndex==lastSnapshotIndex
        return this.snapshot.getMetaData().getLastIncludedTerm();
    }

    /**
     * <p>选举定时器</p>
     * @since 2022/4/8
     * @author dingrui
     * @return void
     */
    private void resetElectionTimer()
    {
        if (Objects.nonNull(this.electionScheduledFuture) && !this.electionScheduledFuture.isDone())
            this.electionScheduledFuture.cancel(true);
        this.electionScheduledFuture = this.scheduledExecutorService.schedule(this::startPreVote, this.getElectionTimeoutMS(), TimeUnit.MILLISECONDS);
    }

    /**
     * <p>超时区间 [t1...t2]时间内follower没有收到leader消息 就进行角色转换为candidate</p>
     * @since 2022/4/8
     * @author dingrui
     * @return long
     */
    private long getElectionTimeoutMS()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        // [5_000...10_000]ms
        return this.raftNodeOption.getElectionTimeoutMS() + random.nextInt(0, this.raftNodeOption.getElectionTimeoutMS());
    }

    /**
     * <p>raft集群节点发起pre-vote请求</p>
     *
     * <p><tt>pre-vote</tt> <tt>vote</tt>为二阶段实现 防止某一个节点断网后还不断增加term发起投票 当该节点网络恢复后 会导致集群的其他节点的term不断增到 最终导致集群状态变更</p>
     * @since 2022/4/8
     * @author dingrui
     * @return void
     */
    private void startPreVote()
    {
        this.lock.lock();
        try
        {
            if (!RaftConfigurationUtil.containsServer(this.configuration, this.localServer.getServerId()))
            {
                this.resetElectionTimer();
                return;
            }
            // 节点状态标识
            this.state = RaftNodeState.PRE_CANDIDATE;
        }
        finally
        {
            this.lock.unlock();
        }
        /**
         * 把自己的拉票请求发给集群中其他所有投票人
         */
        for (RaftProto.Server server : this.configuration.getServersList())
        {
            if (Objects.equals(server.getServerId(), this.localServer.getServerId())) continue;
            // 投票人
            Peer peer = this.peerMap.get(server.getServerId());
            this.executorService.submit(() -> this.preVote(peer));
        }
        this.resetElectionTimer();
    }

    /**
     * <p>客户端正式发起vote 对candidate有效 经历过{@link RaftNode#preVote}之后 已经征得了过半节点的选举同意 开始真正的选举投票操作</p>
     * @since 2022/4/8
     * @author dingrui
     * @return void
     */
    private void startVote()
    {
        this.lock.lock();
        try
        {
            // 节点状态check
            if (!RaftConfigurationUtil.containsServer(this.configuration, this.localServer.getServerId()))
            {
                this.resetElectionTimer();
                return;
            }
            // 增加任期号
            this.curTerm++;
            // 节点状态
            this.state = RaftNodeState.CANDIDATE;
            this.leaderId = 0;
            // 给自己一票
            this.voteFor = this.localServer.getServerId();
        }
        finally
        {
            this.lock.unlock();
        }
        // 向集群中节点发起拉票请求
        for (RaftProto.Server server : this.configuration.getServersList())
        {
            int serverId = server.getServerId();
            if (Objects.equals(serverId, this.localServer.getServerId())) continue;
            Peer peer = this.peerMap.get(serverId);
            this.executorService.submit(() -> this.vote(peer));
        }
    }

    /**
     * <p>客户端发起pre-vote请求</p>
     * <p>preVote是典型的2pc协议<ul>
     *     <li>第一阶段先征求其他节点是否统一选举</li>
     *     <li>如果统一选举则发起真正的选举操作 否则降级为follower角色</li>
     * </ul>
     * 这样避免了网络分区节点重新加入集群触发不必要的选举操作</p>
     * @since 2022/4/8
     * @author dingrui
     * @param peer 服务端 投票人(候选人把自己信息发给投票人 征求他们是否愿意进行投票选举)
     * @return void
     */
    private void preVote(Peer peer)
    {
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        this.lock.lock();
        try
        {
            // 投票人是否同意选举(该属性置空 留着rpc异步执行回调结果赋值)
            peer.setVoteGranted(null);
            // 候选人信息
            requestBuilder.setServerId(this.localServer.getServerId()).setTerm(this.curTerm).setLastLogIndex(this.raftLog.getLastLogIndex()).setLastLogTerm(this.getLastLogTerm());
        }
        finally
        {
            this.lock.unlock();
        }
        RaftProto.VoteRequest request = requestBuilder.build();
        /**
         * 征求投票人是否同意选举的意见
         * 该rpc为异步 响应结果{@link PreVoteResponseCallback#success}
         */
        peer.getConsensusService().preVote(request, new PreVoteResponseCallback(peer, request));
    }

    /**
     * <p>客户端发起正式vote请求</p>
     * @since 2022/4/8
     * @author dingrui
     * @param peer 服务端 投票人(候选人把自己信息发给投票人 获取他们的投票)
     * @return void
     */
    private void vote(Peer peer)
    {
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        this.lock.lock();
        try
        {
            // 投票人是否上票(该属性置空 留着rpc异步执行回调结果赋值)
            peer.setVoteGranted(null);
            // 候选人信息
            requestBuilder.setServerId(this.localServer.getServerId()).setTerm(this.curTerm).setLastLogIndex(this.raftLog.getLastLogIndex()).setLastLogTerm(this.getLastLogTerm());
        }
        finally
        {
            this.lock.unlock();
        }
        RaftProto.VoteRequest request = requestBuilder.build();
        /**
         * 征求投票人是否上票
         * 该rpc为异步 响应结果{@link VoteResponseCallback#success}
         */
        peer.getConsensusService().vote(request, new VoteResponseCallback(peer, request));
    }

    /**
     * <p>节点当选为leader</p>
     * @since 2022/4/8
     * @author dingrui
     * @return void
     */
    private void becomeLeader()
    {
        // 角色变更
        this.state = RaftNodeState.LEADER;
        this.leaderId = this.localServer.getServerId();
        // 当选为leader节点 集群有了leader就可以停止选举投票了
        if (Objects.nonNull(this.electionScheduledFuture) && !this.electionScheduledFuture.isDone())
            this.electionScheduledFuture.cancel(true);
        // leader向集群节点发送心跳
        this.startHeartbeat();
    }

    /**
     * <p>重置心跳定时器</p>
     * @since 2022/4/8
     * @author dingrui
     * @return void
     */
    private void resetHeartbeatTimer()
    {
        if (Objects.nonNull(this.heartbeatScheduledFuture) && !this.heartbeatScheduledFuture.isDone())
            this.heartbeatScheduledFuture.cancel(true);
        this.heartbeatScheduledFuture = this.scheduledExecutorService.schedule(this::startHeartbeat, this.raftNodeOption.getHeartbeatPeriodMS(), TimeUnit.MILLISECONDS);
    }

    /**
     * <p>节点向集群发送心跳 在集群中只有leader才有资格发送心跳</p>
     * @since 2022/4/8
     * @author dingrui
     * @return void
     */
    private void startHeartbeat()
    {
        for (Peer peer : this.peerMap.values())
        {
            if (log.isInfoEnabled())
                log.info("serverId={}为leader 给follower serverId={} 节点发送心跳", this.localServer.getServerId(), peer.getServer().getServerId());
            this.executorService.submit(() -> this.appendEntries(peer));
        }
        this.resetHeartbeatTimer();
    }

    private void advanceCommitIndex()
    {
        int peerNum = this.configuration.getServersList().size();
        long[] matchIndexes = new long[peerNum];
        int i = 0;
        for (RaftProto.Server server : this.configuration.getServersList())
        {
            if (Objects.equals(server.getServerId(), this.localServer.getServerId())) continue;
            Peer peer = this.peerMap.get(server.getServerId());
            matchIndexes[i++] = peer.getMatchIndex();
        }
        matchIndexes[i] = this.raftLog.getLastLogIndex();
        Arrays.sort(matchIndexes);
        long newCommitIndex = matchIndexes[peerNum / 2];
        if (!Objects.equals(this.raftLog.getEntryTerm(newCommitIndex), this.curTerm)) return;
        if (this.commitIndex >= newCommitIndex) return;
        long oldCommitIndex = this.commitIndex;
        this.commitIndex = newCommitIndex;
        this.raftLog.updateMetaData(this.curTerm, null, this.raftLog.getFirstLogIndex(), this.commitIndex);
        for (long index = oldCommitIndex + 1; index <= newCommitIndex; index++)
        {
            RaftProto.LogEntry entry = this.raftLog.getEntry(index);
            if (Objects.isNull(entry)) continue;
            if (Objects.equals(entry.getType(), RaftProto.EntryType.DATA))
                this.stateMachine.apply(entry.getData().toByteArray()); // 加载日志到状态机
            else if (Objects.equals(entry.getType(), RaftProto.EntryType.CONFIGURATION)) this.applyConfiguration(entry);
        }
        this.lastAppliedIndex = this.commitIndex;
        this.commitIndexCondition.signalAll();
    }

    /**
     * <p>索引[start...end]区间内日志内容添加到requestBuilder的日志列表中</p>
     * <p>当发送心跳包时 lastIndex=nextIndex-1 也就是[nextIndex...nextIndex-1] 不会向request日志包中添加日志内容</p>
     * @since 2022/4/8
     * @author dingrui
     * @param nextIndex 从[idx...]开始
     * @param requestBuilder 用于构建请求
     * @return long
     */
    private long packEntries(long nextIndex, RaftProto.AppendEntriesRequest.Builder requestBuilder)
    {
        // [start...end]的日志添加到builder中
        long lastIndex = Math.min(this.raftLog.getLastLogIndex(), nextIndex + this.raftNodeOption.getMaxLogEntriesPerRequest() - 1);
        for (long index = nextIndex; index <= lastIndex; index++)
        {
            RaftProto.LogEntry entry = this.raftLog.getEntry(index);
            requestBuilder.addEntries(entry);
        }
        return lastIndex - nextIndex + 1;
    }

    private boolean installSnapshot(Peer peer)
    {
        if (this.snapshot.getIsTakeSnapshot().get()) return false;
        // cas上锁
        if (!snapshot.getIsTakeSnapshot().compareAndSet(false, true)) return false;
        boolean success = true;
        TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap = this.snapshot.openSnapshotDataFiles();
        try
        {
            boolean isLastRequest = false;
            String lastFileName = null;
            long lastOffset = 0L;
            long lastLength = 0L;
            while (!isLastRequest)
            {
                RaftProto.InstallSnapshotRequest request = this.buildInstallSnapshotRequest(snapshotDataFileMap, lastFileName, lastOffset, lastLength);
                if (Objects.isNull(request))
                {
                    success = false;
                    break;
                }
                if (request.getIsLast()) isLastRequest = true;
                RaftProto.InstallSnapshotResponse response = peer.getConsensusService().installSnapshot(request);
                if (Objects.nonNull(response) && Objects.equals(response.getResCode(), RaftProto.ResCode.SUCCESS))
                {
                    lastFileName = request.getFileName();
                    lastOffset = request.getOffset();
                    lastLength = request.getData().size();
                }
                else
                {
                    success = false;
                    break;
                }
            }

            if (success)
            {
                long lastIncludedIndexInSnapshot;
                this.snapshot.getLock().lock();
                try
                {
                    lastIncludedIndexInSnapshot = this.snapshot.getMetaData().getLastIncludedIndex();
                }
                finally
                {
                    this.snapshot.getLock().unlock();
                }

                this.lock.lock();
                try
                {
                    peer.setNextIndex(lastIncludedIndexInSnapshot + 1);
                }
                finally
                {
                    this.lock.unlock();
                }
            }
        }
        finally
        {
            this.snapshot.closeSnapshotDataFiles(snapshotDataFileMap);
            // cas去锁
            this.snapshot.getIsInstallSnapshot().compareAndSet(true, false);
        }
        return success;
    }

    private RaftProto.InstallSnapshotRequest buildInstallSnapshotRequest(TreeMap<String, Snapshot.SnapshotDataFile> snapshotDataFileMap, String lastFileName, long lastOffset, long lastLength)
    {
        RaftProto.InstallSnapshotRequest.Builder requestBuilder = RaftProto.InstallSnapshotRequest.newBuilder();
        this.snapshot.getLock().lock();
        try
        {
            if (Objects.isNull(lastFileName))
            {
                lastFileName = snapshotDataFileMap.firstKey();
                lastOffset = 0L;
                lastLength = 0L;
            }
            Snapshot.SnapshotDataFile lastFile = snapshotDataFileMap.get(lastFileName);
            long lastFileLength = lastFile.getRandomAccessFile().length();
            String curFileName = lastFileName;
            long curOffset = lastOffset + lastLength;
            int curDataSize = this.raftNodeOption.getMaxSnapshotBytesPerRequest();
            Snapshot.SnapshotDataFile curDataFile = lastFile;
            if (lastOffset + lastLength < lastFileLength)
            {
                if (lastOffset + lastLength + this.raftNodeOption.getMaxSnapshotBytesPerRequest() > lastFileLength)
                    curDataSize = (int) (lastFileLength - (lastOffset + lastLength));
            }
            else
            {
                Map.Entry<String, Snapshot.SnapshotDataFile> curEntry = snapshotDataFileMap.higherEntry(lastFileName);
                if (Objects.isNull(curEntry)) return null;
                curFileName = curEntry.getKey();
                curDataFile = curEntry.getValue();
                curOffset = 0L;
                int curFileLength = (int) curEntry.getValue().getRandomAccessFile().length();
                if (curFileLength < this.raftNodeOption.getMaxSnapshotBytesPerRequest()) curDataSize = curFileLength;
            }
            byte[] curData = new byte[curDataSize];
            curDataFile.getRandomAccessFile().seek(curOffset);
            curDataFile.getRandomAccessFile().read(curData);
            requestBuilder.setData(ByteString.copyFrom(curData));
            requestBuilder.setFileName(curFileName);
            requestBuilder.setOffset(curOffset);
            requestBuilder.setIsFirst(false);
            requestBuilder.setIsLast(Objects.equals(curFileName, snapshotDataFileMap.lastKey()) && curOffset + curDataSize >= curDataFile.getRandomAccessFile().length());
            if (Objects.equals(curFileName, snapshotDataFileMap.firstKey()) && curOffset == 0)
            {
                requestBuilder.setIsFirst(true);
                requestBuilder.setSnapshotMetaData(this.snapshot.getMetaData());
            }
            else requestBuilder.setIsFirst(false);
        }
        catch (Exception ignored)
        {
            return null;
        }
        finally
        {
            this.snapshot.getLock().unlock();
        }

        this.lock.lock();
        try
        {
            requestBuilder.setTerm(this.curTerm);
            requestBuilder.setServerId(this.localServer.getServerId());
        }
        finally
        {
            this.lock.unlock();
        }
        return requestBuilder.build();
    }

    /**
     * <p>preVote响应</p>
     * <p>{@link com.github.bannirui.raft.core.service.async.AsyncConsensusService#preVote}</p>
     * @since 2022/4/8
     * @author dingrui
     */
    @Data
    private class PreVoteResponseCallback implements RpcCallback<RaftProto.VoteResponse>
    {

        /**
         * 投票人
         */
        private Peer peer;

        /**
         * 候选人拉票请求
         */
        private RaftProto.VoteRequest request;

        public PreVoteResponseCallback(Peer peer, RaftProto.VoteRequest request)
        {
            this.peer = peer;
            this.request = request;
        }

        @Override
        public void success(RaftProto.VoteResponse response)
        {
            if (Objects.isNull(response)) return;
            RaftNode.this.lock.lock();
            try
            {
                // 投票人是否同意进行选举
                this.peer.setVoteGranted(response.getGranted());
                // 候选人状态check
                if (!Objects.equals(RaftNode.this.curTerm, this.request.getTerm()) || !Objects.equals(RaftNode.this.state, RaftNodeState.PRE_CANDIDATE))
                {
                    if (log.isInfoEnabled()) log.info("preVote 当前节点作为候选人已经发生了状态变更 投票无效");
                    return;
                }
                if (log.isInfoEnabled())
                    log.info("节点{}任期号{} 收到投票人{}的任期号{}", RaftNode.this.localServer.getServerId(), RaftNode.this.curTerm, this.peer.getServer().getServerId(), response.getTerm());
                /**
                 * 投票人的任期号大于候选人
                 * 对候选人降级操作 降级为follower
                 */
                if (response.getTerm() > RaftNode.this.curTerm) RaftNode.this.stepDown(response.getTerm());
                else
                {
                    // 投票人不同意进行选举
                    if (!response.getGranted()) return;
                    // 集群中同意上票的节点数量
                    int voteGrantedNum = 1;
                    for (RaftProto.Server server : RaftNode.this.configuration.getServersList())
                    {
                        if (Objects.equals(server.getServerId(), RaftNode.this.localServer.getServerId())) continue;
                        Peer peer = RaftNode.this.peerMap.get(server.getServerId());
                        if (Objects.equals(peer.getVoteGranted(), Boolean.TRUE)) voteGrantedNum++;
                    }
                    // 过半节点同意进行选举 发起真正的投票选举操作
                    if (voteGrantedNum > RaftNode.this.configuration.getServersCount() / 2) RaftNode.this.startVote();
                }
            }
            finally
            {
                RaftNode.this.lock.unlock();
            }
        }

        @Override
        public void fail(Throwable throwable)
        {
            this.peer.setVoteGranted(false);
        }
    }

    /**
     * <p>vote响应</p>
     * @since 2022/4/8
     * @author dingrui
     */
    @Data
    private class VoteResponseCallback implements RpcCallback<RaftProto.VoteResponse>
    {

        /**
         * 投票人
         */
        private Peer peer;

        /**
         * 候选人信息
         */
        private RaftProto.VoteRequest request;

        public VoteResponseCallback(Peer peer, RaftProto.VoteRequest request)
        {
            this.peer = peer;
            this.request = request;
        }

        @Override
        public void success(RaftProto.VoteResponse response)
        {
            if (Objects.isNull(response)) return;
            RaftNode.this.lock.lock();
            try
            {
                // 回调结果赋值属性 投票人是否上票
                this.peer.setVoteGranted(response.getGranted());
                // 候选人状态check
                if (!Objects.equals(RaftNode.this.curTerm, this.request.getTerm()) || !Objects.equals(RaftNode.this.state, RaftNodeState.CANDIDATE))
                    return;
                if (log.isInfoEnabled())
                    log.info("节点{}任期号{} 收到投票人{}的任期号{}", RaftNode.this.localServer.getServerId(), RaftNode.this.curTerm, this.peer.getServer().getServerId(), response.getTerm());
                // 候选人任期号太低 降级follower
                if (response.getTerm() > RaftNode.this.curTerm) RaftNode.this.stepDown(response.getTerm());
                else
                {
                    // 投票人不上票
                    if (!response.getGranted()) return;
                    int voteGrantedNum = 0;
                    // 统计自己获得的票数 自己给自己上了一票
                    if (Objects.equals(RaftNode.this.voteFor, RaftNode.this.localServer.getServerId()))
                        voteGrantedNum++;
                    for (RaftProto.Server server : RaftNode.this.configuration.getServersList())
                    {
                        if (Objects.equals(server.getServerId(), RaftNode.this.localServer.getServerId())) continue;
                        Peer peer = RaftNode.this.peerMap.get(server.getServerId());
                        if (Objects.equals(Boolean.TRUE, peer.getVoteGranted())) voteGrantedNum++;
                    }
                    // 获得了过半选票 成为leader
                    if (voteGrantedNum > RaftNode.this.configuration.getServersCount() / 2)
                    {
                        if (log.isInfoEnabled())
                            log.info("节点{}获得了过半选票成为leader", RaftNode.this.localServer.getServerId());
                        RaftNode.this.becomeLeader();
                    }
                }
            }
            finally
            {
                RaftNode.this.lock.unlock();
            }
        }

        @Override
        public void fail(Throwable throwable)
        {
            this.peer.setVoteGranted(false);
        }
    }
}
