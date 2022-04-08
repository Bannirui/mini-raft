package com.github.bannirui.raft.core;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.FileUtil;
import com.baidu.brpc.client.RpcCallback;
import com.github.bannirui.raft.bean.constant.FilePathConstant;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.core.storeage.SegmentLog;
import com.github.bannirui.raft.core.storeage.Snapshot;
import com.google.protobuf.ByteString;
import lombok.AllArgsConstructor;
import lombok.Data;

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
@Data
public class Node
{

    private enum State
    {
        FOLLOWER, PRE_CANDIDATE, CANDIDATE, LEADER;
    }

    /**
     * raft节点配置
     */
    private Option option;

    /**
     * 集群信息 所有的节点
     */
    private RaftProto.Configuration configuration;

    /**
     * 集群中除本机节点的其他节点
     * key=serverId
     */
    private ConcurrentMap<Integer, Peer> peerMap = new ConcurrentHashMap<>();

    /**
     * 本机节点
     */
    private RaftProto.Server localServer;

    /**
     * 状态机
     */
    private StateMachine stateMachine;

    private SegmentLog raftLog;

    private Snapshot snapshot;

    /**
     * 节点初始化时默认状态是follower
     */
    private State state = State.FOLLOWER;

    /**
     * 服务器最后一次知道的任期号
     * 初始化为0 持续递增
     */
    private long currentTerm;

    /**
     * 在当前获得选票的候选人id
     */
    private int voteFor;

    /**
     * leader节点的id
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

    public Node(Option option, List<RaftProto.Server> servers, RaftProto.Server localServer, StateMachine stateMachine)
    {
        // 配置
        this.option = option;
        // 集群
        RaftProto.Configuration.Builder configurationBuilder = RaftProto.Configuration.newBuilder();
        for (RaftProto.Server server : servers)
        {
            if (Objects.isNull(server)) continue;
            configurationBuilder.addServers(server);
        }
        this.configuration = configurationBuilder.build();
        // 本地
        this.localServer = localServer;
        // 状态机
        this.stateMachine = stateMachine;
        // log data
        this.raftLog = new SegmentLog(option.getDataDir(), option.getMaxSegmentFileSize());
        this.snapshot = new Snapshot(option.getDataDir());
        this.snapshot.reload();

        this.currentTerm = this.raftLog.getMetaData().getCurrentTerm();
        this.voteFor = raftLog.getMetaData().getVotedFor();
        this.commitIndex = Math.max(this.snapshot.getMetaData().getLastIncludedIndex(), this.raftLog.getMetaData().getCommitIndex());
        if (this.snapshot.getMetaData().getLastIncludedIndex() > 0 && this.raftLog.getFirstLogIndex() <= this.snapshot.getMetaData().getLastIncludedIndex())
            this.raftLog.truncatePrefix(this.snapshot.getMetaData().getLastIncludedIndex() + 1);
        RaftProto.Configuration configuration = this.snapshot.getMetaData().getConfiguration();
        if (configuration.getServersCount() > 0) this.configuration = configuration;
        // classpath:data/snapshot/data
        String snapshotDataDir = this.snapshot.getSnapshotDir() + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_DATA;
        stateMachine.readSnapshot(snapshotDataDir);
        for (long index = this.snapshot.getMetaData().getLastIncludedIndex() + 1; index <= this.commitIndex; index++)
        {
            RaftProto.LogEntry entry = this.raftLog.getEntry(index);
            if (Objects.equals(entry.getType(), RaftProto.EntryType.DATA))
                stateMachine.apply(entry.getData().toByteArray());
            else if (Objects.equals(entry.getType(), RaftProto.EntryType.CONFIGURATION)) this.applyConfiguration(entry);
        }
        this.lastAppliedIndex = this.commitIndex;
    }

    public void init()
    {
        List<RaftProto.Server> serversList = this.configuration.getServersList();
        if (CollUtil.isEmpty(serversList)) return;
        for (RaftProto.Server server : serversList)
        {
            if (Objects.isNull(server)) continue;
            int serverId = server.getServerId();
            // 除本机之外的节点信息
            if (!Objects.equals(serverId, this.localServer.getServerId()) && !this.peerMap.containsKey(serverId))
            {
                Peer peer = new Peer(server);
                peer.setNextIndex(this.raftLog.getLastLogIndex() + 1);
                this.peerMap.put(server.getServerId(), peer);
            }
        }
        this.executorService = new ThreadPoolExecutor(this.option.getRaftConsensusThreadNum(), this.option.getRaftConsensusThreadNum(), 60, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        this.scheduledExecutorService = Executors.newScheduledThreadPool(2);
        this.scheduledExecutorService.scheduleWithFixedDelay(this::takeSnapshot, this.option.getSnapshotPeriodS(), this.option.getSnapshotPeriodS(), TimeUnit.SECONDS);
        this.resetElectionTimer();
    }

    public boolean replicate(byte[] data, RaftProto.EntryType entryType)
    {
        this.lock.lock();
        long newLastLogIndex = 0L;
        try
        {
            if (Objects.equals(this.state, State.LEADER)) return false;
            RaftProto.LogEntry logEntry = RaftProto.LogEntry.newBuilder().setTerm(this.currentTerm).setType(entryType).setData(ByteString.copyFrom(data)).build();
            newLastLogIndex = this.raftLog.append(logEntry);
            this.configuration.getServersList().forEach(server -> {
                Peer peer = this.peerMap.get(server.getServerId());
                if (Objects.nonNull(peer)) this.executorService.submit(() -> this.appendEntries(peer));
            });
            if (this.option.isAsyncWrite()) return true;
            long startTime = System.currentTimeMillis();
            while (this.lastAppliedIndex < newLastLogIndex)
            {
                if (System.currentTimeMillis() - startTime >= this.option.getMaxAwaitTimeoutMS()) break;
                this.commitIndexCondition.await(this.option.getMaxAwaitTimeoutMS(), TimeUnit.MILLISECONDS);
            }
        }
        catch (Exception ignored)
        {
        }
        finally
        {
            this.lock.unlock();
        }
        if (this.lastAppliedIndex < newLastLogIndex) return false;
        return true;
    }

    public void appendEntries(Peer peer)
    {
        RaftProto.AppendEntriesRequest.Builder requestBuilder = RaftProto.AppendEntriesRequest.newBuilder();
        long prevLogIndex, numEntries;
        boolean isNeedInstallSnapshot = false;
        this.lock.lock();
        try
        {
            long firstLogIndex = this.raftLog.getFirstLogIndex();
            if (peer.getNextIndex() < firstLogIndex) isNeedInstallSnapshot = true;
        }
        finally
        {
            this.lock.unlock();
        }
        if (isNeedInstallSnapshot && !this.installSnapshot(peer)) return;
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
            if (prevLogIndex == 0) prevLogTerm = 0;
            else if (prevLogIndex == lastSnapshotIndex) prevLogTerm = lastSnapshotTerm;
            else prevLogTerm = this.raftLog.getEntryTerm(prevLogIndex);
            requestBuilder.setServerId(this.localServer.getServerId());
            requestBuilder.setTerm(this.currentTerm);
            requestBuilder.setPrevLogTerm(prevLogTerm);
            requestBuilder.setPrevLogIndex(prevLogIndex);
            numEntries = this.packEntries(peer.getNextIndex(), requestBuilder);
            requestBuilder.setCommitIndex(Math.min(this.commitIndex, prevLogIndex + numEntries));
        }
        finally
        {
            this.lock.unlock();
        }
        RaftProto.AppendEntriesRequest request = requestBuilder.build();
        RaftProto.AppendEntriesResponse response = peer.getAsyncConsensusService().appendEntries(request);
        this.lock.lock();
        try
        {
            if (Objects.isNull(response))
            {
                boolean exist = this.configuration.getServersList().stream().filter(Objects::nonNull).anyMatch(server -> Objects.equals(server.getServerId(), peer.getServer().getServerId()));
                if (!exist)
                {
                    this.peerMap.remove(peer.getServer().getServerId());
                    peer.getRpcClient().stop();
                }
                return;
            }
            if (response.getTerm() > this.currentTerm) this.stepDown(response.getTerm());
            else
            {
                if (!Objects.equals(response.getResCode(), RaftProto.ResCode.SUCCESS))
                    peer.setNextIndex(response.getLastLogIndex() + 1);
                else
                {
                    long matchIndex = prevLogIndex + numEntries;
                    peer.setMatchIndex(matchIndex);
                    peer.setNextIndex(matchIndex + 1);
                    boolean exist = this.configuration.getServersList().stream().filter(Objects::nonNull).anyMatch(server -> Objects.equals(server.getServerId(), peer.getServer().getServerId()));
                    if (exist) this.advanceCommitIndex();
                    else
                    {
                        if (this.raftLog.getLastLogIndex() - peer.getMatchIndex() <= this.option.getCatchupMargin())
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

    public void stepDown(long newTerm)
    {
        if (this.currentTerm > newTerm) return;
        if (this.currentTerm < newTerm)
        {
            this.currentTerm = newTerm;
            this.leaderId = 0;
            this.voteFor = 0;
            this.raftLog.updateMetaData(this.currentTerm, this.voteFor, null, null);
        }
        this.state = State.FOLLOWER;
        if (Objects.nonNull(this.heartbeatScheduledFuture) && !this.heartbeatScheduledFuture.isDone())
            this.heartbeatScheduledFuture.cancel(true);
        this.resetElectionTimer();
    }

    public void takeSnapshot()
    {
        if (this.snapshot.getIsInstallSnapshot().get()) return;
        this.snapshot.getIsTakeSnapshot().compareAndSet(false, true);
        try
        {
            long localLastAppliedIndex;
            long lastAppliedTerm = 0L;
            RaftProto.Configuration.Builder localConfiguration = RaftProto.Configuration.newBuilder();
            this.lock.lock();
            try
            {
                if (this.raftLog.getTotalSize() < this.option.getSnapshotMinLogSize()) return;
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
                String tmpSnapshotDir = this.snapshot.getSnapshotDir() + FilePathConstant.Snapthot.Dir.SNAPSHOT_TMP;
                this.snapshot.updateMetaData(tmpSnapshotDir, localLastAppliedIndex, lastAppliedTerm, localConfiguration.build());
                String tmpSnapshotDataDir = tmpSnapshotDir + File.separator + "data";
                this.stateMachine.writeSnapshot(tmpSnapshotDataDir);
                try
                {
                    File snapshotDirFile = new File(this.snapshot.getSnapshotDir());
                    if (snapshotDirFile.exists()) FileUtil.del(snapshotDirFile);
                    FileUtil.move(new File(tmpSnapshotDir), new File(this.snapshot.getSnapshotDir()), true);
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
        return this.snapshot.getMetaData().getLastIncludedTerm();
    }

    private void resetElectionTimer()
    {
        if (Objects.nonNull(this.electionScheduledFuture) && !this.electionScheduledFuture.isDone())
            this.electionScheduledFuture.cancel(true);
        this.electionScheduledFuture = this.scheduledExecutorService.schedule(this::startPreVote, this.getElectionTimeoutMS(), TimeUnit.MILLISECONDS);
    }

    private long getElectionTimeoutMS()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        return this.option.getElectionTimeoutMS() + random.nextInt(0, this.option.getElectionTimeoutMS());
    }

    private void startPreVote()
    {
        this.lock.lock();
        try
        {
            boolean exist = this.configuration.getServersList().stream().anyMatch(e -> Objects.nonNull(e) && Objects.equals(e.getServerId(), this.localServer.getServerId()));
            if (!exist)
            {
                this.resetElectionTimer();
                return;
            }
            this.state = State.PRE_CANDIDATE;
        }
        finally
        {
            this.lock.unlock();
        }
        for (RaftProto.Server server : this.configuration.getServersList())
        {
            if (Objects.equals(server.getServerId(), this.localServer.getServerId())) continue;
            Peer peer = this.peerMap.get(server.getServerId());
            this.executorService.submit(() -> {
                this.preVote(peer);
            });
        }
        this.resetElectionTimer();
    }

    private void startVote()
    {
        this.lock.lock();
        try
        {
            boolean exist = this.configuration.getServersList().stream().anyMatch(e -> Objects.nonNull(e) && Objects.equals(e.getServerId(), this.localServer.getServerId()));
            if (!exist)
            {
                this.resetElectionTimer();
                return;
            }
            this.currentTerm++;
            this.state = State.CANDIDATE;
            this.leaderId = 0;
            this.voteFor = this.localServer.getServerId();
        }
        finally
        {
            this.lock.unlock();
        }
        for (RaftProto.Server server : this.configuration.getServersList())
        {
            int serverId = server.getServerId();
            if (Objects.equals(serverId, this.localServer.getServerId())) continue;
            Peer peer = this.peerMap.get(serverId);
            this.executorService.submit(() -> {
                this.requestVote(peer);
            });
        }
    }

    private void preVote(Peer peer)
    {
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        this.lock.lock();
        try
        {
            peer.setVoteGranted(null);
            requestBuilder.setServerId(this.localServer.getServerId()).setTerm(this.currentTerm).setLastLogIndex(this.raftLog.getLastLogIndex()).setLastLogTerm(this.getLastLogTerm());
        }
        finally
        {
            this.lock.unlock();
        }
        RaftProto.VoteRequest request = requestBuilder.build();
        peer.getAsyncConsensusService().preVote(request, new PreVoteResponseCallback(peer, request));
    }

    private void requestVote(Peer peer)
    {
        RaftProto.VoteRequest.Builder requestBuilder = RaftProto.VoteRequest.newBuilder();
        this.lock.lock();
        try
        {
            peer.setVoteGranted(null);
            requestBuilder.setServerId(this.localServer.getServerId()).setTerm(this.currentTerm).setLastLogIndex(this.raftLog.getLastLogIndex()).setLastLogTerm(this.getLastLogTerm());
        }
        finally
        {
            this.lock.unlock();
        }
        RaftProto.VoteRequest request = requestBuilder.build();
        peer.getAsyncConsensusService().requestVote(request, new VoteResponseCallback(peer, request));
    }

    private void becomeLeader()
    {
        this.state = State.LEADER;
        this.leaderId = this.localServer.getServerId();
        if (Objects.nonNull(this.electionScheduledFuture) && !this.electionScheduledFuture.isDone())
            this.electionScheduledFuture.cancel(true);
        this.startNewHeartbeat();
    }

    private void resetHeartbeatTimer()
    {
        if (Objects.nonNull(this.heartbeatScheduledFuture) && !this.heartbeatScheduledFuture.isDone())
            this.heartbeatScheduledFuture.cancel(true);
        this.heartbeatScheduledFuture = this.scheduledExecutorService.schedule(this::startNewHeartbeat, this.option.getHeartbeatPeriodMS(), TimeUnit.MILLISECONDS);
    }

    private void startNewHeartbeat()
    {
        for (Peer peer : this.peerMap.values())
            this.executorService.submit(() -> this.appendEntries(peer));
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
        if (!Objects.equals(this.raftLog.getEntryTerm(newCommitIndex), this.currentTerm)) return;
        if (this.commitIndex >= newCommitIndex) return;
        long oldCommitIndex = this.commitIndex;
        this.raftLog.updateMetaData(this.currentTerm, null, this.raftLog.getFirstLogIndex(), this.commitIndex);
        for (long index = oldCommitIndex + 1; index <= newCommitIndex; index++)
        {
            RaftProto.LogEntry entry = this.raftLog.getEntry(index);
            if (Objects.isNull(entry)) continue;
            if (Objects.equals(entry.getType(), RaftProto.EntryType.DATA))
                this.stateMachine.apply(entry.getData().toByteArray());
            else if (Objects.equals(entry.getType(), RaftProto.EntryType.CONFIGURATION)) this.applyConfiguration(entry);
        }
        this.lastAppliedIndex = this.commitIndex;
        this.commitIndexCondition.signalAll();
    }

    private long packEntries(long nextIndex, RaftProto.AppendEntriesRequest.Builder requestBuilder)
    {
        long lastIndex = Math.min(this.raftLog.getLastLogIndex(), nextIndex + this.option.getMaxLogEntriesPerRequest() - 1);
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
                RaftProto.InstallSnapshotResponse response = peer.getAsyncConsensusService().installSnapshot(request);
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
            int curDataSize = this.option.getMaxSnapshotBytesPerRequest();
            Snapshot.SnapshotDataFile curDataFile = lastFile;
            if (lastOffset + lastLength < lastFileLength)
            {
                if (lastOffset + lastLength + this.option.getMaxSnapshotBytesPerRequest() > lastFileLength)
                    curDataSize = (int) (lastFileLength - (lastOffset + lastLength));
            }
            else
            {
                Map.Entry<String, Snapshot.SnapshotDataFile> curEntry = snapshotDataFileMap.higherEntry(lastFileName);
                if (Objects.isNull(curEntry)) return null;
                curDataFile = curEntry.getValue();
                curFileName = curEntry.getKey();
                curOffset = 0L;
                int curFileLength = (int) curEntry.getValue().getRandomAccessFile().length();
                if (curFileLength < this.option.getMaxSnapshotBytesPerRequest()) curDataSize = curFileLength;
            }
            byte[] curData = new byte[curDataSize];
            curDataFile.getRandomAccessFile().seek(curOffset);
            curDataFile.getRandomAccessFile().read(curData);
            requestBuilder.setData(ByteString.copyFrom(curData));
            requestBuilder.setFileName(curFileName);
            requestBuilder.setOffset(curOffset);
            requestBuilder.setIsFirst(false);
            if (Objects.equals(curFileName, snapshotDataFileMap.lastKey()) && curOffset + curDataSize >= curDataFile.getRandomAccessFile().length())
                requestBuilder.setIsLast(true);
            else requestBuilder.setIsLast(false);

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
            requestBuilder.setTerm(this.currentTerm);
            requestBuilder.setServerId(this.localServer.getServerId());
        }
        finally
        {
            this.lock.unlock();
        }
        return requestBuilder.build();
    }

    @Data
    @AllArgsConstructor
    private class PreVoteResponseCallback implements RpcCallback<RaftProto.VoteResponse>
    {

        private Peer peer;
        private RaftProto.VoteRequest request;

        @Override
        public void success(RaftProto.VoteResponse response)
        {
            Node.this.lock.lock();
            try
            {
                this.peer.setVoteGranted(response.getGranted());
                if (!Objects.equals(Node.this.currentTerm, this.request.getTerm()) || !Objects.equals(Node.this.state, State.PRE_CANDIDATE))
                    return;
                if (response.getTerm() > Node.this.currentTerm) Node.this.stepDown(response.getTerm());
                else
                {
                    if (!response.getGranted()) return;
                    int voteGrantedNum = 1;
                    for (RaftProto.Server server : Node.this.configuration.getServersList())
                    {
                        if (Objects.equals(server.getServerId(), Node.this.localServer.getServerId())) continue;
                        Peer peer = Node.this.peerMap.get(server.getServerId());
                        if (Objects.equals(peer.getVoteGranted(), Boolean.TRUE)) voteGrantedNum++;
                    }
                    if (voteGrantedNum > Node.this.configuration.getServersCount() / 2) Node.this.startVote();
                }
            }
            finally
            {
                Node.this.lock.unlock();
            }
        }

        @Override
        public void fail(Throwable throwable)
        {
            this.peer.setVoteGranted(false);
        }
    }

    @Data
    @AllArgsConstructor
    private class VoteResponseCallback implements RpcCallback<RaftProto.VoteResponse>
    {

        private Peer peer;
        private RaftProto.VoteRequest request;

        @Override
        public void success(RaftProto.VoteResponse response)
        {
            Node.this.lock.lock();
            try
            {
                this.peer.setVoteGranted(response.getGranted());
                if (!Objects.equals(Node.this.currentTerm, this.request.getTerm()) || !Objects.equals(Node.this.state, State.CANDIDATE))
                    return;
                if (response.getTerm() > Node.this.currentTerm) Node.this.stepDown(response.getTerm());
                else
                {
                    if (!response.getGranted()) return;
                    int voteGrantedNum = 0;
                    if (Objects.equals(Node.this.voteFor, Node.this.localServer.getServerId())) voteGrantedNum++;
                    for (RaftProto.Server server : Node.this.configuration.getServersList())
                    {
                        if (Objects.equals(server.getServerId(), Node.this.localServer.getServerId())) continue;
                        Peer peer = Node.this.peerMap.get(server.getServerId());
                        if (Objects.equals(Boolean.TRUE, peer.getVoteGranted())) voteGrantedNum++;
                    }
                    if (voteGrantedNum > Node.this.configuration.getServersCount() / 2) Node.this.becomeLeader();
                }
            }
            finally
            {
                Node.this.lock.unlock();
            }
        }

        @Override
        public void fail(Throwable throwable)
        {
            this.peer.setVoteGranted(false);
        }
    }
}
