package com.github.bannirui.raft.core.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.file.FileMode;
import com.github.bannirui.raft.bean.constant.FilePathConstant;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.common.util.FileUtil;
import com.github.bannirui.raft.common.util.RaftConfigurationUtil;
import com.github.bannirui.raft.core.RaftNode;
import com.github.bannirui.raft.core.service.ConsensusService;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
@Slf4j
public class ConsensusServiceImpl implements ConsensusService
{

    private RaftNode raftNode;

    public ConsensusServiceImpl(RaftNode raftNode)
    {
        this.raftNode = raftNode;
    }

    @Override
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request)
    {
        this.raftNode.getLock().lock();
        try
        {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            // 投票人的任期号 回给拉票人 方便它更新任期号
            responseBuilder.setTerm(this.raftNode.getCurTerm());
            // 拉票的候选人不在集群
            if (!RaftConfigurationUtil.containsServer(this.raftNode.getConfiguration(), request.getServerId()))
                return responseBuilder.build();
            /**
             * 考察候选人任期号
             * 拉票候选人的任期号比投票人还低 不给候选人上票
             */
            if (request.getTerm() < this.raftNode.getCurTerm())
            {
                responseBuilder.setGranted(false);
                return responseBuilder.build();
            }
            /**
             * 考察候选人数据
             */
            boolean vote2Candidate = request.getLastLogTerm() > this.raftNode.getLastLogTerm() || (request.getLastLogTerm() == this.raftNode.getLastLogTerm() && request.getLastLogIndex() >= this.raftNode.getRaftLog().getLastLogIndex());
            if (!vote2Candidate) return responseBuilder.build();
            else
            {
                responseBuilder.setGranted(true);
                return responseBuilder.build();
            }
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.VoteResponse vote(RaftProto.VoteRequest request)
    {
        this.raftNode.getLock().lock();
        try
        {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(this.raftNode.getCurTerm());
            if (!RaftConfigurationUtil.containsServer(this.raftNode.getConfiguration(), request.getServerId()))
                return responseBuilder.build();
            if (request.getTerm() < this.raftNode.getCurTerm()) return responseBuilder.build();
            if (request.getTerm() > this.raftNode.getCurTerm()) this.raftNode.stepDown(request.getTerm());
            boolean isLogOK = request.getLastLogTerm() > this.raftNode.getLastLogTerm() || (request.getLastLogTerm() == this.raftNode.getLastLogTerm() && request.getLastLogIndex() >= this.raftNode.getRaftLog().getLastLogIndex());
            if (isLogOK && this.raftNode.getVoteFor() == 0)
            {
                this.raftNode.stepDown(request.getTerm());
                this.raftNode.setVoteFor(request.getServerId());
                this.raftNode.getRaftLog().updateMetaData(this.raftNode.getCurTerm(), this.raftNode.getVoteFor(), null, null);
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(this.raftNode.getCurTerm());
            }
            return responseBuilder.build();
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request)
    {
        if (log.isErrorEnabled() && CollUtil.isNotEmpty(request.getEntriesList()))
            log.error("{}为follower 收到来自{}这个leader的数据同步请求", this.raftNode.getLocalServer().getServerId(), request.getServerId());
        this.raftNode.getLock().lock();
        try
        {
            RaftProto.AppendEntriesResponse.Builder responseBuilder = RaftProto.AppendEntriesResponse.newBuilder();
            // leader的任期
            long leaderTerm = request.getTerm();
            // leader最新的日志任期
            long leaderLogTerm = request.getPrevLogTerm();
            // leader最新的日志索引
            long leaderLogIndex = request.getPrevLogIndex();
            // follower的任期
            long followerTerm = this.raftNode.getCurTerm();
            // follower日志的索引
            long followerLastLogIndex = this.raftNode.getRaftLog().getLastLogIndex();
            responseBuilder.setTerm(followerTerm);
            responseBuilder.setResCode(RaftProto.ResCode.FAIL);
            responseBuilder.setLastLogIndex(followerLastLogIndex);
            // leader的任期号比follower小
            if (leaderTerm < followerTerm) return responseBuilder.build();
            // 尝试看看当前follower是不是要降级
            this.raftNode.stepDown(leaderTerm);
            /**
             * 在集群每个节点上都维护leader的serverId信息
             * 0代表中没有维护leader的serverId 更新上这个信息
             */
            if (this.raftNode.getLeaderId() == 0) this.raftNode.setLeaderId(request.getServerId());
            // follower记录的leader id跟集群实际leader的id不一样 网络分区脑裂会导致这样的情况
            if (!Objects.equals(this.raftNode.getLeaderId(), request.getServerId()))
            {
                this.raftNode.stepDown(leaderTerm + 1);
                responseBuilder.setResCode(RaftProto.ResCode.FAIL);
                responseBuilder.setTerm(leaderTerm + 1);
                return responseBuilder.build();
            }
            // follower没有跟上leader的同步日志 返回resCode=false
            if (leaderLogIndex > followerLastLogIndex) return responseBuilder.build();
            if (leaderLogIndex >= this.raftNode.getRaftLog().getFirstLogIndex() && !Objects.equals(this.raftNode.getRaftLog().getEntryTerm(leaderLogIndex), leaderLogTerm))
            {
                if (leaderLogIndex > 0)
                {
                    responseBuilder.setLastLogIndex(leaderLogIndex - 1);
                    return responseBuilder.build();
                }
            }
            if (request.getEntriesCount() == 0)
            {
                // leader同步给follower的日志内容为空 说明发送的心跳包
                if (log.isInfoEnabled())
                    log.info("follower={}收到了心跳包 回复leader={}响应", this.raftNode.getLocalServer().getServerId(), request.getServerId());
                responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
                responseBuilder.setTerm(followerTerm);
                responseBuilder.setLastLogIndex(followerLastLogIndex);
                this.advanceCommitIndex(request);
                return responseBuilder.build();
            }
            responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long index = leaderLogIndex;
            for (RaftProto.LogEntry entry : request.getEntriesList())
            {
                index++;
                if (index < this.raftNode.getRaftLog().getFirstLogIndex()) continue;
                if (followerLastLogIndex >= index)
                {
                    if (Objects.equals(this.raftNode.getRaftLog().getEntryTerm(index), entry.getTerm())) continue;
                    long lastIndexKept = index - 1;
                    this.raftNode.getRaftLog().truncateSuffix(lastIndexKept);
                }
                entries.add(entry);
            }
            this.raftNode.getRaftLog().append(entries);
            responseBuilder.setLastLogIndex(followerLastLogIndex);
            this.advanceCommitIndex(request);
            return responseBuilder.build();
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }
    }

    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request)
    {
        RaftProto.InstallSnapshotResponse.Builder responseBuilder = RaftProto.InstallSnapshotResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.FAIL);
        this.raftNode.getLock().lock();
        try
        {
            responseBuilder.setTerm(this.raftNode.getCurTerm());
            if (request.getTerm() < this.raftNode.getCurTerm()) return responseBuilder.build();
            this.raftNode.stepDown(request.getTerm());
            if (this.raftNode.getLeaderId() == 0) this.raftNode.setLeaderId(request.getServerId());
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }
        if (this.raftNode.getSnapshot().getIsTakeSnapshot().get()) return responseBuilder.build();
        this.raftNode.getSnapshot().getIsTakeSnapshot().set(true);
        RandomAccessFile randomAccessFile = null;
        this.raftNode.getSnapshot().getLock().lock();
        try
        {
            String tmpSnapshotDir = this.raftNode.getSnapshot().getSnapshotDir() + FilePathConstant.Snapthot.Dir.SNAPSHOT_TMP;
            File file = new File(tmpSnapshotDir);
            if (request.getIsFirst())
            {
                if (file.exists()) file.delete();
                file.mkdir();
                this.raftNode.getSnapshot().updateMetaData(tmpSnapshotDir, request.getSnapshotMetaData().getLastIncludedIndex(), request.getSnapshotMetaData().getLastIncludedTerm(), request.getSnapshotMetaData().getConfiguration());
            }
            String curDataDirName = tmpSnapshotDir + File.separator + "data";
            File curDataDir = new File(curDataDirName);
            if (!curDataDir.exists()) curDataDir.mkdirs();
            String curDataFileName = curDataDirName + File.separator + request.getFileName();
            File curDataFile = new File(curDataFileName);
            if (!curDataFile.getParentFile().exists()) curDataFile.getParentFile().mkdirs();
            if (!curDataFile.exists()) curDataFile.createNewFile();
            randomAccessFile = FileUtil.open(tmpSnapshotDir + File.separator + "data", request.getFileName(), FileMode.rw);
            randomAccessFile.seek(request.getOffset());
            randomAccessFile.write(request.getData().toByteArray());
            if (request.getIsLast())
            {
                File snapshotDirFile = new File(this.raftNode.getSnapshot().getSnapshotDir());
                if (snapshotDirFile.exists()) cn.hutool.core.io.FileUtil.del(snapshotDirFile);
                cn.hutool.core.io.FileUtil.move(new File(tmpSnapshotDir), snapshotDirFile, true);
            }
            responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
        }
        catch (Exception ignored)
        {
        }
        finally
        {
            FileUtil.close(randomAccessFile);
            this.raftNode.getLock().unlock();
        }

        if (request.getIsLast() && Objects.equals(responseBuilder.getResCode(), RaftProto.ResCode.SUCCESS))
        {
            // classpath:data/snapshot/data
            String snapshotDataDir = this.raftNode.getSnapshot().getSnapshotDir() + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_DATA;
            this.raftNode.getStateMachine().readSnapshot(snapshotDataDir);
            long lastSnapshotIndex;
            this.raftNode.getSnapshot().getLock().lock();
            try
            {
                this.raftNode.getSnapshot().reload();
                lastSnapshotIndex = this.raftNode.getSnapshot().getMetaData().getLastIncludedIndex();
            }
            finally
            {
                this.raftNode.getSnapshot().getLock().unlock();
            }
            this.raftNode.getLock().lock();
            try
            {
                this.raftNode.getRaftLog().truncatePrefix(lastSnapshotIndex + 1);
            }
            finally
            {
                this.raftNode.getLock().unlock();
            }
        }
        if (request.getIsLast()) this.raftNode.getSnapshot().getIsInstallSnapshot().set(false);
        return responseBuilder.build();
    }

    private void advanceCommitIndex(RaftProto.AppendEntriesRequest request)
    {
        long newCommitIndex = Math.min(request.getCommitIndex(), request.getPrevLogIndex() + request.getEntriesCount());
        this.raftNode.setCommitIndex(newCommitIndex);
        this.raftNode.getRaftLog().updateMetaData(null, null, null, newCommitIndex);
        if (this.raftNode.getLastAppliedIndex() < this.raftNode.getCommitIndex())
        {
            for (long index = this.raftNode.getLastAppliedIndex() + 1; index <= this.raftNode.getCommitIndex(); index++)
            {
                RaftProto.LogEntry entry = this.raftNode.getRaftLog().getEntry(index);
                if (Objects.isNull(entry)) continue;
                if (Objects.equals(entry.getType(), RaftProto.EntryType.DATA))
                    this.raftNode.getStateMachine().apply(entry.getData().toByteArray());
                else if (Objects.equals(entry.getType(), RaftProto.EntryType.CONFIGURATION))
                    this.raftNode.applyConfiguration(entry);
                this.raftNode.setLastAppliedIndex(index);
            }
        }
    }
}
