package com.github.bannirui.raft.core.service.impl;

import cn.hutool.core.io.file.FileMode;
import com.github.bannirui.raft.bean.constant.FilePathConstant;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.common.util.FileUtil;
import com.github.bannirui.raft.common.util.RaftConfigurationUtil;
import com.github.bannirui.raft.core.Node;
import com.github.bannirui.raft.core.service.ConsensusService;

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
public class ConsensusServiceImpl implements ConsensusService
{

    private Node node;

    public ConsensusServiceImpl(Node node)
    {
        this.node = node;
    }

    @Override
    public RaftProto.VoteResponse preVote(RaftProto.VoteRequest request)
    {
        this.node.getLock().lock();
        try
        {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(this.node.getCurrentTerm());
            if (!RaftConfigurationUtil.containsServer(this.node.getConfiguration(), request.getServerId()))
                return responseBuilder.build();
            if (request.getTerm() < this.node.getCurrentTerm()) return responseBuilder.build();
            boolean isLogOk = request.getLastLogTerm() > this.node.getLastLogTerm() || (request.getLastLogTerm() == this.node.getLastLogTerm() && request.getLastLogIndex() >= this.node.getRaftLog().getLastLogIndex());
            if (!isLogOk) return responseBuilder.build();
            else
            {
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(this.node.getCurrentTerm());
            }
            return responseBuilder.build();
        }
        finally
        {
            this.node.getLock().unlock();
        }
    }

    @Override
    public RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request)
    {
        this.node.getLock().lock();
        try
        {
            RaftProto.VoteResponse.Builder responseBuilder = RaftProto.VoteResponse.newBuilder();
            responseBuilder.setGranted(false);
            responseBuilder.setTerm(this.node.getCurrentTerm());
            if (!RaftConfigurationUtil.containsServer(this.node.getConfiguration(), request.getServerId()))
                return responseBuilder.build();
            if (request.getTerm() < this.node.getCurrentTerm()) return responseBuilder.build();
            if (request.getTerm() > this.node.getCurrentTerm()) this.node.stepDown(request.getTerm());
            boolean isLogOK = request.getLastLogTerm() > this.node.getLastLogTerm() || (request.getLastLogTerm() == this.node.getLastLogTerm() && request.getLastLogIndex() >= this.node.getRaftLog().getLastLogIndex());
            if (isLogOK && this.node.getVoteFor() == 0)
            {
                this.node.stepDown(request.getTerm());
                this.node.setVoteFor(request.getServerId());
                this.node.getRaftLog().updateMetaData(this.node.getCurrentTerm(), this.node.getVoteFor(), null, null);
                responseBuilder.setGranted(true);
                responseBuilder.setTerm(this.node.getCurrentTerm());
            }
            return responseBuilder.build();
        }
        finally
        {
            this.node.getLock().unlock();
        }
    }

    @Override
    public RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request)
    {
        this.node.getLock().lock();
        try
        {
            RaftProto.AppendEntriesResponse.Builder responseBuilder = RaftProto.AppendEntriesResponse.newBuilder();
            responseBuilder.setTerm(this.node.getCurrentTerm());
            responseBuilder.setResCode(RaftProto.ResCode.FAIL);
            responseBuilder.setLastLogIndex(this.node.getRaftLog().getLastLogIndex());
            if (request.getTerm() < this.node.getCurrentTerm()) return responseBuilder.build();
            this.node.stepDown(request.getTerm());
            if (this.node.getLeaderId() == 0) this.node.setLeaderId(request.getServerId());
            if (!Objects.equals(this.node.getLeaderId(), request.getServerId()))
            {
                this.node.stepDown(request.getTerm() + 1);
                responseBuilder.setResCode(RaftProto.ResCode.FAIL);
                responseBuilder.setTerm(request.getTerm() + 1);
                return responseBuilder.build();
            }
            if (request.getPrevLogIndex() > this.node.getRaftLog().getLastLogIndex()) return responseBuilder.build();
            if (request.getPrevLogIndex() >= this.node.getRaftLog().getFirstLogIndex() && !Objects.equals(this.node.getRaftLog().getEntryTerm(request.getPrevLogIndex()), request.getPrevLogTerm()))
            {
                if (request.getPrevLogIndex() > 0)
                {
                    responseBuilder.setLastLogIndex(request.getPrevLogIndex() - 1);
                    return responseBuilder.build();
                }
            }
            if (request.getEntriesCount() == 0)
            {
                responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
                responseBuilder.setTerm(this.node.getCurrentTerm());
                responseBuilder.setLastLogIndex(this.node.getRaftLog().getLastLogIndex());
                this.advanceCommitIndex(request);
                return responseBuilder.build();
            }
            responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
            List<RaftProto.LogEntry> entries = new ArrayList<>();
            long index = request.getPrevLogIndex();
            for (RaftProto.LogEntry entry : request.getEntriesList())
            {
                index++;
                if (index < this.node.getRaftLog().getFirstLogIndex()) continue;
                if (this.node.getRaftLog().getLastLogIndex() >= index)
                {
                    if (Objects.equals(this.node.getRaftLog().getEntryTerm(index), entry.getTerm())) continue;
                    long lastIndexKept = index - 1;
                    this.node.getRaftLog().truncateSuffix(lastIndexKept);
                }
                entries.add(entry);
            }
            this.node.getRaftLog().append(entries);
            responseBuilder.setLastLogIndex(this.node.getRaftLog().getLastLogIndex());
            this.advanceCommitIndex(request);
            return responseBuilder.build();
        }
        finally
        {
            this.node.getLock().unlock();
        }
    }

    @Override
    public RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request)
    {
        RaftProto.InstallSnapshotResponse.Builder responseBuilder = RaftProto.InstallSnapshotResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.FAIL);
        this.node.getLock().lock();
        try
        {
            responseBuilder.setTerm(this.node.getCurrentTerm());
            if (request.getTerm() < this.node.getCurrentTerm()) return responseBuilder.build();
            this.node.stepDown(request.getTerm());
            if (this.node.getLeaderId() == 0) this.node.setLeaderId(request.getServerId());
        }
        finally
        {
            this.node.getLock().unlock();
        }
        if (this.node.getSnapshot().getIsTakeSnapshot().get()) return responseBuilder.build();
        this.node.getSnapshot().getIsTakeSnapshot().set(true);
        RandomAccessFile randomAccessFile = null;
        this.node.getSnapshot().getLock().lock();
        try
        {
            String tmpSnapshotDir = this.node.getSnapshot().getSnapshotDir() + FilePathConstant.Snapthot.Dir.SNAPSHOT_TMP;
            File file = new File(tmpSnapshotDir);
            if (request.getIsFirst())
            {
                if (file.exists()) file.delete();
                file.mkdir();
                this.node.getSnapshot().updateMetaData(tmpSnapshotDir, request.getSnapshotMetaData().getLastIncludedIndex(), request.getSnapshotMetaData().getLastIncludedTerm(), request.getSnapshotMetaData().getConfiguration());
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
                File snapshotDirFile = new File(this.node.getSnapshot().getSnapshotDir());
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
            this.node.getLock().unlock();
        }

        if (request.getIsLast() && Objects.equals(responseBuilder.getResCode(), RaftProto.ResCode.SUCCESS))
        {
            // classpath:data/snapshot/data
            String snapshotDataDir = this.node.getSnapshot().getSnapshotDir() + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_DATA;
            this.node.getStateMachine().readSnapshot(snapshotDataDir);
            long lastSnapshotIndex;
            this.node.getSnapshot().getLock().lock();
            try
            {
                this.node.getSnapshot().reload();
                lastSnapshotIndex = this.node.getSnapshot().getMetaData().getLastIncludedIndex();
            }
            finally
            {
                this.node.getSnapshot().getLock().unlock();
            }
            this.node.getLock().lock();
            try
            {
                this.node.getRaftLog().truncatePrefix(lastSnapshotIndex + 1);
            }
            finally
            {
                this.node.getLock().unlock();
            }
        }
        if (request.getIsLast()) this.node.getSnapshot().getIsInstallSnapshot().set(false);
        return responseBuilder.build();
    }

    private void advanceCommitIndex(RaftProto.AppendEntriesRequest request)
    {
        long newCommitIndex = Math.min(request.getCommitIndex(), request.getPrevLogIndex() + request.getEntriesCount());
        this.node.setCommitIndex(newCommitIndex);
        this.node.getRaftLog().updateMetaData(null, null, null, newCommitIndex);
        if (this.node.getLastAppliedIndex() < this.node.getCommitIndex())
        {
            for (long index = this.node.getLastAppliedIndex() + 1; index <= this.node.getCommitIndex(); index++)
            {
                RaftProto.LogEntry entry = this.node.getRaftLog().getEntry(index);
                if (Objects.isNull(entry)) continue;
                if (Objects.equals(entry.getType(), RaftProto.EntryType.DATA))
                    this.node.getStateMachine().apply(entry.getData().toByteArray());
                else if (Objects.equals(entry.getType(), RaftProto.EntryType.CONFIGURATION))
                    this.node.applyConfiguration(entry);
                this.node.setLastAppliedIndex(index);
            }
        }
    }
}
