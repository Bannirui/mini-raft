package com.github.bannirui.raft.core.service;

import com.github.bannirui.raft.bean.proto.RaftProto;

/**
 * <p>raft节点之间通信接口</p>
 * @since 2022/4/4
 * @author dingrui
 */
public interface ConsensusService
{
    RaftProto.VoteResponse preVote(RaftProto.VoteRequest request);

    RaftProto.VoteResponse requestVote(RaftProto.VoteRequest request);

    RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request);

    RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request);
}
