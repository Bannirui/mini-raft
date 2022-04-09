package com.github.bannirui.raft.core.service.async;

import com.baidu.brpc.client.RpcCallback;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.core.service.ConsensusService;

import java.util.concurrent.Future;

/**
 * <p>用于生成client异步调用所需的proxy</p>
 * @since 2022/4/4
 * @author dingrui
 */
public interface AsyncConsensusService extends ConsensusService
{
    /**
     * <p>{@link ConsensusService#preVote}</p>
     * @since 2022/4/4
     * @author dingrui
     * @return java.util.concurrent.Future<com.github.bannirui.raft.bean.proto.RaftProto.VoteResponse>
     */
    Future<RaftProto.VoteResponse> preVote(RaftProto.VoteRequest request, RpcCallback<RaftProto.VoteResponse> callback);

    /**
     * <p>{@link ConsensusService#vote}</p>
     * @since 2022/4/4
     * @author dingrui
     * @return java.util.concurrent.Future<com.github.bannirui.raft.bean.proto.RaftProto.VoteResponse>
     */
    Future<RaftProto.VoteResponse> vote(RaftProto.VoteRequest request, RpcCallback<RaftProto.VoteResponse> callback);

    /**
     * <p>{@link ConsensusService#appendEntries}</p>
     * @since 2022/4/4
     * @author dingrui
     * @return java.util.concurrent.Future<com.github.bannirui.raft.bean.proto.RaftProto.AppendEntriesResponse>
     */
    Future<RaftProto.AppendEntriesResponse> appendEntries(RaftProto.AppendEntriesRequest request, RpcCallback<RaftProto.AppendEntriesResponse> callback);

    /**
     * <p>{@link ConsensusService#installSnapshot}</p>
     * @since 2022/4/4
     * @author dingrui
     * @return java.util.concurrent.Future<com.github.bannirui.raft.bean.proto.RaftProto.InstallSnapshotResponse>
     */
    Future<RaftProto.InstallSnapshotResponse> installSnapshot(RaftProto.InstallSnapshotRequest request, RpcCallback<RaftProto.InstallSnapshotResponse> callback);
}
