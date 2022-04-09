package com.github.bannirui.raft.core.service;

import com.github.bannirui.raft.bean.proto.RaftProto;

/**
 * <p>raft节点之间通信接口</p>
 * @since 2022/4/4
 * @author dingrui
 */
public interface ConsensusService
{
    /**
     * <p>接受拉票请求
     * 投票人考察拉票人的信息<ul>
     *     <li>候选人的任期</li>
     *     <li>候选人的数据</li>
     * </ul>
     * 决定是否要给候选人上票</p>
     * @since 2022/4/8
     * @author dingrui
     * @param request 拉票人信息
     * @return com.github.bannirui.raft.bean.proto.RaftProto.VoteResponse
     */
    RaftProto.VoteResponse preVote(RaftProto.VoteRequest request);

    RaftProto.VoteResponse vote(RaftProto.VoteRequest request);

    /**
     * <p>leader向follower同步日志</p>
     * <p>该方法实现在raft集群节点的follower上</p>
     * @since 2022/4/8
     * @author dingrui
     * @param request 请求中包含了要同步的日志信息 当前request中entries是空的话表示是leader发送的心跳包
     * @return com.github.bannirui.raft.bean.proto.RaftProto.AppendEntriesResponse
     */
    RaftProto.AppendEntriesResponse appendEntries(RaftProto.AppendEntriesRequest request);

    RaftProto.InstallSnapshotResponse installSnapshot(RaftProto.InstallSnapshotRequest request);
}
