package com.github.bannirui.raft.core.service.async;

import com.baidu.brpc.client.RpcCallback;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.core.service.ClientService;

import java.util.concurrent.Future;

/**
 * <p>生成client异步调用所需的proxy {@link ClientService}</p>
 * @since 2022/4/4
 * @author dingrui
 */
public interface AsyncClientService extends ClientService
{
    /**
     * <p>{@link ClientService#getLeader}</p>
     * @since 2022/4/4
     * @author dingrui
     * @return java.util.concurrent.Future<com.github.bannirui.raft.bean.proto.RaftProto.GetLeaderResponse>
     */
    Future<RaftProto.GetLeaderResponse> getLeader(RaftProto.GetLeaderRequest request, RpcCallback<RaftProto.GetLeaderResponse> callback);

    /**
     * <P>{@link ClientService#getConfiguration}</P>
     * @since 2022/4/4
     * @author dingrui
     * @return java.util.concurrent.Future<com.github.bannirui.raft.bean.proto.RaftProto.GetConfigurationResponse>
     */
    Future<RaftProto.GetConfigurationResponse> getConfiguration(RaftProto.GetConfigurationRequest request, RpcCallback<RaftProto.GetConfigurationResponse> callback);

    /**
     * <p>{@link ClientService#addPeers}</p>
     * @since 2022/4/4
     * @author dingrui
     * @return java.util.concurrent.Future<com.github.bannirui.raft.bean.proto.RaftProto.AddPeersResponse>
     */
    Future<RaftProto.AddPeersResponse> addPeers(RaftProto.AddPeersRequest request, RpcCallback<RaftProto.AddPeersResponse> callback);

    /**
     * <p>{@link ClientService#removePeers}</p>
     * @since 2022/4/4
     * @author dingrui
     * @return java.util.concurrent.Future<com.github.bannirui.raft.bean.proto.RaftProto.RemovePeersResponse>
     */
    Future<RaftProto.RemovePeersResponse> removePeers(RaftProto.RemovePeersRequest request, RpcCallback<RaftProto.RemovePeersResponse> callback);
}
