package com.github.bannirui.raft.core.service;

import com.github.bannirui.raft.bean.proto.RaftProto;

/**
 * <p>raft集群管理接口</p>
 * @since 2022/4/4
 * @author dingrui
 */
public interface ClientService
{
    /**
     * <p>获取raft集群leader节点信息</p>
     * @since 2022/4/4
     * @author dingrui
     * @param request 请求
     * @return com.github.bannirui.raft.bean.proto.RaftProto.GetLeaderResponse leader节点
     */
    RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request);

    /**
     * <p>获取raft集群所有节点信息</p>
     * @since 2022/4/4
     * @author dingrui
     * @param request 请求
     * @return com.github.bannirui.raft.bean.proto.RaftProto.GetConfigurationResponse raft集群各节点地址 以及主从关系
     */
    RaftProto.GetConfigurationResponse getConfiguration(RaftProto.GetConfigurationRequest request);

    /**
     * <p>向raft集群添加节点</p>
     * @since 2022/4/4
     * @author dingrui
     * @param request 要添加的节点信息
     * @return com.github.bannirui.raft.bean.proto.RaftProto.AddPeersResponse
     */
    RaftProto.AddPeersResponse addPeers(RaftProto.AddPeersRequest request);

    /**
     * <p>从raft集群删除节点</p>
     * @since 2022/4/4
     * @author dingrui
     * @param request 要移除的节点信息
     * @return com.github.bannirui.raft.bean.proto.RaftProto.RemovePeersResponse
     */
    RaftProto.RemovePeersResponse removePeers(RaftProto.RemovePeersRequest request);
}
