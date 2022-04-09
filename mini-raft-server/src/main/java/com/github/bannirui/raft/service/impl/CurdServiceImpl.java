package com.github.bannirui.raft.service.impl;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.bannirui.raft.api.bean.proto.CurdProto;
import com.github.bannirui.raft.api.service.CurdService;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.core.RaftNode;
import com.github.bannirui.raft.core.Peer;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
@Slf4j
public class CurdServiceImpl implements CurdService
{

    private RaftNode raftNode;
    private StateMachineImpl stateMachine;

    /**
     * 每个服务实例都维护集群的leader serverId
     * -1表示没有leader
     */
    private int leaderId = -1;

    /**
     * 集群中leader的rpc服务
     * rpc服务本身的负载会将请求路由到集群中的节点 但是这个负载策略跟节点角色没关系
     * 有可能将请求打到leader节点 也有可能将请求打到follower节点 因此需要维护集群中leader的信息 将follower收到的请求转发给leader
     */
    private RpcClient leaderRpcClient = null;
    private Lock leaderLock = new ReentrantLock();

    public CurdServiceImpl(RaftNode raftNode, StateMachineImpl stateMachine)
    {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    @Override
    public CurdProto.SetResponse put(CurdProto.SetRequest request)
    {
        if (log.isErrorEnabled())
            log.error("节点{} 收到put请求 key={}", this.raftNode.getLocalServer().getServerId(), request.getKey());
        CurdProto.SetResponse.Builder responseBuilder = CurdProto.SetResponse.newBuilder();
        // 该集群没有leader 不提供client端的读写请求
        if (this.raftNode.getLeaderId() <= 0)
        {
            if (log.isErrorEnabled())
                log.error("节点{} 维护在当前节点的leaderId=-1 表示集群没有leader 不提供客户端服务", this.raftNode.getLocalServer().getServerId());
            responseBuilder.setSuccess(false);
        }
        else if (!Objects.equals(this.raftNode.getLeaderId(), this.raftNode.getLocalServer().getServerId()))
        {
            // 当前节点不是leader 将请求转发给leader节点
            if (log.isErrorEnabled())
                log.error("节点{} 集群leader={} 当前节点不是leader 将请求转发给leader", this.raftNode.getLocalServer().getServerId(), this.raftNode.getLeaderId());
            // follower转发请求给leader之前 防止维护的leader信息已经过时 先尝试更新leader信息
            this.onLeaderChangeEvent();
            // 拿到leader rpc的代理
            CurdService curdService = BrpcProxy.getProxy(this.leaderRpcClient, CurdService.class);
            CurdProto.SetResponse responseFromLeader = curdService.put(request);
            responseBuilder.mergeFrom(responseFromLeader);
        }
        else
        {
            // 当前节点是leader节点
            byte[] data = request.toByteArray();
            boolean success = this.raftNode.replicate(data, RaftProto.EntryType.DATA);
            responseBuilder.setSuccess(success);
            if (log.isErrorEnabled())
                log.error("节点{} 是leader 处理put请求 key={} value={} 请求处理结果{}", this.raftNode.getLocalServer().getServerId(), request.getKey(), request.getValue(), success);
        }
        return responseBuilder.build();
    }

    @Override
    public CurdProto.GetResponse get(CurdProto.GetRequest request)
    {
        if (log.isErrorEnabled())
            log.error("节点{} 收到get请求 key={}", this.raftNode.getLocalServer().getServerId(), request.getKey());
        return this.stateMachine.get(request);
    }

    /**
     * <p>follower将请求转发给leader之前 防止自己维护的leader信息已经过期 尝试更新leader信息</p>
     * @since 2022/4/8
     * @author dingrui
     * @return void
     */
    private void onLeaderChangeEvent()
    {
        // 集群没有leader或者当前节点就是leader 都直接返回
        if (Objects.equals(this.raftNode.getLeaderId(), -1) || Objects.equals(this.raftNode.getLeaderId(), this.raftNode.getLocalServer().getServerId()) || Objects.equals(this.leaderId, this.raftNode.getLeaderId()))
            return;
        this.leaderLock.lock();
        try
        {
            // 如果已经维护了leader信息就先删除
            if (!Objects.equals(-1, leaderId) && Objects.nonNull(this.leaderRpcClient))
            {
                this.leaderRpcClient.stop();
                this.leaderRpcClient = null;
                this.leaderId = -1;
            }
            // 更新集群中leader的信息
            this.leaderId = this.raftNode.getLeaderId();
            Peer peer = this.raftNode.getPeerMap().get(this.leaderId);
            Endpoint endpoint = new Endpoint(peer.getServer().getEndpoint().getHost(), peer.getServer().getEndpoint().getPort());
            RpcClientOptions rpcClientOptions = new RpcClientOptions();
            rpcClientOptions.setGlobalThreadPoolSharing(true);
            this.leaderRpcClient = new RpcClient(endpoint, rpcClientOptions);
        }
        finally
        {
            this.leaderLock.unlock();
        }
    }
}
