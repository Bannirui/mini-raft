package com.github.bannirui.raft.core;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.core.service.async.AsyncConsensusService;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * <p>服务器节点的封装<ul>
 *     <li>服务器信息</li>
 *     <li>该服务器人rpc客户端</li>
 *     <li>该服务器暴露的rpc服务</li>
 * </ul>
 * 集群中区别本机和其他节点 将其他节点信息用{@link Peer}封装起来放到{@link java.util.Map}中</p>
 * @since 2022/4/4
 * @author dingrui
 */
@Getter
@Setter
public class Peer
{
    /**
     * 节点信息
     */
    private RaftProto.Server server;

    /**
     * 该节点的rpc客户端
     */
    private RpcClient rpcClient;

    /**
     * 该节点暴露的rpc服务
     * 集群间互相通信的rpc
     */
    private AsyncConsensusService consensusService;

    /**
     * leader需要发送给follower的下一个日志条目的索引值
     * 该域只对leader节点才有效
     */
    private long nextIndex;

    /**
     * 已经复制日志的最高索引值
     */
    private long matchIndex;

    /**
     * 在preVote和vote的时候使用
     * 标识候选人有没有得到投票人的上票
     */
    private volatile Boolean voteGranted;

    /**
     * 标识follower同步的日志是否跟上了leader
     * [0...diff] 集群容忍leader和follower日志信息存在一定的滞后性 该区间就是leader和follower的滞后容忍区间
     */
    private volatile boolean isCatchUp;

    public Peer(RaftProto.Server server)
    {
        if (Objects.isNull(server)) return;
        this.server = server;
        this.rpcClient = new RpcClient(new Endpoint(server.getEndpoint().getHost(), server.getEndpoint().getPort()));
        this.consensusService = BrpcProxy.getProxy(this.rpcClient, AsyncConsensusService.class);
        this.isCatchUp = false;
    }
}
