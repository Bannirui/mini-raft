package com.github.bannirui.raft.core;

import cn.hutool.core.util.StrUtil;
import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.core.service.async.AsyncConsensusService;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
@Getter
@Setter
public class Peer
{
    private RaftProto.Server server;
    private RpcClient rpcClient;
    private AsyncConsensusService asyncConsensusService;

    /**
     * 需要发送给follower的下一个日志条目的索引值
     * 该域只对leader节点才有效
     */
    private long nextIndex;

    /**
     * 已经复制日志的最高索引值
     */
    private long matchIndex;

    private volatile Boolean voteGranted;

    private volatile boolean isCatchUp;

    public Peer(RaftProto.Server server)
    {
        RaftProto.Endpoint endpoint = null;
        String host = null;
        Integer port = null;
        if (Objects.isNull(server) || Objects.isNull(endpoint = server.getEndpoint()) || StrUtil.isBlank(host = endpoint.getHost()) || Objects.isNull(port = endpoint.getPort()))
            return;
        this.server = server;
        this.rpcClient = new RpcClient(new Endpoint(host, port));
        this.asyncConsensusService = BrpcProxy.getProxy(this.rpcClient, AsyncConsensusService.class);
        this.isCatchUp = false;
    }
}
