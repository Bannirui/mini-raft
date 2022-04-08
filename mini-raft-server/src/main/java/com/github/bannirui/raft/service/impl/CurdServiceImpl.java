package com.github.bannirui.raft.service.impl;

import com.baidu.brpc.client.BrpcProxy;
import com.baidu.brpc.client.RpcClient;
import com.baidu.brpc.client.RpcClientOptions;
import com.baidu.brpc.client.instance.Endpoint;
import com.github.bannirui.raft.api.bean.proto.CurdProto;
import com.github.bannirui.raft.api.service.CurdService;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.core.Node;
import com.github.bannirui.raft.core.Peer;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
public class CurdServiceImpl implements CurdService
{

    private Node node;
    private StateMachineImpl stateMachine;
    private int leaderId = -1;
    private RpcClient leaderRpcClient = null;
    private Lock leaderLock = new ReentrantLock();

    public CurdServiceImpl(Node node, StateMachineImpl stateMachine)
    {
        this.node = node;
        this.stateMachine = stateMachine;
    }

    @Override
    public CurdProto.SetResponse set(CurdProto.SetRequest request)
    {
        CurdProto.SetResponse.Builder responseBuilder = CurdProto.SetResponse.newBuilder();
        if (this.node.getLeaderId() <= 0) responseBuilder.setSuccess(false);
        else if (!Objects.equals(this.node.getLeaderId(), this.node.getLocalServer().getServerId()))
        {
            this.onLeaderChangeEvent();
            CurdService curdService = BrpcProxy.getProxy(this.leaderRpcClient, CurdService.class);
            CurdProto.SetResponse responseFromLeader = curdService.set(request);
            responseBuilder.mergeFrom(responseFromLeader);
        }
        else
        {
            byte[] data = request.toByteArray();
            boolean success = this.node.replicate(data, RaftProto.EntryType.DATA);
            responseBuilder.setSuccess(success);
        }
        return responseBuilder.build();
    }

    @Override
    public CurdProto.GetResponse get(CurdProto.GetRequest request)
    {
        return this.stateMachine.get(request);
    }

    private void onLeaderChangeEvent()
    {
        if (!Objects.equals(this.node.getLeaderId(), -1) && !Objects.equals(this.node.getLeaderId(), this.node.getLocalServer().getServerId()) && !Objects.equals(this.leaderId, this.node.getLeaderId()))
        {
            this.leaderLock.lock();
            try
            {
                if (!Objects.equals(-1, leaderId) && Objects.nonNull(this.leaderRpcClient))
                {
                    this.leaderRpcClient.stop();
                    this.leaderRpcClient = null;
                    this.leaderId = -1;
                }
                this.leaderId = this.node.getLeaderId();
                Peer peer = this.node.getPeerMap().get(this.leaderId);
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
}
