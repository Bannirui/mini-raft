package com.github.bannirui.raft.core.service.impl;

import cn.hutool.core.collection.CollUtil;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.common.util.RaftConfigurationUtil;
import com.github.bannirui.raft.core.RaftNode;
import com.github.bannirui.raft.core.Peer;
import com.github.bannirui.raft.core.service.ClientService;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @since 2022/4/6
 * @author dingrui
 */
public class ClientServiceImpl implements ClientService
{

    private RaftNode raftNode;

    public ClientServiceImpl(RaftNode raftNode)
    {
        this.raftNode = raftNode;
    }

    @Override
    public RaftProto.GetLeaderResponse getLeader(RaftProto.GetLeaderRequest request)
    {
        RaftProto.GetLeaderResponse.Builder responseBuilder = RaftProto.GetLeaderResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
        RaftProto.Endpoint.Builder endpointBuilder = RaftProto.Endpoint.newBuilder();
        this.raftNode.getLock().lock();
        try
        {
            int leaderId = this.raftNode.getLeaderId();
            if (leaderId == 0) responseBuilder.setResCode(RaftProto.ResCode.FAIL);
            else if (Objects.equals(leaderId, this.raftNode.getLocalServer().getServerId()))
            {
                endpointBuilder.setHost(this.raftNode.getLocalServer().getEndpoint().getHost());
                endpointBuilder.setPort(this.raftNode.getLocalServer().getEndpointOrBuilder().getPort());
            }
            else
            {
                RaftProto.Configuration configuration = this.raftNode.getConfiguration();
                for (RaftProto.Server server : configuration.getServersList())
                {
                    if (Objects.equals(server.getServerId(), leaderId))
                    {
                        endpointBuilder.setHost(server.getEndpoint().getHost());
                        endpointBuilder.setPort(server.getEndpoint().getPort());
                        break;
                    }
                }
            }
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }
        responseBuilder.setLeader(endpointBuilder.build());
        return responseBuilder.build();
    }

    @Override
    public RaftProto.GetConfigurationResponse getConfiguration(RaftProto.GetConfigurationRequest request)
    {
        RaftProto.GetConfigurationResponse.Builder responseBuilder = RaftProto.GetConfigurationResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
        this.raftNode.getLock().lock();
        try
        {
            RaftProto.Configuration configuration = this.raftNode.getConfiguration();
            List<RaftProto.Server> serversList = null;
            if (Objects.nonNull(configuration) && CollUtil.isNotEmpty(serversList = configuration.getServersList()))
            {
                RaftProto.Server leader = serversList.stream().filter(Objects::nonNull).filter(e -> Objects.equals(e.getServerId(), this.raftNode.getLeaderId())).findAny().orElseGet(() -> null);
                responseBuilder.setLeader(leader);
                responseBuilder.addAllServers(serversList);
            }
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }
        return responseBuilder.build();
    }

    @Override
    public RaftProto.AddPeersResponse addPeers(RaftProto.AddPeersRequest request)
    {
        if (Objects.isNull(request)) return null;
        RaftProto.AddPeersResponse.Builder responseBuilder = RaftProto.AddPeersResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.FAIL);
        int serversCount;
        if ((serversCount = request.getServersCount()) == 0 || serversCount % 2 != 0)
        {
            responseBuilder.setResMsg("added server's size can only be multiple of 2");
            return responseBuilder.build();
        }
        for (RaftProto.Server server : request.getServersList())
        {
            if (this.raftNode.getPeerMap().containsKey(server.getServerId()))
            {
                responseBuilder.setResMsg("already be added to configuration");
                return responseBuilder.build();
            }
        }
        List<Peer> requestPeers = new ArrayList<>(serversCount);
        for (RaftProto.Server server : request.getServersList())
        {
            Peer peer = new Peer(server);
            peer.setNextIndex(1);
            requestPeers.add(peer);
            this.raftNode.getPeerMap().putIfAbsent(server.getServerId(), peer);
            this.raftNode.getExecutorService().submit(() -> this.raftNode.appendEntries(peer));
        }
        int catchupNum = 0;
        this.raftNode.getLock().lock();
        try
        {
            while (catchupNum < requestPeers.size())
            {
                try
                {
                    this.raftNode.getCatchUpCondition().await();
                }
                catch (Exception ignored)
                {
                }
                catchupNum = (int) (requestPeers.stream().filter(Peer::isCatchUp).count());
                if (catchupNum == requestPeers.size()) break;
            }
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }

        if (catchupNum == requestPeers.size())
        {
            this.raftNode.getLock().lock();
            byte[] configurationData;
            RaftProto.Configuration newConfiguration;
            try
            {
                newConfiguration = RaftProto.Configuration.newBuilder(this.raftNode.getConfiguration()).addAllServers(request.getServersList()).build();
                configurationData = newConfiguration.toByteArray();
            }
            finally
            {
                this.raftNode.getLock().unlock();
            }
            if (this.raftNode.replicate(configurationData, RaftProto.EntryType.CONFIGURATION))
                responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
        }
        if (!Objects.equals(responseBuilder.getResCode(), RaftProto.ResCode.SUCCESS))
        {
            this.raftNode.getLock().lock();
            try
            {
                for (Peer peer : requestPeers)
                {
                    peer.getRpcClient().stop();
                    this.raftNode.getPeerMap().remove(peer.getServer().getServerId());
                }
            }
            finally
            {
                this.raftNode.getLock().unlock();
            }
        }
        return responseBuilder.build();
    }

    @Override
    public RaftProto.RemovePeersResponse removePeers(RaftProto.RemovePeersRequest request)
    {
        RaftProto.RemovePeersResponse.Builder responseBuilder = RaftProto.RemovePeersResponse.newBuilder();
        responseBuilder.setResCode(RaftProto.ResCode.FAIL);
        int serversCount = request.getServersCount();
        if (serversCount == 0 || serversCount % 2 != 0)
        {
            responseBuilder.setResMsg("removed server's size can only multiple of 2");
            return responseBuilder.build();
        }
        this.raftNode.getLock().lock();
        try
        {
            for (RaftProto.Server server : request.getServersList())
            {
                boolean exist = this.raftNode.getConfiguration().getServersList().stream().filter(Objects::nonNull).anyMatch(e -> Objects.equals(e.getServerId(), server.getServerId()));
                if (!exist) return responseBuilder.build();
            }
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }

        this.raftNode.getLock().lock();
        RaftProto.Configuration newConfiguration;
        byte[] configurationData;
        try
        {
            newConfiguration = RaftConfigurationUtil.removeServers(this.raftNode.getConfiguration(), request.getServersList());
            configurationData = newConfiguration.toByteArray();
        }
        finally
        {
            this.raftNode.getLock().unlock();
        }
        if (this.raftNode.replicate(configurationData, RaftProto.EntryType.CONFIGURATION))
            responseBuilder.setResCode(RaftProto.ResCode.SUCCESS);
        return responseBuilder.build();
    }
}
