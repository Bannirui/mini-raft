package com.github.bannirui.raft.bootstrap;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.baidu.brpc.server.RpcServer;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.core.Node;
import com.github.bannirui.raft.core.Option;
import com.github.bannirui.raft.core.service.impl.ClientServiceImpl;
import com.github.bannirui.raft.core.service.impl.ConsensusServiceImpl;
import com.github.bannirui.raft.service.impl.CurdServiceImpl;
import com.github.bannirui.raft.service.impl.StateMachineImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
@Component
public class RaftServerBootstrap
{

    /**
     * 配置文件
     * 当前单点服务器
     */
    @Value("${app.server}")
    private String server;

    /**
     * 配置文件
     * 当前单点服务器日志存储路径
     */
    @Value("${app.dataPath}")
    private String dataPath;

    /**
     * 配置文件
     * 集群服务器
     */
    @Value("${app.cluster}")
    private String cluster;

    private List<RaftProto.Server> clusterServer;
    private RaftProto.Server localServer;

    @PostConstruct
    public void init()
    {
        this.clusterServer = this.parseCluster(this.cluster);
        this.localServer = this.parseServer(this.server);
        Integer port = null;
        if (CollUtil.isEmpty(this.clusterServer) || Objects.isNull(this.localServer) || Objects.isNull(this.localServer.getEndpoint()) || Objects.isNull(port = this.localServer.getEndpoint().getPort()))
            throw new IllegalArgumentException("init failed caz config");
        RpcServer rpcServer = new RpcServer(port);
        // raft配置
        Option option = new Option();
        option.setDataDir(this.dataPath);
        option.setSnapshotMinLogSize(10 * 1024);
        option.setSnapshotPeriodS(30);
        option.setMaxSegmentFileSize(1024 * 1024);
        // 状态机
        StateMachineImpl stateMachine = new StateMachineImpl(this.dataPath);
        // raft节点
        Node node = new Node(option, this.clusterServer, this.localServer, stateMachine);
        // rpc 客户端调用
        ClientServiceImpl clientService = new ClientServiceImpl(node);
        // rpc 节点互相调用
        ConsensusServiceImpl consensusService = new ConsensusServiceImpl(node);
        // 自定义服务
        CurdServiceImpl curdService = new CurdServiceImpl(node, stateMachine);
        rpcServer.registerService(clientService);
        rpcServer.registerService(consensusService);
        rpcServer.registerService(curdService);
        // 启动rpc服务
        rpcServer.start();
        node.init();
    }

    public void start()
    {
        System.out.println("MINI-RAFT SERVER START...");
    }

    /**
     *
     * @since 2022/4/4
     * @author dingrui
     * @param s: host:port:serverId,host:port:serverId...
     * @return java.util.List<com.github.bannirui.raft.bean.proto.RaftProto.Server>
     */
    public List<RaftProto.Server> parseCluster(String s)
    {
        String[] split = s.split(StrUtil.COMMA);
        if (ArrayUtil.isEmpty(split)) return null;
        List<RaftProto.Server> ret = new ArrayList<>();
        for (String s0 : split)
        {
            RaftProto.Server server = this.parseServer(s0);
            if (Objects.nonNull(server)) ret.add(server);
        }
        return ret;
    }

    /**
     *
     * @since 2022/4/4
     * @author dingrui
     * @param s: host:port:serverId
     * @return com.github.bannirui.raft.bean.proto.RaftProto.Server
     */
    public RaftProto.Server parseServer(String s)
    {
        String[] split = s.split(StrUtil.COLON);
        if (ArrayUtil.isEmpty(split) || split.length != 3) return null;
        String host = split[0];
        Integer port = Convert.toInt(split[1]);
        Integer serverId = Convert.toInt(split[2]);
        if (StrUtil.isBlank(host) || Objects.isNull(port) || Objects.isNull(serverId)) return null;
        RaftProto.Endpoint endpoint = RaftProto.Endpoint.newBuilder().setHost(host).setPort(port).build();
        return RaftProto.Server.newBuilder().setEndpoint(endpoint).setServerId(serverId).build();
    }
}
