package com.github.bannirui.raft.common.util;

import cn.hutool.core.collection.CollUtil;
import com.github.bannirui.raft.bean.proto.RaftProto;
import lombok.experimental.UtilityClass;

import java.util.List;
import java.util.Objects;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
@UtilityClass
public class RaftConfigurationUtil
{

    /**
     * <p>某个服务器是否存在于集群中</p>
     * @since 2022/4/8
     * @author dingrui
     * @param configuration 集群信息 集群中所有节点
     * @param serverId 单机节点id
     * @return boolean 单机属于集群一员返回<tt>true</tt> 否则返回<tt>false</tt>
     */
    public boolean containsServer(RaftProto.Configuration configuration, int serverId)
    {
        List<RaftProto.Server> servers = null;
        if (Objects.isNull(configuration) || CollUtil.isEmpty(servers = configuration.getServersList())) return false;
        return servers.stream().filter(Objects::nonNull).anyMatch(e -> Objects.equals(e.getServerId(), serverId));
    }

    public RaftProto.Configuration removeServers(RaftProto.Configuration configuration, List<RaftProto.Server> servers)
    {
        RaftProto.Configuration.Builder configurationBuilder = RaftProto.Configuration.newBuilder();
        for (RaftProto.Server server : configuration.getServersList())
        {
            boolean toBeRemoved = false;
            for (RaftProto.Server server1 : servers)
            {
                if (Objects.equals(server.getServerId(), server1.getServerId()))
                {
                    toBeRemoved = true;
                    break;
                }
            }
            if (!toBeRemoved) configurationBuilder.addServers(server);
        }
        return configurationBuilder.build();
    }
}
