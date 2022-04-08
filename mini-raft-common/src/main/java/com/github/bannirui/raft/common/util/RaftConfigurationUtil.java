package com.github.bannirui.raft.common.util;

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

    public boolean containsServer(RaftProto.Configuration configuration, int serverId)
    {
        return configuration.getServersList().stream().filter(Objects::nonNull).anyMatch(e -> Objects.equals(e.getServerId(), serverId));
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
