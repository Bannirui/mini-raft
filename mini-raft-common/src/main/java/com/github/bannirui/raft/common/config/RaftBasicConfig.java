package com.github.bannirui.raft.common.config;

import com.github.bannirui.raft.common.bean.RaftNodeOption;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 *
 * @since 2022/4/9
 * @author dingrui
 */
@Data
@Component
@PropertySource("classpath:raft.properties")
@ConfigurationProperties(prefix = "raft")
public class RaftBasicConfig
{

    /**
     * raft节点配置
     */
    private RaftNodeOption raftNodeOption;

    /**
     * ./data
     */
    @Value("${raft.raftNodeOption.dataDir}")
    private String raftDataDir;
}
