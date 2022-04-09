package com.github.bannirui.raft.common.config;

import com.github.bannirui.raft.common.bean.RaftNodeOption;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 * @since 2022/4/9
 * @author dingrui
 */
@Configuration
@RequiredArgsConstructor
public class RaftBeanConfig
{

    private final RaftBasicConfig raftBasicConfig;

    /**
     * <p>raft集群节点配置</p>
     * @since 2022/4/9
     * @author dingrui
     * @return com.github.bannirui.raft.common.bean.RaftNodeOption
     */
    @Bean
    public RaftNodeOption raftNodeOption()
    {
        return this.raftBasicConfig.getRaftNodeOption();
    }

    /**
     * <p>raft日志根目录</p>
     * @since 2022/4/9
     * @author dingrui
     * @return java.lang.String
     */
    @Bean
    public String raftDataDir()
    {
        return this.raftBasicConfig.getRaftDataDir();
    }
}
