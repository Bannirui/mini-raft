package com.github.bannirui.raft.bean;

/**
 * <p>raft集群节点的状态</p>
 * @since 2022/4/9
 * @author dingrui
 */
public enum RaftNodeState
{
    FOLLOWER,
    /**
     * 投票选举二阶段
     */
    PRE_CANDIDATE, CANDIDATE, LEADER;
}
