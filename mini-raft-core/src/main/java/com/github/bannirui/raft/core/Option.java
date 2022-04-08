package com.github.bannirui.raft.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <p>raft配置选项</p>
 * @since 2022/4/4
 * @author dingrui
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Option
{
    /**
     * follower转换candidate时间
     * 单位:ms
     */
    private int electionTimeoutMS = 5_000;

    /**
     * leader发送心跳
     * 单位:ms
     */
    private int heartbeatPeriodMS = 5_00;

    /**
     * snapshot定时器执行间隔
     * 单位:s
     */
    private int snapshotPeriodS = 3600;

    /**
     * log entry进行snapshot的阈值大小
     */
    private int snapshotMinLogSize = 100 * 1024 * 1024;

    private int maxSnapshotBytesPerRequest = 500 * 1024;
    private int maxLogEntriesPerRequest = 5_000;

    /**
     * 单个segment文件大小
     * 100M
     */
    private int maxSegmentFileSize = 100 * 1_024 * 1_024;

    /**
     * follower与leader差距阈值
     * 在阈值之内才可以参与选举和提供服务
     */
    private long catchupMargin = 500;

    /**
     * replicate最大的等待时间
     * 单位:ms
     */
    private long maxAwaitTimeoutMS = 1_000;

    /**
     * 线程池大小
     * 与其他节点进行同步 选主操作的线程池
     */
    private int raftConsensusThreadNum = 20;

    /**
     * 是否异步写数据
     * <tt>true</tt>表示主节点保存后就返回 然后主节点异步同步给从节点
     * <tt>false</tt>表示主节点同步给大多数从节点后才返回客户端响应
     */
    private boolean asyncWrite = false;

    /**
     * raft的log和snapshot父目录
     * 绝对路径
     */
    private String dataDir = System.getProperty("com.github.bannirui.raft.data.dir");
}
