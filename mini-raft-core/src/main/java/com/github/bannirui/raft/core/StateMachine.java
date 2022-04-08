package com.github.bannirui.raft.core;

/**
 * <p>raft状态机</p>
 * @since 2022/4/4
 * @author dingrui
 */
public interface StateMachine
{
    /**
     * <p>对状态机中数据进行snapshot 每个节点本地定时调用</p>
     * @since 2022/4/4
     * @author dingrui
     * @param snapshotDir snapshot数据输出目录
     * @return void
     */
    void writeSnapshot(String snapshotDir);

    /**
     * <p>读取snapshot到状态机 节点启动时进行调用</p>
     * @since 2022/4/4
     * @author dingrui
     * @param snapshotDir snapshot数据目录
     * @return void
     */
    void readSnapshot(String snapshotDir);

    /**
     * <p>将数据应用到状态机</p>
     * @since 2022/4/4
     * @author dingrui
     * @param data 数据
     * @return void
     */
    void apply(byte[] data);
}
