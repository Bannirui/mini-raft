package com.github.bannirui.raft.bean.constant;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
public interface FilePathConstant
{
    interface Snapthot
    {
        interface Dir
        {
            String SNAPSHOT = "snapshot";
            String SNAPSHOT_DATA = "data";
            String SNAPSHOT_METADATA = "metadata";
            String SNAPSHOT_TMP = ".tmp";
        }
    }
}
