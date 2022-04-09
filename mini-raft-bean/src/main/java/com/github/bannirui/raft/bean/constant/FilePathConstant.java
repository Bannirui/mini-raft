package com.github.bannirui.raft.bean.constant;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
public interface FilePathConstant
{
    interface Log
    {
        interface Dir
        {
            String LOG = "log";
            String LOG_DATA = "data";
            String LOG_METADATA = "metadata";
        }
    }

    interface Snapthot
    {
        interface Dir
        {
            String SNAPSHOT = "snapshot";
            String SNAPSHOT_DATA = "data";
            String SNAPSHOT_METADATA = "metadata";
            String SNAPSHOT_TMP = ".tmp";
            String SNAPSHOT_TMP_DATA = "data";
        }
    }

    interface Rocksdb
    {
        interface Dir
        {
            String ROCKSDB = "rocksdb";
        }
    }
}
