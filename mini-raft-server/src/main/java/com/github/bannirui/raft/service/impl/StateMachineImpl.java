package com.github.bannirui.raft.service.impl;

import cn.hutool.core.io.FileUtil;
import com.github.bannirui.raft.api.bean.proto.CurdProto;
import com.github.bannirui.raft.bean.constant.FilePathConstant;
import com.github.bannirui.raft.core.StateMachine;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Objects;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
@Slf4j
public class StateMachineImpl implements StateMachine
{

    private RocksDB db;

    // ./data
    private String raftDataDir;

    static
    {
        RocksDB.loadLibrary();
    }

    public StateMachineImpl(String raftDataDir)
    {
        this.raftDataDir = raftDataDir;
    }

    @Override
    public void writeSnapshot(String snapshotDir)
    {
        Checkpoint checkpoint = Checkpoint.create(this.db);
        try
        {
            checkpoint.createCheckpoint(snapshotDir);
        }
        catch (RocksDBException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void readSnapshot(String snapshotDir)
    {
        if (Objects.nonNull(this.db)) this.db.close();
        String dataDir = this.raftDataDir + File.separator + FilePathConstant.Rocksdb.Dir.ROCKSDB;
        File dataFile = new File(dataDir);
        if (dataFile.exists()) FileUtil.del(dataFile);
        File snapshotFile = new File(snapshotDir);
        if (snapshotFile.exists()) FileUtil.copy(snapshotFile, dataFile, false);
        Options options = new Options();
        options.setCreateIfMissing(true);
        try
        {
            this.db = RocksDB.open(options, dataDir);
        }
        catch (RocksDBException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void apply(byte[] data)
    {
        try
        {
            CurdProto.SetRequest request = CurdProto.SetRequest.parseFrom(data);
            this.db.put(request.getKey().getBytes(), request.getValue().getBytes());
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public CurdProto.GetResponse get(CurdProto.GetRequest request)
    {
        try
        {
            CurdProto.GetResponse.Builder responseBuilder = CurdProto.GetResponse.newBuilder();
            String key = request.getKey();
            if (log.isErrorEnabled()) log.error("状态机 收到请求 key={}", key);
            byte[] keyBytes = key.getBytes();
            byte[] valueBytes = this.db.get(keyBytes);
            if (Objects.nonNull(valueBytes))
            {
                String value = new String(valueBytes);
                responseBuilder.setValue(value);
            }
            if (log.isInfoEnabled()) log.info("状态机从rocksdb中获取到的数据 key={} value={}", key, responseBuilder.getValue());
            return responseBuilder.build();
        }
        catch (Exception e)
        {
            log.error("状态机获取数据异常 err={}", e.getMessage());
            return null;
        }
    }
}
