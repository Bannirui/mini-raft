package com.github.bannirui.raft.service.impl;

import cn.hutool.core.io.FileUtil;
import com.github.bannirui.raft.core.StateMachine;
import com.github.bannirui.raft.api.bean.proto.CurdProto;
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
public class StateMachineImpl implements StateMachine
{

    private RocksDB db;
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
        String dataDir = this.raftDataDir + File.separator + "rocksdb_data";
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
        CurdProto.GetResponse.Builder responseBuilder = CurdProto.GetResponse.newBuilder();
        byte[] keyBytes = request.getKey().getBytes();
        byte[] valueBytes = null;
        try
        {
            valueBytes = this.db.get(keyBytes);
        }
        catch (Exception ignored)
        {
            return null;
        }
        if (Objects.nonNull(valueBytes))
        {
            String value = new String(valueBytes);
            responseBuilder.setValue(value);
        }
        return responseBuilder.build();
    }
}
