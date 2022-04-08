package com.github.bannirui.raft.core.storeage;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.file.FileMode;
import cn.hutool.core.map.MapUtil;
import com.github.bannirui.raft.bean.constant.FilePathConstant;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.common.util.FileUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Snapshot
{

    @Data
    public static class SnapshotDataFile
    {
        private String fileName;
        private RandomAccessFile randomAccessFile;
    }

    /**
     * 目录
     * classpath:data/snapshot
     */
    private String snapshotDir;
    private RaftProto.SnapshotMetaData metaData;

    /**
     * 正在安装snapshot
     * leader向follower安装 leader和follower同时处于installSnapshot状态
     */
    private AtomicBoolean isInstallSnapshot = new AtomicBoolean(false);

    /**
     * 节点自己是否在对状态机做snapshot
     */
    private AtomicBoolean isTakeSnapshot = new AtomicBoolean(false);

    private Lock lock = new ReentrantLock();

    public Snapshot(String dataDir)
    {
        // classpath:data/snapshot
        this.snapshotDir = dataDir + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT;
        String snapshotDataDir = this.snapshotDir + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_DATA;
        File file = new File(snapshotDataDir);
        if (!file.exists()) file.mkdirs();
    }

    public void reload()
    {
        if (Objects.isNull(this.metaData = this.readMetaData()))
            this.metaData = RaftProto.SnapshotMetaData.newBuilder().build();
    }

    public TreeMap<String, SnapshotDataFile> openSnapshotDataFiles()
    {
        // classpath:data/snapshot/data
        String snapshotDataDir = this.snapshotDir + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_DATA;
        Path snapshotDataPath = FileSystems.getDefault().getPath(snapshotDataDir);
        Path snapshotDataRealPath = null;
        try
        {
            snapshotDataRealPath = snapshotDataPath.toRealPath();
        }
        catch (Exception ignored)
        {
        }
        if (Objects.isNull(snapshotDataRealPath)) return null;
        String dir = snapshotDataRealPath.toString();
        List<String> fileNames = null;
        try
        {
            fileNames = FileUtil.getSortedFilesInDirectory(dir, dir);
        }
        catch (Exception ignored)
        {
        }
        if (CollUtil.isEmpty(fileNames)) return null;
        TreeMap<String, SnapshotDataFile> ret = new TreeMap<>();
        for (String fileName : fileNames)
        {
            RandomAccessFile f = cn.hutool.core.io.FileUtil.createRandomAccessFile(new File(dir + File.separator + fileName), FileMode.r);
            SnapshotDataFile s = new SnapshotDataFile();
            s.setFileName(fileName);
            s.setRandomAccessFile(f);
            ret.put(fileName, s);
        }
        return ret;
    }

    public void closeSnapshotDataFiles(Map<String, SnapshotDataFile> data)
    {
        if (MapUtil.isEmpty(data)) return;
        data.forEach((key, value) -> {
            try
            {
                value.getRandomAccessFile().close();
            }
            catch (IOException ignored)
            {
            }
        });
    }

    public RaftProto.SnapshotMetaData readMetaData()
    {
        // classpath:data/snapshot/metadata
        String fileName = this.snapshotDir + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_METADATA;
        File file = new File(fileName);
        RaftProto.SnapshotMetaData ret = null;
        try (RandomAccessFile f = cn.hutool.core.io.FileUtil.createRandomAccessFile(file, FileMode.r))
        {
            ret = FileUtil.read(f, RaftProto.SnapshotMetaData.class);
        }
        catch (Exception ignored)
        {
        }
        return ret;
    }

    public void updateMetaData(String dir, Long lastIncludedIndex, Long lastIncludedTerm, RaftProto.Configuration configuration)
    {
        RaftProto.SnapshotMetaData metaData = RaftProto.SnapshotMetaData.newBuilder().setLastIncludedIndex(lastIncludedIndex).setLastIncludedTerm(lastIncludedTerm).setConfiguration(configuration).build();
        String fileName = dir + File.separator + "metadata";
        File dirFile = new File(dir);
        if (!dirFile.exists()) dirFile.mkdirs();
        File file = new File(fileName);
        if (file.exists()) cn.hutool.core.io.FileUtil.del(file);
        try
        {
            file.createNewFile();
        }
        catch (Exception ignored)
        {
        }
        if (!file.exists()) return;
        try (RandomAccessFile f = cn.hutool.core.io.FileUtil.createRandomAccessFile(file, FileMode.rw))
        {
            FileUtil.write(f, metaData);
        }
        catch (Exception ignored)
        {
        }
    }
}