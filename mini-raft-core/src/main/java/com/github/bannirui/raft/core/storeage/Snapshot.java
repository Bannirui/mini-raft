package com.github.bannirui.raft.core.storeage;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.io.file.FileMode;
import cn.hutool.core.map.MapUtil;
import com.github.bannirui.raft.bean.constant.FilePathConstant;
import com.github.bannirui.raft.bean.proto.RaftProto;
import com.github.bannirui.raft.common.util.FileUtil;
import lombok.Data;
import org.apache.commons.io.FileUtils;

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
     * ./data/snapshot
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
        // ./data/snapshot
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

    /**
     * <p>打开./data/snapshot/data目录下的文件 如果是软连接 就打开实际文件句柄</p>
     * @since 2022/4/9
     * @author dingrui
     * @return java.util.TreeMap<java.lang.String, com.github.bannirui.raft.core.storeage.Snapshot.SnapshotDataFile> key=文件名 value=文件句柄
     */
    public TreeMap<String, SnapshotDataFile> openSnapshotDataFiles()
    {
        TreeMap<String, SnapshotDataFile> ret = new TreeMap<>();
        // ./data/snapshot/data
        String snapshotDataDir = this.snapshotDir + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_DATA;
        try
        {
            Path snapshotDataPath = FileSystems.getDefault().getPath(snapshotDataDir);
            snapshotDataPath = snapshotDataPath.toRealPath();
            snapshotDataDir = snapshotDataPath.toString();
            List<String> fileNames = FileUtil.getSortedFilesInDirectory(snapshotDataDir, snapshotDataDir);
            if (CollUtil.isEmpty(fileNames)) return ret;
            for (String fileName : fileNames)
            {
                RandomAccessFile f = FileUtil.open(snapshotDataDir, fileName, FileMode.r);
                SnapshotDataFile s = new SnapshotDataFile();
                s.setFileName(fileName);
                s.setRandomAccessFile(f);
                ret.put(fileName, s);
            }
        }
        catch (Exception ignored)
        {
        }
        return ret;
    }

    public void closeSnapshotDataFiles(Map<String, SnapshotDataFile> data)
    {
        if (MapUtil.isEmpty(data)) return;
        data.forEach((fileName, file) -> {
            try
            {
                file.getRandomAccessFile().close();
            }
            catch (IOException ignored)
            {
            }
        });
    }

    public RaftProto.SnapshotMetaData readMetaData()
    {
        // ./data/snapshot/metadata
        String fileName = this.snapshotDir + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_METADATA;
        File file = new File(fileName);
        RaftProto.SnapshotMetaData ret = null;
        try (RandomAccessFile f = new RandomAccessFile(file, "r"))
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
        // ./data/snapshot/metadata
        String snapshotMetaFile = dir + File.separator + FilePathConstant.Snapthot.Dir.SNAPSHOT_METADATA;
        RandomAccessFile randomAccessFile = null;
        try
        {
            File dirFile = new File(dir);
            if (!dirFile.exists()) dirFile.mkdirs();
            File file = new File(snapshotMetaFile);
            if (!file.exists()) FileUtils.forceDelete(file);
            file.createNewFile();
            randomAccessFile = new RandomAccessFile(file, "rw");
            FileUtil.write(randomAccessFile, metaData);
        }
        catch (Exception ignored)
        {
        }
        finally
        {
            FileUtil.close(randomAccessFile);
        }
    }
}