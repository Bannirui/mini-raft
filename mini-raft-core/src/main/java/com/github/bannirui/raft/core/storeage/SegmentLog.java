package com.github.bannirui.raft.core.storeage;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.file.FileMode;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.github.bannirui.raft.bean.constant.FilePathConstant;
import com.github.bannirui.raft.bean.proto.RaftProto;
import lombok.Data;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
@Data
public class SegmentLog
{

    // .../log
    private String logDir;

    // .../log/data
    private String logDataDir;

    private int maxSegmentFileSize;
    private RaftProto.LogMetaData metaData;
    private TreeMap<Long, Segment> startLogIndexSegmentMap = new TreeMap<>();

    /**
     * segment log占用的总内存大小
     * 用于判断是否需要做snapshot
     */
    private volatile long totalSize;

    public SegmentLog(String raftDataDir, int maxSegmentFileSize)
    {
        // .../log
        this.logDir = raftDataDir + File.separator + FilePathConstant.Log.Dir.LOG;
        // .../log/data
        this.logDataDir = this.logDir + File.separator + FilePathConstant.Log.Dir.LOG_DATA;
        this.maxSegmentFileSize = maxSegmentFileSize;
        File file = new File(this.logDataDir);
        if (!file.exists()) file.mkdirs();
        this.readSegments();
        this.startLogIndexSegmentMap.values().forEach(this::loadSegmentData);
        this.metaData = this.readMetaData();
        if (Objects.isNull(this.metaData))
        {
            if (MapUtil.isNotEmpty(this.startLogIndexSegmentMap))
                throw new RuntimeException("no readable metadata file but found segments");
            this.metaData = RaftProto.LogMetaData.newBuilder().setFirstLogIndex(1).build();
        }
    }

    public RaftProto.LogEntry getEntry(long index)
    {
        if (index == 0) return null;
        long firstLogIndex = this.getFirstLogIndex();
        long lastLogIndex = this.getLastLogIndex();
        if (index < firstLogIndex || index > lastLogIndex) return null;
        if (MapUtil.isEmpty(this.startLogIndexSegmentMap)) return null;
        Map.Entry<Long, Segment> segmentEntry = this.startLogIndexSegmentMap.floorEntry(index);
        Segment segment = null;
        if (Objects.isNull(segmentEntry) || Objects.isNull(segment = segmentEntry.getValue())) return null;
        return segment.getEntry(index);
    }

    public long getEntryTerm(long index)
    {
        RaftProto.LogEntry entry = this.getEntry(index);
        if (Objects.isNull(entry)) return 0L;
        return entry.getTerm();
    }

    public long getFirstLogIndex()
    {
        if (Objects.isNull(this.metaData)) return -1L;
        return this.metaData.getFirstLogIndex();
    }

    /**
     * <p>有两种情况下segment为空<ul>
     *     <li>第一次初始化的时候 firstLogIndex=1 lastLogIndex=0</li>
     *     <li>snapshot刚完成 日志正好被清理掉了 firstLogIndex=snapshotIndex+1 lastLogIndex=snapshotIndex</li>
     * </ul></p>
     * @since 2022/4/9
     * @author dingrui
     * @return long
     */
    public long getLastLogIndex()
    {
        if (MapUtil.isEmpty(this.startLogIndexSegmentMap)) return this.getFirstLogIndex() - 1;
        Map.Entry<Long, Segment> lastSegmentEntry = this.startLogIndexSegmentMap.lastEntry();
        Segment lastSegment = null;
        if (Objects.isNull(lastSegmentEntry) || Objects.isNull(lastSegment = lastSegmentEntry.getValue())) return -1L;
        return lastSegment.getEndIndex();
    }

    public long append(List<RaftProto.LogEntry> entries)
    {
        long newLastLogIndex = this.getLastLogIndex();
        if (CollUtil.isEmpty(entries)) return newLastLogIndex;
        for (RaftProto.LogEntry entry : entries)
        {
            if (Objects.isNull(entry)) continue;
            newLastLogIndex++;
            int entrySize = entry.getSerializedSize();
            int segmentSize = this.startLogIndexSegmentMap.size();
            boolean isNeedNewSegmentFile = false;
            try
            {
                if (segmentSize == 0) isNeedNewSegmentFile = true;
                else
                {
                    Segment segment = this.startLogIndexSegmentMap.lastEntry().getValue();
                    if (!segment.isCanWrite()) isNeedNewSegmentFile = true;
                    else if (segment.getFileSize() + entrySize >= this.maxSegmentFileSize)
                    {
                        isNeedNewSegmentFile = true;
                        // 最后一个segment的文件close并改名
                        segment.getRandomAccessFile().close();
                        segment.setCanWrite(false);
                        String newFileName = String.format("%020d-%020d", segment.getStartIndex(), segment.getEndIndex());
                        String newFullFileName = this.logDataDir + File.separator + newFileName;
                        File newFile = new File(newFullFileName);
                        String oldFullFileName = this.logDataDir + File.separator + segment.getFileName();
                        File oldFile = new File(oldFullFileName);
                        FileUtils.moveFile(oldFile, newFile);
                        segment.setFileName(newFileName);
                        segment.setRandomAccessFile(com.github.bannirui.raft.common.util.FileUtil.open(this.logDataDir, newFileName, FileMode.r));
                    }
                }
                Segment newSegment;
                // 新建segment文件
                if (isNeedNewSegmentFile)
                {
                    // open new segment file
                    String newSegmentFileName = String.format("open-%d", newLastLogIndex);
                    String newFullFileName = this.logDataDir + File.separator + newSegmentFileName;
                    File newSegmentFile = new File(newFullFileName);
                    if (!newSegmentFile.exists()) newSegmentFile.createNewFile();
                    Segment segment = new Segment();
                    segment.setCanWrite(true);
                    segment.setStartIndex(newLastLogIndex);
                    segment.setEndIndex(0);
                    segment.setFileName(newSegmentFileName);
                    segment.setRandomAccessFile(com.github.bannirui.raft.common.util.FileUtil.open(this.logDataDir, newSegmentFileName, FileMode.rw));
                    newSegment = segment;
                }
                else newSegment = this.startLogIndexSegmentMap.lastEntry().getValue();
                // 写proto到segment中
                if (entry.getIndex() == 0)
                    entry = RaftProto.LogEntry.newBuilder(entry).setIndex(newLastLogIndex).build();
                newSegment.setEndIndex(entry.getIndex());
                newSegment.getEntries().add(new Segment.Record(newSegment.getRandomAccessFile().getFilePointer(), entry));
                com.github.bannirui.raft.common.util.FileUtil.write(newSegment.getRandomAccessFile(), entry);
                newSegment.setFileSize(newSegment.getRandomAccessFile().length());
                if (!this.startLogIndexSegmentMap.containsKey(newSegment.getStartIndex()))
                    this.startLogIndexSegmentMap.put(newSegment.getStartIndex(), newSegment);
                this.totalSize += entrySize;
            }
            catch (IOException e)
            {
                throw new RuntimeException("append raft log exception, msg=" + e.getMessage());
            }
        }
        return newLastLogIndex;
    }

    public long append(RaftProto.LogEntry entry)
    {
        List<RaftProto.LogEntry> l = new ArrayList<RaftProto.LogEntry>()
        {{
            add(entry);
        }};
        return this.append(l);
    }

    public void truncatePrefix(long newFirstIndex)
    {
        long oldFirstIndex = this.getFirstLogIndex();
        if (newFirstIndex <= oldFirstIndex) return;
        while (MapUtil.isNotEmpty(this.startLogIndexSegmentMap))
        {
            Segment segment = this.startLogIndexSegmentMap.firstEntry().getValue();
            if (segment.isCanWrite()) break;
            if (newFirstIndex <= segment.getEndIndex()) break;
            File oldFile = new File(this.logDataDir + File.separator + segment.getFileName());
            try
            {
                com.github.bannirui.raft.common.util.FileUtil.close(segment.getRandomAccessFile());
                FileUtils.forceDelete(oldFile);
                this.totalSize -= segment.getFileSize();
                this.startLogIndexSegmentMap.remove(segment.getStartIndex());
            }
            catch (Exception ignored)
            {

            }
        }
        long newActualFirstIndex;
        if (MapUtil.isEmpty(this.startLogIndexSegmentMap)) newActualFirstIndex = newFirstIndex;
        else newActualFirstIndex = this.startLogIndexSegmentMap.firstKey();
        this.updateMetaData(null, null, newActualFirstIndex, null);
    }

    public void truncateSuffix(long newEndIndex)
    {
        if (newEndIndex >= this.getLastLogIndex()) return;
        while (MapUtil.isNotEmpty(this.startLogIndexSegmentMap))
        {
            try
            {
                Segment segment = this.startLogIndexSegmentMap.lastEntry().getValue();
                if (newEndIndex == segment.getEndIndex()) break;
                else if (newEndIndex < segment.getStartIndex())
                {
                    this.totalSize -= segment.getFileSize();
                    segment.getRandomAccessFile().close();
                    String fullFileName = this.logDataDir + File.separator + segment.getFileName();
                    FileUtils.forceDelete(new File(fullFileName));
                    this.startLogIndexSegmentMap.remove(segment.getStartIndex());
                }
                else if (newEndIndex < segment.getEndIndex())
                {
                    int i = (int) (newEndIndex + 1 - segment.getStartIndex());
                    segment.setEndIndex(newEndIndex);
                    long newFileSize = segment.getEntries().get(i).getOffset();
                    this.totalSize -= (segment.getFileSize() - newFileSize);
                    segment.setFileSize(newFileSize);
                    segment.getEntries().removeAll(segment.getEntries().subList(i, segment.getEntries().size()));
                    FileChannel channel = segment.getRandomAccessFile().getChannel();
                    channel.truncate(segment.getFileSize());
                    channel.close();
                    segment.getRandomAccessFile().close();
                    String oldFullFileName = this.logDataDir + File.separator + segment.getFileName();
                    String newFileName = String.format("%020d-%020d", segment.getStartIndex(), segment.getEndIndex());
                    segment.setFileName(newFileName);
                    String newFullFileName = this.logDataDir + File.separator + newFileName;
                    File newFile = new File(newFullFileName);
                    new File(oldFullFileName).renameTo(new File(newFullFileName));
                    segment.setRandomAccessFile(com.github.bannirui.raft.common.util.FileUtil.open(this.logDataDir, segment.getFileName(), FileMode.rw));
                }
            }
            catch (Exception ignored)
            {
            }
        }
    }

    public void loadSegmentData(Segment segment)
    {
        if (Objects.isNull(segment)) return;
        try
        {
            RandomAccessFile f = segment.getRandomAccessFile();
            long totalLength = segment.getFileSize();
            long offset = 0L;
            while (offset < totalLength)
            {
                RaftProto.LogEntry entry = com.github.bannirui.raft.common.util.FileUtil.read(f, RaftProto.LogEntry.class);
                if (Objects.isNull(entry)) throw new RuntimeException("read segment log failed");
                Segment.Record record = new Segment.Record(offset, entry);
                segment.getEntries().add(record);
                offset = f.getFilePointer();
            }
            this.totalSize += totalLength;
        }
        catch (Exception e)
        {
            throw new RuntimeException("file not found");
        }
        int entrySize = segment.getEntries().size();
        if (entrySize > 0)
        {
            segment.setStartIndex(segment.getEntries().get(0).getEntry().getIndex());
            segment.setEndIndex(segment.getEntries().get(entrySize - 1).getEntry().getIndex());
        }
    }

    /**
     * @since 2022/4/9
     * @author dingrui
     * @param fileName open-1 open-2 ... 或 1-2 3-7 ...
     * @return void
     */
    public void readSegment(String fileName)
    {
        try
        {
            if (StrUtil.isBlank(fileName)) return;
            String[] splitArray = fileName.split(StrUtil.DASHED);
            if (ArrayUtil.isEmpty(splitArray) || splitArray.length != 2) return;
            Segment segment = new Segment();
            segment.setFileName(fileName);
            if (Objects.equals(splitArray[0], "open"))
            {
                segment.setCanWrite(true);
                segment.setStartIndex(Convert.toLong(splitArray[1]));
                segment.setEndIndex(0);
            }
            else
            {
                segment.setCanWrite(false);
                segment.setStartIndex(Convert.toLong(splitArray[0]));
                segment.setEndIndex(Convert.toLong(splitArray[1]));
            }
            RandomAccessFile randomAccessFile = com.github.bannirui.raft.common.util.FileUtil.open(this.logDataDir, fileName, FileMode.rw);
            segment.setRandomAccessFile(randomAccessFile);
            segment.setFileSize(randomAccessFile.length());
            this.startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
        }
        catch (Exception ignored)
        {
        }
    }

    public void readSegments()
    {
        List<String> fileNames = null;
        try
        {
            fileNames = com.github.bannirui.raft.common.util.FileUtil.getSortedFilesInDirectory(this.logDataDir, this.logDataDir);
        }
        catch (Exception ignored)
        {
        }
        if (CollUtil.isEmpty(fileNames)) return;
        for (String fileName : fileNames)
            readSegment(fileName);
    }

    public RaftProto.LogMetaData readMetaData()
    {
        // .../log/metadata
        String fileName = this.logDir + File.separator + FilePathConstant.Log.Dir.LOG_METADATA;
        File file = new File(fileName);
        try (RandomAccessFile f = new RandomAccessFile(file, "r"))
        {
            return com.github.bannirui.raft.common.util.FileUtil.read(f, RaftProto.LogMetaData.class);
        }
        catch (Exception ignored)
        {
            return null;
        }
    }

    public void updateMetaData(Long curTerm, Integer votedFor, Long firstLogIndex, Long commitIndex)
    {
        RaftProto.LogMetaData.Builder builder = RaftProto.LogMetaData.newBuilder(this.metaData);
        if (Objects.nonNull(curTerm)) builder.setCurrentTerm(curTerm);
        if (Objects.nonNull(votedFor)) builder.setVotedFor(votedFor);
        if (Objects.nonNull(firstLogIndex)) builder.setFirstLogIndex(firstLogIndex);
        if (Objects.nonNull(commitIndex)) builder.setCommitIndex(commitIndex);
        this.metaData = builder.build();
        // .../log/metadata
        String fileName = this.logDir + File.separator + FilePathConstant.Log.Dir.LOG_METADATA;
        File file = new File(fileName);
        try (RandomAccessFile f = new RandomAccessFile(file, "rw"))
        {
            com.github.bannirui.raft.common.util.FileUtil.write(f, this.metaData);
        }
        catch (Exception ignored)
        {
        }
    }
}
