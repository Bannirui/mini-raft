package com.github.bannirui.raft.core.storeage;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ArrayUtil;
import cn.hutool.core.util.StrUtil;
import com.github.bannirui.raft.bean.constant.SegmentLogConstant;
import com.github.bannirui.raft.bean.proto.RaftProto;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
@Data
public class SegmentLog implements SegmentLogService
{

    private String logDir;
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
        this.logDir = raftDataDir + File.separator + SegmentLogConstant.LOG_DIR;
        // .../log/data
        this.logDataDir = logDir + File.separator + SegmentLogConstant.LOG_DATA_DIR;
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

    @Override
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

    @Override
    public long getEntryTerm(long index)
    {
        RaftProto.LogEntry entry = this.getEntry(index);
        if (Objects.isNull(entry)) return 0L;
        return entry.getTerm();
    }

    @Override
    public long getFirstLogIndex()
    {
        if (Objects.isNull(this.metaData)) return -1L;
        return this.metaData.getFirstLogIndex();
    }

    @Override
    public long getLastLogIndex()
    {
        if (MapUtil.isEmpty(this.startLogIndexSegmentMap)) return this.getFirstLogIndex() - 1;
        Map.Entry<Long, Segment> lastSegmentEntry = this.startLogIndexSegmentMap.lastEntry();
        Segment lastSegment = null;
        if (Objects.isNull(lastSegmentEntry) || Objects.isNull(lastSegment = lastSegmentEntry.getValue())) return -1L;
        return lastSegment.getEndIndex();
    }

    @Override
    public long append(List<RaftProto.LogEntry> entries)
    {
        long lastLogIndex = this.getLastLogIndex();
        if (CollUtil.isEmpty(entries)) return lastLogIndex;
        for (RaftProto.LogEntry entry : entries)
        {
            lastLogIndex = this.append(entry);
        }
        return lastLogIndex;
    }

    @Override
    public long append(RaftProto.LogEntry entry)
    {
        long lastLogIndex = this.getLastLogIndex();
        if (Objects.isNull(entry)) return lastLogIndex;
        int entrySize = entry.getSerializedSize();
        boolean needNewSegmentFile = false;
        if (MapUtil.isEmpty(this.startLogIndexSegmentMap)) needNewSegmentFile = true;
        else
        {
            Segment segment = this.startLogIndexSegmentMap.lastEntry().getValue();
            if (!segment.isCanWrite()) needNewSegmentFile = true;
            else if (segment.getFileSize() + entrySize >= this.maxSegmentFileSize)
            {
                needNewSegmentFile = true;
                try
                {
                    segment.getRandomAccessFile().close();
                }
                catch (Exception e)
                {
                    throw new RuntimeException("append log failed");
                }
                segment.setCanWrite(false);
                String newFileName = String.format("%020d-%020d", segment.getStartIndex(), segment.getEndIndex());
                String newFullFileName = this.logDataDir + File.separator + newFileName;
                File newFile = new File(newFullFileName);
                String preFullFileName = this.logDataDir + File.separator + segment.getFileName();
                File preFile = new File(preFullFileName);
                FileUtil.move(preFile, newFile, true);
                segment.setFileName(newFileName);
                RandomAccessFile randomAccessFile = FileUtil.createRandomAccessFile(newFile, FileMode.r);
                segment.setRandomAccessFile(randomAccessFile);
            }
        }
        Segment segment = null;
        if (needNewSegmentFile)
        {
            String newSegmentFileName = String.format("open-%d", lastLogIndex);
            String newFullFileName = this.logDir + File.separator + newSegmentFileName;
            File newSegmentFile = new File(newFullFileName);
            if (!newSegmentFile.exists())
            {
                try
                {
                    newSegmentFile.createNewFile();
                }
                catch (IOException e)
                {
                    throw new RuntimeException("append log failed");
                }
            }
            segment = Segment.builder().canWrite(true).startIndex(++lastLogIndex).endIndex(0).fileName(newSegmentFileName).randomAccessFile(FileUtil.createRandomAccessFile(newSegmentFile, FileMode.rw)).build();
        }
        else
        {
            segment = this.startLogIndexSegmentMap.lastEntry().getValue();
        }
        if (entry.getIndex() == 0) entry = RaftProto.LogEntry.newBuilder().setIndex(lastLogIndex).build();
        segment.setEndIndex(entry.getIndex());
        RandomAccessFile randomAccessFile = segment.getRandomAccessFile();
        long offset;
        try
        {
            offset = randomAccessFile.getFilePointer();
        }
        catch (Exception e)
        {
            throw new RuntimeException("append log failed");
        }
        segment.getEntries().add(new Segment.Record(offset, entry));
        com.github.bannirui.raft.common.util.FileUtil.write(randomAccessFile, entry);
        if (!this.startLogIndexSegmentMap.containsKey(segment.getStartIndex()))
            this.startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
        this.totalSize += entrySize;
        return lastLogIndex;
    }

    @Override
    public void truncatePrefix(long firstIndex)
    {
        long oldFirstIndex = this.getFirstLogIndex();
        if (firstIndex <= oldFirstIndex) return;
        while (MapUtil.isNotEmpty(this.startLogIndexSegmentMap))
        {
            Segment segment = this.startLogIndexSegmentMap.firstEntry().getValue();
            if (segment.isCanWrite()) break;
            if (firstIndex <= segment.getEndIndex()) break;
            File oldFile = new File(this.logDataDir + File.separator + segment.getFileName());
            com.github.bannirui.raft.common.util.FileUtil.close(segment.getRandomAccessFile());
            FileUtil.del(oldFile);
            this.totalSize -= segment.getFileSize();
            this.startLogIndexSegmentMap.remove(segment.getStartIndex());
        }
        long newActualFirstIndex;
        if (MapUtil.isEmpty(this.startLogIndexSegmentMap)) newActualFirstIndex = firstIndex;
        else newActualFirstIndex = this.startLogIndexSegmentMap.firstKey();
        this.updateMetaData(null, null, newActualFirstIndex, null);
    }

    @Override
    public void truncateSuffix(long endIndex)
    {
        if (endIndex >= this.getLastLogIndex()) return;
        while (MapUtil.isNotEmpty(this.startLogIndexSegmentMap))
        {
            Segment segment = this.startLogIndexSegmentMap.lastEntry().getValue();
            if (endIndex == segment.getEndIndex()) break;
            else if (endIndex < segment.getStartIndex())
            {
                this.totalSize -= segment.getFileSize();
                try
                {
                    segment.getRandomAccessFile().close();
                }
                catch (IOException ignored)
                {
                }
                String fullFileName = this.logDir + File.separator + segment.getFileName();
                FileUtil.del(fullFileName);
                this.startLogIndexSegmentMap.remove(segment.getStartIndex());
            }
            else if (endIndex < segment.getEndIndex())
            {
                int i = (int) (endIndex + 1 - segment.getStartIndex());
                segment.setEndIndex(endIndex);
                long newFileSize = segment.getEntries().get(i).getOffset();
                this.totalSize -= (segment.getFileSize() - newFileSize);
                segment.setFileSize(newFileSize);
                segment.getEntries().removeAll(segment.getEntries().subList(i, segment.getEntries().size()));
                FileChannel channel = segment.getRandomAccessFile().getChannel();
                try
                {
                    channel.truncate(segment.getFileSize());
                    channel.close();
                    segment.getRandomAccessFile().close();
                }
                catch (Exception ignored)
                {
                }
                String oldFullFileName = this.logDir + File.separator + segment.getFileName();
                String newFileName = String.format("%020d-%020d", segment.getStartIndex(), segment.getEndIndex());
                segment.setFileName(newFileName);
                String newFullFileName = this.logDir + File.separator + newFileName;
                File newFile = new File(newFullFileName);
                new File(oldFullFileName).renameTo(newFile);
                segment.setRandomAccessFile(FileUtil.createRandomAccessFile(newFile, FileMode.rw));
            }
        }
    }

    @Override
    public void loadSegmentData(Segment segment)
    {
        if (Objects.isNull(segment)) return;
        RandomAccessFile f = segment.getRandomAccessFile();
        long totalLength = segment.getFileSize();
        long offset = 0L;
        while (offset < totalLength)
        {
            RaftProto.LogEntry entry = com.github.bannirui.raft.common.util.FileUtil.read(f, RaftProto.LogEntry.class);
            if (Objects.isNull(entry)) return;
            Segment.Record record = new Segment.Record(offset, entry);
            segment.getEntries().add(record);
            try
            {
                offset = f.getFilePointer();
            }
            catch (IOException ignored)
            {
                throw new RuntimeException("file not found");
            }
        }
        this.totalSize += totalLength;
        int entrySize = segment.getEntries().size();
        if (entrySize > 0)
        {
            segment.setStartIndex(segment.getEntries().get(0).getEntry().getIndex());
            segment.setEndIndex(segment.getEntries().get(entrySize - 1).getEntry().getIndex());
        }
    }

    @Override
    public void readSegment(String fileName)
    {
        if (StrUtil.isBlank(fileName)) return;
        String[] splitArray = fileName.split("-");
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
        String fullFileName = this.logDataDir + File.separator + fileName;
        RandomAccessFile randomAccessFile = FileUtil.createRandomAccessFile(new File(fullFileName), FileMode.rw);
        segment.setRandomAccessFile(randomAccessFile);
        long fileSize = 0L;
        try
        {
            fileSize = randomAccessFile.length();
        }
        catch (Exception ignored)
        {
            return;
        }
        segment.setFileSize(fileSize);
        this.startLogIndexSegmentMap.put(segment.getStartIndex(), segment);
    }

    @Override
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

    @Override
    public RaftProto.LogMetaData readMetaData()
    {
        String fileName = this.logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile f = FileUtil.createRandomAccessFile(file, FileMode.r))
        {
            return com.github.bannirui.raft.common.util.FileUtil.read(f, RaftProto.LogMetaData.class);
        }
        catch (Exception ignored)
        {
            return null;
        }
    }

    @Override
    public void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex, Long commitIndex)
    {
        RaftProto.LogMetaData.Builder builder = RaftProto.LogMetaData.newBuilder(this.metaData);
        if (Objects.nonNull(currentTerm)) builder.setCurrentTerm(currentTerm);
        if (Objects.nonNull(votedFor)) builder.setVotedFor(votedFor);
        if (Objects.nonNull(firstLogIndex)) builder.setFirstLogIndex(firstLogIndex);
        if (Objects.nonNull(commitIndex)) builder.setCommitIndex(commitIndex);
        this.metaData = builder.build();
        String fileName = this.logDir + File.separator + "metadata";
        File file = new File(fileName);
        try (RandomAccessFile f = FileUtil.createRandomAccessFile(file, FileMode.rw))
        {
            com.github.bannirui.raft.common.util.FileUtil.write(f, metaData);
        }
        catch (Exception ignored)
        {

        }
    }
}
