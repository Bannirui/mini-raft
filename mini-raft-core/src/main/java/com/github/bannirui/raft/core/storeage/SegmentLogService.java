package com.github.bannirui.raft.core.storeage;

import com.github.bannirui.raft.bean.proto.RaftProto;

import java.util.List;

/**
 *
 * @since 2022/4/5
 * @author dingrui
 */
public interface SegmentLogService
{
    RaftProto.LogEntry getEntry(long index);

    long getEntryTerm(long index);

    long getFirstLogIndex();

    long getLastLogIndex();

    long append(List<RaftProto.LogEntry> entries);

    long append(RaftProto.LogEntry entry);

    void truncatePrefix(long firstIndex);

    void truncateSuffix(long endIndex);

    void loadSegmentData(Segment segment);

    void readSegment(String fileName);

    void readSegments();

    RaftProto.LogMetaData readMetaData();

    void updateMetaData(Long currentTerm, Integer votedFor, Long firstLogIndex, Long commitIndex);
}
