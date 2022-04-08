package com.github.bannirui.raft.core.storeage;

import com.github.bannirui.raft.bean.proto.RaftProto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *
 * @since 2022/4/4
 * @author dingrui
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Segment
{
    @Data
    public static class Record
    {
        private long offset;
        private RaftProto.LogEntry entry;

        public Record(long offset, RaftProto.LogEntry entry)
        {
            this.offset = offset;
            this.entry = entry;
        }
    }

    private boolean canWrite;
    private long startIndex;
    private long endIndex;
    private long fileSize;
    private String fileName;
    private RandomAccessFile randomAccessFile;
    private List<Record> entries = new ArrayList<>();

    public RaftProto.LogEntry getEntry(long index)
    {
        if (this.startIndex == 0 || this.endIndex == 0) return null;
        if (index < this.startIndex || index > this.endIndex) return null;
        int indexInList = (int) (index - this.startIndex);
        Record record = this.entries.get(indexInList);
        if (Objects.isNull(record)) return null;
        return record.entry;
    }
}
