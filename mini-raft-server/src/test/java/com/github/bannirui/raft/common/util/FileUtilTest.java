package com.github.bannirui.raft.common.util;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.file.FileMode;
import com.github.bannirui.raft.bean.proto.RaftProto;
import org.junit.Test;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 *
 * @since 2022/4/7
 * @author dingrui
 */
public class FileUtilTest
{

    @Test
    public void testWrite() throws FileNotFoundException
    {
        String src = "classpath:data/test.txt";
        File file = ResourceUtils.getFile(src);
        RandomAccessFile f = com.github.bannirui.raft.common.util.FileUtil.open(file, FileMode.rw);
        RaftProto.LogMetaData e = RaftProto.LogMetaData.newBuilder().setCommitIndex(1L).setFirstLogIndex(2L).setCurrentTerm(3L).setVotedFor(4).build();
        com.github.bannirui.raft.common.util.FileUtil.write(f, e);
    }

    @Test
    public void testRead() throws FileNotFoundException
    {
        String src = "classpath:data/test.txt";
        File file = ResourceUtils.getFile(src);
        RandomAccessFile f = com.github.bannirui.raft.common.util.FileUtil.open(file, FileMode.r);
        RaftProto.LogMetaData ret = com.github.bannirui.raft.common.util.FileUtil.read(f, RaftProto.LogMetaData.class);
        System.out.println();
    }

    @Test
    public void testCreateFile() throws IOException
    {
        String src = "classpath:data/test.txt";
        File file = ResourceUtils.getFile(src);
        RandomAccessFile f = FileUtil.createRandomAccessFile(file, FileMode.rw);
        String s = FileUtil.readLine(f, StandardCharsets.UTF_8);
        System.out.println();
    }
}
