package com.github.bannirui.raft.common.util;

import cn.hutool.core.io.file.FileMode;
import cn.hutool.core.util.ArrayUtil;
import com.google.protobuf.Message;
import lombok.experimental.UtilityClass;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 *
 * @since 2022/4/5
 * @author dingrui
 */
@UtilityClass
public class FileUtil
{

    public RandomAccessFile open(String dir, String fileName, FileMode mode)
    {
        return cn.hutool.core.io.FileUtil.createRandomAccessFile(new File(dir + File.separator + fileName), mode);
    }

    public RandomAccessFile open(File file, FileMode mode)
    {
        return cn.hutool.core.io.FileUtil.createRandomAccessFile(file, mode);
    }

    public void close(RandomAccessFile f)
    {
        if (Objects.isNull(f)) return;
        try
        {
            f.close();
        }
        catch (Exception ignored)
        {
        }
    }

    public <T extends Message> T read(RandomAccessFile f, Class<T> clazz)
    {
        if (Objects.isNull(f)) return null;
        try
        {
            long crc32FromFile = f.readLong();
            int dataLen = f.readInt();
            int hasReadLen = (Long.SIZE + Integer.SIZE) / Byte.SIZE;
            if (f.length() - hasReadLen < dataLen) return null;
            byte[] data = new byte[dataLen];
            int readLen = f.read(data);
            if (readLen != dataLen) return null;
            long crc32FromData = getCRC32(data);
            if (crc32FromFile != crc32FromData) return null;
            Method method = clazz.getMethod("parseFrom", byte[].class);
            T message = (T) method.invoke(clazz, data);
            return message;
        }
        catch (Exception e)
        {
            return null;
        }
    }

    public <T extends Message> void write(RandomAccessFile f, T data)
    {
        byte[] bytes = data.toByteArray();
        long crc32 = getCRC32(bytes);
        try
        {
            f.writeLong(crc32);
            f.writeInt(bytes.length);
            f.write(bytes);
        }
        catch (Exception e)
        {
            throw new RuntimeException("write proto to file failed");
        }
    }

    public long getCRC32(byte[] data)
    {
        CRC32 crc32 = new CRC32();
        crc32.update(data);
        return crc32.getValue();
    }

    public List<String> getSortedFilesInDirectory(String dirName, String rootDirName) throws IOException
    {
        List<String> fileList = new ArrayList<>();
        File rootDir = new File(rootDirName);
        File dir = new File(dirName);
        if (!rootDir.isDirectory() || !dir.isDirectory()) return fileList;
        String rootPath = rootDir.getCanonicalPath();
        if (!rootPath.endsWith("/")) rootPath += "/";
        File[] files = dir.listFiles();
        if (ArrayUtil.isEmpty(files)) return fileList;
        for (File file : files)
        {
            if (file.isDirectory()) fileList.addAll(getSortedFilesInDirectory(file.getCanonicalPath(), rootPath));
            else fileList.add(file.getCanonicalPath().substring(rootPath.length()));
        }
        Collections.sort(fileList);
        return fileList;
    }
}
