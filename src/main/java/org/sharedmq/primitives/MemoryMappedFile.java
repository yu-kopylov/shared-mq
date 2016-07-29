package org.sharedmq.primitives;

import org.sharedmq.util.IOUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A memory-mapped file.
 */
public class MemoryMappedFile implements DataFile {

    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private MappedByteBuffer buffer;

    //todo: remove capacity parameter (it should be always zero)
    public MemoryMappedFile(File file, int capacity) throws IOException {
        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            channel = randomAccessFile.getChannel();
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
        } catch (Throwable e) {
            buffer = null;
            IOUtils.closeOnError(e, channel, randomAccessFile);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        IOUtils.close(channel, randomAccessFile);
    }

    @Override
    public long length() throws IOException {
        return randomAccessFile.length();
    }

    @Override
    public int capacity() {
        return buffer.capacity();
    }

    @Override
    public void ensureCapacity(int capacity) throws IOException {
        if (buffer.capacity() < capacity) {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
        }
    }

    @Override
    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    @Override
    public long getLong(int offset) {
        return buffer.getLong(offset);
    }

    //todo: test
    @Override
    public void readBytes(int fileOffset, byte[] array, int arrayOffset, int dataLength) {
        buffer.position(fileOffset);
        buffer.get(array, arrayOffset, dataLength);
    }

    @Override
    public <TRecord> TRecord get(int offset, StorageAdapter<TRecord> adapter) throws IOException {
        buffer.position(offset);
        return adapter.load(buffer);
    }

    @Override
    public void putInt(int offset, int value) {
        buffer.putInt(offset, value);
    }

    @Override
    public void putLong(int offset, long value) {
        buffer.putLong(offset, value);
    }

    @Override
    public void writeBytes(int offset, byte[] array, int arrayOffset, int dataLength) {
        buffer.position(offset);
        buffer.put(array, arrayOffset, dataLength);
    }

    @Override
    public <TRecord> void put(int offset, TRecord value, StorageAdapter<TRecord> adapter) {
        buffer.position(offset);
        adapter.store(buffer, value);
    }
}
