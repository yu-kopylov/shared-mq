package org.sharedmq.primitives;

import org.sharedmq.util.IOUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A memory-mapped file.
 */
public class MemoryMappedFile implements Closeable {

    private RandomAccessFile randomAccessFile;
    private FileChannel channel;
    private MappedByteBuffer buffer;

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

    public long capacity() {
        return buffer.capacity();
    }

    public void ensureCapacity(int capacity) throws IOException {
        if (buffer.capacity() < capacity) {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
        }
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        IOUtils.close(channel, randomAccessFile);
    }

    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    public void putInt(int offset, int value) {
        buffer.putInt(offset, value);
    }

    public <TRecord> TRecord get(int offset, StorageAdapter<TRecord> adapter) {
        buffer.position(offset);
        return adapter.load(buffer);
    }

    public <TRecord> void put(int offset, StorageAdapter<TRecord> adapter, TRecord value) {
        buffer.position(offset);
        adapter.store(buffer, value);
    }
}
