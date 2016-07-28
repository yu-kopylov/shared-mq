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

    public long getLong(int offset) {
        return buffer.getLong(offset);
    }

    //todo: test
    public byte[] getBytes(int fileOffset, int dataLength) {
        byte[] bytes = new byte[dataLength];
        getBytes(fileOffset, bytes, 0, dataLength);
        return bytes;
    }

    //todo: test
    public void getBytes(int fileOffset, byte[] array, int arrayOffset, int dataLength) {
        buffer.position(fileOffset);
        buffer.get(array, arrayOffset, dataLength);
    }

    public <TRecord> TRecord get(int offset, StorageAdapter<TRecord> adapter) throws IOException {
        buffer.position(offset);
        return adapter.load(buffer);
    }

    public void putInt(int offset, int value) {
        buffer.putInt(offset, value);
    }

    public void putLong(int offset, long value) {
        buffer.putLong(offset, value);
    }

    public void putBytes(int offset, byte[] array) {
        buffer.position(offset);
        buffer.put(array, 0, array.length);
    }

    public <TRecord> void put(int offset, TRecord value, StorageAdapter<TRecord> adapter) {
        buffer.position(offset);
        adapter.store(buffer, value);
    }
}
