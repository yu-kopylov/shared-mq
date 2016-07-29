package org.sharedmq.primitives;

import java.io.Closeable;
import java.io.IOException;

/**
 * An interface for data files with random access.
 */
public interface DataFile extends Closeable {

    //todo: describe
    long length() throws IOException;

    //todo: describe
    int capacity();

    //todo: describe
    void ensureCapacity(int capacity) throws IOException;

    int getInt(int offset);

    long getLong(int offset);

    void readBytes(int fileOffset, byte[] buffer, int bufferOffset, int dataLength);

    <TRecord> TRecord get(int offset, StorageAdapter<TRecord> adapter) throws IOException;

    void putInt(int offset, int value) throws IOException;

    void putLong(int offset, long value) throws IOException;

    void writeBytes(int offset, byte[] buffer, int bufferOffset, int dataLength) throws IOException;

    <TRecord> void put(int offset, TRecord value, StorageAdapter<TRecord> adapter) throws IOException;
}
