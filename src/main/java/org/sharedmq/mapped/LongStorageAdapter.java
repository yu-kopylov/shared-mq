package org.sharedmq.mapped;

import java.nio.ByteBuffer;

/**
 * A {@link StorageAdapter} implementation for the records of type {@link Long}.
 */
public class LongStorageAdapter implements StorageAdapter<Long> {

    private static final LongStorageAdapter instance = new LongStorageAdapter();

    public static LongStorageAdapter getInstance() {
        return instance;
    }

    @Override
    public int getRecordSize() {
        return 8;
    }

    @Override
    public void store(ByteBuffer buffer, Long record) {
        if (record == null) {
            throw new IllegalArgumentException("The LongStorageAdapter does not support null records.");
        }
        buffer.putLong(record);
    }

    @Override
    public Long load(ByteBuffer buffer) {
        return buffer.getLong();
    }
}
