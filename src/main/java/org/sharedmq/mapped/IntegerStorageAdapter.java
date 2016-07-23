package org.sharedmq.mapped;

import java.nio.ByteBuffer;

/**
 * A {@link StorageAdapter} implementation for the records of type {@link Integer}.
 */
public class IntegerStorageAdapter implements StorageAdapter<Integer> {

    private static final IntegerStorageAdapter instance = new IntegerStorageAdapter();

    public static IntegerStorageAdapter getInstance() {
        return instance;
    }

    @Override
    public int getRecordSize() {
        return 4;
    }

    @Override
    public void store(ByteBuffer buffer, Integer record) {
        if (record == null) {
            throw new IllegalArgumentException("The IntegerStorageAdapter does not support null records.");
        }
        buffer.putInt(record);
    }

    @Override
    public Integer load(ByteBuffer buffer) {
        return buffer.getInt();
    }
}
