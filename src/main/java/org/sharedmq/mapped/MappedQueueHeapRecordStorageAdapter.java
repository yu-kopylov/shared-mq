package org.sharedmq.mapped;

import org.sharedmq.primitives.StorageAdapter;

import java.nio.ByteBuffer;

/**
 * A storage adapter for the {@link MappedQueueHeapRecord}.
 */
public class MappedQueueHeapRecordStorageAdapter implements StorageAdapter<MappedQueueHeapRecord> {

    private static MappedQueueHeapRecordStorageAdapter instance = new MappedQueueHeapRecordStorageAdapter();

    public static MappedQueueHeapRecordStorageAdapter getInstance() {
        return instance;
    }

    @Override
    public int getRecordSize() {
        return 4 + 8;
    }

    @Override
    public void store(ByteBuffer buffer, MappedQueueHeapRecord record) {
        if (record == null) {
            throw new IllegalArgumentException(
                    "The MappedQueueHeapRecordStorageAdapter does not support null records.");
        }
        buffer.putInt(record.getMessageNumber());
        buffer.putLong(record.getVisibleSince());
    }

    @Override
    public MappedQueueHeapRecord load(ByteBuffer buffer) {
        int messageNumber = buffer.getInt();
        long visibleSince = buffer.getLong();
        return new MappedQueueHeapRecord(messageNumber, visibleSince);
    }
}
