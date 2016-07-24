package org.sharedmq.internals;

import org.sharedmq.primitives.StorageAdapter;

import java.nio.ByteBuffer;

/**
 * A storage adapter for the {@link PriorityQueueRecord}.
 */
public class PriorityQueueRecordStorageAdapter implements StorageAdapter<PriorityQueueRecord> {

    private static PriorityQueueRecordStorageAdapter instance = new PriorityQueueRecordStorageAdapter();

    public static PriorityQueueRecordStorageAdapter getInstance() {
        return instance;
    }

    @Override
    public int getRecordSize() {
        return 4 + 8;
    }

    @Override
    public void store(ByteBuffer buffer, PriorityQueueRecord record) {
        if (record == null) {
            throw new IllegalArgumentException(
                    "The PriorityQueueRecordStorageAdapter does not support null records.");
        }
        buffer.putInt(record.getMessageNumber());
        buffer.putLong(record.getVisibleSince());
    }

    @Override
    public PriorityQueueRecord load(ByteBuffer buffer) {
        int messageNumber = buffer.getInt();
        long visibleSince = buffer.getLong();
        return new PriorityQueueRecord(messageNumber, visibleSince);
    }
}
