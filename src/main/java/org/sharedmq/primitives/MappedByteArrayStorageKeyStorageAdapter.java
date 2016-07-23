package org.sharedmq.primitives;

import java.nio.ByteBuffer;

/**
 * An adapter for the {@link MappedByteArrayStorageKey}.
 */
public class MappedByteArrayStorageKeyStorageAdapter implements StorageAdapter<MappedByteArrayStorageKey> {

    private static final MappedByteArrayStorageKeyStorageAdapter instance
            = new MappedByteArrayStorageKeyStorageAdapter();

    public static MappedByteArrayStorageKeyStorageAdapter getInstance() {
        return instance;
    }

    @Override
    public int getRecordSize() {
        return 4 * 2 + 8;
    }

    @Override
    public void store(ByteBuffer buffer, MappedByteArrayStorageKey key) {

        if (key == null) {
            throw new IllegalArgumentException(
                    "The MappedByteArrayStorageKeyStorageAdapter does not support null records.");
        }

        buffer.putInt(key.getSegmentNumber());
        buffer.putInt(key.getRecordNumber());
        buffer.putLong(key.getRecordId());
    }

    @Override
    public MappedByteArrayStorageKey load(ByteBuffer buffer) {
        int segmentNumber = buffer.getInt();
        int recordNumber = buffer.getInt();
        long recordId = buffer.getLong();
        return new MappedByteArrayStorageKey(segmentNumber, recordNumber, recordId);
    }
}
