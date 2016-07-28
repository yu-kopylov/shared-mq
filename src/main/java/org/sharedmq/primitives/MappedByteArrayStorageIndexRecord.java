package org.sharedmq.primitives;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An index record that is stored in {@link MappedByteArrayStorageSegment}.<br/>
 * <br/>
 * This class is immutable.
 */
public class MappedByteArrayStorageIndexRecord {

    private static final IndexRecordStorageAdapter storageAdapter = new IndexRecordStorageAdapter();

    /**
     * The unique identifier of the stored byte array.<br/>
     * Records within segment are reusable,
     * so we need an additional key to check that record still belongs to an expected array.
     */
    private final long recordId;

    /**
     * The offset of the array data within segment.
     */
    private final int dataOffset;

    /**
     * The length of the array data.
     */
    private final int dataLength;

    /**
     * Marker that shows that array was deleted and this record can be reused.
     */
    private final boolean free;

    public MappedByteArrayStorageIndexRecord(long recordId, int dataOffset, int dataLength, boolean free) {
        this.recordId = recordId;
        this.dataOffset = dataOffset;
        this.dataLength = dataLength;
        this.free = free;
    }

    public long getRecordId() {
        return recordId;
    }

    public int getDataOffset() {
        return dataOffset;
    }

    public int getDataLength() {
        return dataLength;
    }

    public boolean isFree() {
        return free;
    }

    /**
     * Returns a new record that is marked as free.
     */
    public MappedByteArrayStorageIndexRecord markDeleted() {
        return new MappedByteArrayStorageIndexRecord(recordId, dataOffset, dataLength, true);
    }

    /**
     * Returns a new record with the given offset of the array data within segment.
     *
     * @param dataOffset A new offset of the array data within segment.
     */
    public MappedByteArrayStorageIndexRecord relocateData(int dataOffset) {
        return new MappedByteArrayStorageIndexRecord(recordId, dataOffset, dataLength, free);
    }

    public static StorageAdapter<MappedByteArrayStorageIndexRecord> getStorageAdapter() {
        return storageAdapter;
    }

    private static class IndexRecordStorageAdapter implements StorageAdapter<MappedByteArrayStorageIndexRecord> {
        @Override
        public int getRecordSize() {
            return 8 + 2 * 4 + 1;
        }

        @Override
        public void store(ByteBuffer buffer, MappedByteArrayStorageIndexRecord record) {
            buffer.putLong(record.getRecordId());
            buffer.putInt(record.getDataOffset());
            buffer.putInt(record.getDataLength());
            buffer.put((byte) (record.isFree() ? 1 : 0));
        }

        @Override
        public MappedByteArrayStorageIndexRecord load(ByteBuffer buffer) throws IOException {
            long recordId = buffer.getLong();
            int dataOffset = buffer.getInt();
            int dataLength = buffer.getInt();
            boolean free = buffer.get() != 0;
            return new MappedByteArrayStorageIndexRecord(recordId, dataOffset, dataLength, free);
        }
    }
}
