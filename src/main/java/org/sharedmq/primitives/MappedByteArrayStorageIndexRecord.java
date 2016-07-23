package org.sharedmq.primitives;

import java.nio.ByteBuffer;

/**
 * An index record that is stored in {@link MappedByteArrayStorageSegment}.<br/>
 * <br/>
 * This class is immutable.
 */
public class MappedByteArrayStorageIndexRecord {

    public static final int ByteSize = 8 + 2 * 4 + 1;

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

    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(recordId);
        buffer.putInt(dataOffset);
        buffer.putInt(dataLength);
        buffer.put((byte) (free ? 1 : 0));
    }

    public static MappedByteArrayStorageIndexRecord readFrom(ByteBuffer buffer) {
        long recordId = buffer.getLong();
        int dataOffset = buffer.getInt();
        int dataLength = buffer.getInt();
        boolean free = buffer.get() != 0;
        return new MappedByteArrayStorageIndexRecord(recordId, dataOffset, dataLength, free);
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
}
