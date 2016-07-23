package org.sharedmq.mapped;

/**
 * A key that uniquely identifies a position of a byte array within the {@link MappedByteArrayStorage}.
 */
public class MappedByteArrayStorageKey {

    private final int segmentNumber;
    private final int recordNumber;
    private final long recordId;

    public MappedByteArrayStorageKey(int segmentNumber, int recordNumber, long recordId) {
        this.segmentNumber = segmentNumber;
        this.recordNumber = recordNumber;
        this.recordId = recordId;
    }

    /**
     * The number of segment where record is stored.
     */
    public int getSegmentNumber() {
        return segmentNumber;
    }

    /**
     * The number of record with segment.
     */
    public int getRecordNumber() {
        return recordNumber;
    }

    /**
     * The unique identifier of the stored byte array.<br/>
     * Records within segment are reusable,
     * so we need an additional key to check that this key belongs to the stored byte array.
     */
    public long getRecordId() {
        return recordId;
    }
}
