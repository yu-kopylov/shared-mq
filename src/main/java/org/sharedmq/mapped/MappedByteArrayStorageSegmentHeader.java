package org.sharedmq.mapped;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The header of the {@link MappedByteArrayStorageSegment}.
 */
public class MappedByteArrayStorageSegmentHeader {

    public static final int ByteSize = 7 * 4;

    private static final int DataMarker = 0x5345474D;

    /**
     * The number of index records.
     */
    private int indexRecordCount;

    /**
     * The number of free records.<br/>
     * It is always less than or equal to the number of index records.
     */
    private int freeRecordCount;

    /**
     * The number of the last record that contain data.<br/>
     * It is used to estimate, how much space can be freed by truncating the index record array.
     */
    private int lastNonFreeRecord;

    /**
     * The size of the unallocated space.
     */
    private int unallocatedSpace;

    /**
     * The size of the allocated space.<br/>
     * It is used to determine a position for a new array content.
     */
    private int allocatedSpace;

    /**
     * The amount space in the allocated parts of the segment that is no longer used.<br/>
     * It is used to determine, how much space can be reclaimed by garbage collection.
     */
    private int releasedSpace;

    public int getIndexRecordCount() {
        return indexRecordCount;
    }

    public void setIndexRecordCount(int indexRecordCount) {
        this.indexRecordCount = indexRecordCount;
    }

    public int getFreeRecordCount() {
        return freeRecordCount;
    }

    public void setFreeRecordCount(int freeRecordCount) {
        this.freeRecordCount = freeRecordCount;
    }

    public int getLastNonFreeRecord() {
        return lastNonFreeRecord;
    }

    public void setLastNonFreeRecord(int lastNonFreeRecord) {
        this.lastNonFreeRecord = lastNonFreeRecord;
    }

    public int getUnallocatedSpace() {
        return unallocatedSpace;
    }

    public void setUnallocatedSpace(int unallocatedSpace) {
        this.unallocatedSpace = unallocatedSpace;
    }

    public int getAllocatedSpace() {
        return allocatedSpace;
    }

    public void setAllocatedSpace(int allocatedSpace) {
        this.allocatedSpace = allocatedSpace;
    }

    public int getReleasedSpace() {
        return releasedSpace;
    }

    public void setReleasedSpace(int releasedSpace) {
        this.releasedSpace = releasedSpace;
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putInt(DataMarker);
        buffer.putInt(indexRecordCount);
        buffer.putInt(freeRecordCount);
        buffer.putInt(lastNonFreeRecord);
        buffer.putInt(unallocatedSpace);
        buffer.putInt(allocatedSpace);
        buffer.putInt(releasedSpace);
    }

    public void readFrom(ByteBuffer buffer) throws IOException {
        int dataMarker = buffer.getInt();
        if (dataMarker != DataMarker) {
            throw new IOException("Invalid MappedByteArrayStorageSegmentHeader.");
        }
        indexRecordCount = buffer.getInt();
        freeRecordCount = buffer.getInt();
        lastNonFreeRecord = buffer.getInt();
        unallocatedSpace = buffer.getInt();
        allocatedSpace = buffer.getInt();
        releasedSpace = buffer.getInt();
    }
}
