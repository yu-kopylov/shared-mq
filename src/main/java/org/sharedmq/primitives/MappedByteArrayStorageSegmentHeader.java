package org.sharedmq.primitives;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The header of the {@link MappedByteArrayStorageSegment}.
 */
public class MappedByteArrayStorageSegmentHeader {

    private static final int DataMarker = 0x5345474D;

    private static final StorageAdapter<MappedByteArrayStorageSegmentHeader> storageAdapter
            = new SegmentHeaderStorageAdapter();

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

    public static StorageAdapter<MappedByteArrayStorageSegmentHeader> getStorageAdapter() {
        return storageAdapter;
    }

    private static class SegmentHeaderStorageAdapter implements StorageAdapter<MappedByteArrayStorageSegmentHeader> {
        @Override
        public int getRecordSize() {
            return 7 * 4;
        }

        @Override
        public void store(ByteBuffer buffer, MappedByteArrayStorageSegmentHeader header) {
            buffer.putInt(DataMarker);
            buffer.putInt(header.getIndexRecordCount());
            buffer.putInt(header.getFreeRecordCount());
            buffer.putInt(header.getLastNonFreeRecord());
            buffer.putInt(header.getUnallocatedSpace());
            buffer.putInt(header.getAllocatedSpace());
            buffer.putInt(header.getReleasedSpace());
        }

        @Override
        public MappedByteArrayStorageSegmentHeader load(ByteBuffer buffer) throws IOException {
            int dataMarker = buffer.getInt();
            if (dataMarker != DataMarker) {
                throw new IOException("Invalid MappedByteArrayStorageSegmentHeader.");
            }
            MappedByteArrayStorageSegmentHeader header = new MappedByteArrayStorageSegmentHeader();
            header.setIndexRecordCount(buffer.getInt());
            header.setFreeRecordCount(buffer.getInt());
            header.setLastNonFreeRecord(buffer.getInt());
            header.setUnallocatedSpace(buffer.getInt());
            header.setAllocatedSpace(buffer.getInt());
            header.setReleasedSpace(buffer.getInt());
            return header;
        }
    }
}
