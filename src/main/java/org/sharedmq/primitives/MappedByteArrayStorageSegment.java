package org.sharedmq.primitives;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A segment within the {@link MappedByteArrayStorage}.<br/>
 * Each segment has fixed size and allocates arrays within itself.<br/>
 * <br/>
 * Segment has the following structure:
 * <ul>
 * <li>Header - the general information about segment;</li>
 * <li>Index Records Array and Free Records Queue - a variable size array of index records and heap values;</li>
 * <li>Unallocated Space - the space that will be used to allocate new index records and array contents;</li>
 * <li>Allocated Space - the space where existing arrays are stored.</li>
 * </ul>
 * <h1>Index Records Array and Deleted Records Queue</h1>
 * The array of indexed and deleted records serves two purposes:
 * <ol>
 * <li>Store information about stored arrays. This part is never moved.</li>
 * <li>Keep a queue (heap) of deleted records. This part gets reordered according to the heap rules.</li>
 * </ol>
 * So it is a two structures within the same address range.<br/>
 * It is possible because the heap of deleted records cannot contain more elements than the list of records.
 * <p>
 * <h1>Unallocated Space</h1>
 * The unallocated space gets cut at the beginning, when a new index record is allocated;
 * and gets cut at the end when a new array content is stored.<br/>
 * <h1>Array Allocation</h1>
 * When an array is added to the segment, its content is added to the end of the unallocated space
 * and information about the array size and position within segment is stored to the index record.<br/>
 * If there are free index records, then the record with minimal number is used for array.
 * If there are no free records, then a new index record is added to the beginning of the unallocated space.<br/>
 * The fact that the record with minimal number is used is important.
 * It guarantees that the end of the index has as much free records as possible,
 * allowing truncation of the index at the end.
 * <h1>Array Removal</h1>
 * When an array is deleted, its index record is marked as free
 * and its record number is placed to the heap of deleted records.<br/>
 * However, allocated space remains unchanged.
 * <h1>Garbage Collection</h1>
 * After array is deleted, some part of allocated space becomes unused.<br/>
 * Garbage collection allows to reclaim that space, by compacting the arrays contents
 * at the end of the segment and updating the index records.<br/>
 * <br/>
 * <h1>Thread-safety</h1>
 * This class is not thread-safe.<br/>
 * It also should not be reused within different operations, because it caches some values.<br/>
 */
public class MappedByteArrayStorageSegment {

    private static final Logger logger = LoggerFactory.getLogger(MappedByteArrayStorageSegment.class);

    private static final StorageAdapter<MappedByteArrayStorageSegmentHeader> SegmentHeaderStorageAdapter
            = MappedByteArrayStorageSegmentHeader.getStorageAdapter();

    private static final StorageAdapter<MappedByteArrayStorageIndexRecord> IndexRecordStorageAdapter
            = MappedByteArrayStorageIndexRecord.getStorageAdapter();

    /**
     * The size of one index record including its heap part.
     */
    public static final int IndexRecordSize = IndexRecordStorageAdapter.getRecordSize() + 4;

    private final MemoryMappedFile mappedFile;

    private final int segmentNumber;
    private final int segmentOffset;
    private final int segmentSize;

    private final MappedByteArrayStorageSegmentHeader header;

    private MappedByteArrayStorageSegment(
            MemoryMappedFile mappedFile,
            int segmentNumber,
            int segmentOffset,
            int segmentSize,
            MappedByteArrayStorageSegmentHeader header
    ) {
        this.mappedFile = mappedFile;
        this.segmentNumber = segmentNumber;
        this.segmentOffset = segmentOffset;
        this.segmentSize = segmentSize;
        this.header = header;
    }

    public static MappedByteArrayStorageSegment create(
            MemoryMappedFile mappedFile,
            int segmentNumber,
            int segmentOffset,
            int segmentSize
    ) {
        MappedByteArrayStorageSegmentHeader header = new MappedByteArrayStorageSegmentHeader();
        header.setIndexRecordCount(0);
        header.setFreeRecordCount(0);
        header.setLastNonFreeRecord(-1);
        header.setUnallocatedSpace(segmentSize - SegmentHeaderStorageAdapter.getRecordSize());
        header.setAllocatedSpace(0);
        header.setReleasedSpace(0);

        mappedFile.put(segmentOffset, header, SegmentHeaderStorageAdapter);

        return new MappedByteArrayStorageSegment(mappedFile, segmentNumber, segmentOffset, segmentSize, header);
    }

    public static MappedByteArrayStorageSegment read(
            MemoryMappedFile mappedFile,
            int segmentNumber,
            int segmentOffset,
            int segmentSize
    ) throws IOException {
        MappedByteArrayStorageSegmentHeader header
                = mappedFile.get(segmentOffset, SegmentHeaderStorageAdapter);
        return new MappedByteArrayStorageSegment(mappedFile, segmentNumber, segmentOffset, segmentSize, header);
    }

    /**
     * Reads the segment header from the memory-mapped file.
     *
     * @return The header of this segment.
     * @throws IOException If the header cannot be read.
     */
    public MappedByteArrayStorageSegmentHeader getHeader() throws IOException {
        return mappedFile.get(segmentOffset, SegmentHeaderStorageAdapter);
    }

    public boolean canAllocate(int arrayLength) {
        // Here we calculate values assuming that garbage collection will be performed.
        // In particular, we assume that free records in the end of the index will be removed.
        int reclaimableRecordsCount = header.getIndexRecordCount() - header.getLastNonFreeRecord() - 1;
        int reclaimableRecordsSpace = reclaimableRecordsCount * IndexRecordSize;
        int unallocatedSpace = reclaimableRecordsSpace + header.getUnallocatedSpace() + header.getReleasedSpace();

        int requiredUnallocatedSpace = arrayLength;
        boolean canReuseIndexRecord = header.getFreeRecordCount() > reclaimableRecordsCount;
        if (!canReuseIndexRecord) {
            requiredUnallocatedSpace += IndexRecordSize;
        }
        return unallocatedSpace >= requiredUnallocatedSpace;
    }

    public boolean canAllocateWithoutGarbageCollection(int arrayLength) {
        int requiredUnallocatedSpace = arrayLength;
        if (header.getFreeRecordCount() == 0) {
            requiredUnallocatedSpace += IndexRecordSize;
        }
        return header.getUnallocatedSpace() >= requiredUnallocatedSpace;
    }

    public MappedByteArrayStorageKey addArray(long recordId, byte[] array) throws IOException {
        if (canAllocateWithoutGarbageCollection(array.length)) {
            return allocateArray(recordId, array);
        } else if (canAllocate(array.length)) {
            collectGarbage();
            return allocateArray(recordId, array);
        } else {
            throw new IOException("The segment does not have enough free space for the array.");
        }
    }

    public byte[] getArray(MappedByteArrayStorageKey key) throws IOException {
        int recordNumber = key.getRecordNumber();
        if (recordNumber >= header.getIndexRecordCount()) {
            return null;
        }

        int recordOffset = getIndexRecordOffset(recordNumber);
        MappedByteArrayStorageIndexRecord record = mappedFile.get(recordOffset, IndexRecordStorageAdapter);

        if (record.isFree() || record.getRecordId() != key.getRecordId()) {
            return null;
        }

        byte[] array = new byte[record.getDataLength()];
        mappedFile.readBytes(segmentOffset + record.getDataOffset(), array, 0, record.getDataLength());
        return array;
    }

    public boolean deleteArray(MappedByteArrayStorageKey key) throws IOException {
        int recordNumber = key.getRecordNumber();
        if (recordNumber >= header.getIndexRecordCount()) {
            return false;
        }

        int recordOffset = getIndexRecordOffset(recordNumber);

        MappedByteArrayStorageIndexRecord record = mappedFile.get(recordOffset, IndexRecordStorageAdapter);

        if (record.isFree() || record.getRecordId() != key.getRecordId()) {
            return false;
        }

        record = record.markDeleted();
        mappedFile.put(recordOffset, record, IndexRecordStorageAdapter);

        pushFreeRecordNumber(recordNumber);

        if (recordNumber == header.getLastNonFreeRecord()) {
            int lastNonFreeRecord = getLastNonFreeRecord(recordNumber - 1);
            header.setLastNonFreeRecord(lastNonFreeRecord);
        }
        header.setReleasedSpace(header.getReleasedSpace() + record.getDataLength());

        mappedFile.put(segmentOffset, header, SegmentHeaderStorageAdapter);

        return true;
    }

    /**
     * Searches for the last non-free index record with the record number
     * lower than or equal to the given record number.<br/>
     * <br/>
     * This method has complexity of O(indexRecordCount) in the worst case.<br/>
     * However, when this method is called during deletion of all records from the index,
     * it will read each record only once, independently of the order of deletion.<br/>
     * So the average complexity for this method is O(1).
     *
     * @param recordNumber The number of the record to start searching from.
     * @return The record number of the last non-free record; or -1 if all records are free.
     */
    private int getLastNonFreeRecord(int recordNumber) throws IOException {
        while (recordNumber >= 0) {
            int recordOffset = getIndexRecordOffset(recordNumber);
            MappedByteArrayStorageIndexRecord record = mappedFile.get(recordOffset, IndexRecordStorageAdapter);
            if (!record.isFree()) {
                break;
            }
            recordNumber--;
        }
        return recordNumber;
    }

    private MappedByteArrayStorageKey allocateArray(long recordId, byte[] array) {

        int dataOffset = segmentSize - header.getAllocatedSpace() - array.length;
        mappedFile.writeBytes(segmentOffset + dataOffset, array, 0, array.length);

        header.setUnallocatedSpace(header.getUnallocatedSpace() - array.length);
        header.setAllocatedSpace(header.getAllocatedSpace() + array.length);

        MappedByteArrayStorageIndexRecord record
                = new MappedByteArrayStorageIndexRecord(recordId, dataOffset, array.length, false);

        int recordNumber;

        if (header.getFreeRecordCount() == 0) {
            recordNumber = header.getIndexRecordCount();
            header.setIndexRecordCount(recordNumber + 1);
            header.setUnallocatedSpace(header.getUnallocatedSpace() - IndexRecordSize);
        } else {
            recordNumber = pollFreeRecordNumber();
        }

        if (recordNumber > header.getLastNonFreeRecord()) {
            header.setLastNonFreeRecord(recordNumber);
        }

        // Note that heap part of the index is not updated here.
        // Heap is either remains unchanged or was already updated by the pollFreeRecordNumber method.
        int recordOffset = getIndexRecordOffset(recordNumber);
        mappedFile.put(recordOffset, record, IndexRecordStorageAdapter);

        mappedFile.put(segmentOffset, header, SegmentHeaderStorageAdapter);

        return new MappedByteArrayStorageKey(segmentNumber, recordNumber, recordId);
    }

    /**
     * Pushes a free record number to the heap.
     *
     * @param recordNumber The number of the free record.
     */
    private void pushFreeRecordNumber(int recordNumber) {

        int heapValueIndex = header.getFreeRecordCount();

        putHeapValueAt(heapValueIndex, recordNumber);

        header.setFreeRecordCount(heapValueIndex + 1);

        moveFreeRecordNumberUp(heapValueIndex);
    }

    /**
     * Retrieves the minimal number of a free record from the heap.<br/>
     * The returned value is removed from the heap.
     *
     * @return The number of the record.
     * @throws IllegalStateException If the heap is empty.
     */
    private int pollFreeRecordNumber() {
        int heapSize = header.getFreeRecordCount();

        if (header.getFreeRecordCount() == 0) {
            throw new IllegalStateException("The heap of free records is empty.");
        }

        int firstValue = getHeapValueAt(0);
        int lastValue = getHeapValueAt(heapSize - 1);

        putHeapValueAt(0, lastValue);

        header.setFreeRecordCount(heapSize - 1);

        moveFreeRecordNumberDown(0);

        return firstValue;
    }

    private void moveFreeRecordNumberUp(int valueIndex) {
        int value = getHeapValueAt(valueIndex);
        while (valueIndex > 0) {

            int parentValueIndex = (valueIndex - 1) / 2;
            int parentValue = getHeapValueAt(parentValueIndex);

            if (value >= parentValue) {
                break;
            }

            putHeapValueAt(parentValueIndex, value);
            putHeapValueAt(valueIndex, parentValue);

            valueIndex = parentValueIndex;
        }
    }

    private void moveFreeRecordNumberDown(int valueIndex) {
        int heapSize = header.getFreeRecordCount();
        int value = getHeapValueAt(valueIndex);
        while (valueIndex * 2 + 1 < heapSize) {

            int childValueIndex = valueIndex * 2 + 1;
            int childValue = getHeapValueAt(childValueIndex);

            {
                int childValueIndex2 = valueIndex * 2 + 2;
                if (childValueIndex2 < heapSize) {
                    int childValue2 = getHeapValueAt(childValueIndex2);
                    if (childValue2 < childValue) {
                        childValue = childValue2;
                        childValueIndex = childValueIndex2;
                    }
                }
            }

            if (value <= childValue) {
                break;
            }

            putHeapValueAt(childValueIndex, value);
            putHeapValueAt(valueIndex, childValue);

            valueIndex = childValueIndex;
        }
    }

    public void collectGarbage() throws IOException {

        Stopwatch stopwatch = Stopwatch.createStarted();

        int removedRecordsCount = header.getIndexRecordCount() - header.getLastNonFreeRecord() - 1;
        // We can get rid of the lastNonFreeRecord parameter,
        // if index record array truncation is moved to the deleteArray method.
        header.setIndexRecordCount(header.getLastNonFreeRecord() + 1);
        header.setFreeRecordCount(header.getFreeRecordCount() - removedRecordsCount);

        int nextHeapIndex = 0;

        List<MappedByteArrayStorageIndexRecord> allRecords = new ArrayList<>();
        int allocatedSpace = 0;
        for (int i = 0; i < header.getIndexRecordCount(); i++) {
            int recordOffset = getIndexRecordOffset(i);
            MappedByteArrayStorageIndexRecord record = mappedFile.get(recordOffset, IndexRecordStorageAdapter);
            allRecords.add(record);
            if (!record.isFree()) {
                allocatedSpace += record.getDataLength();
            } else {
                putHeapValueAt(nextHeapIndex++, i);
            }
        }

        byte[] tempBuffer = new byte[allocatedSpace];
        int firstDataOffset = segmentSize - allocatedSpace;
        int nextDataOffset = firstDataOffset;
        int nextTempOffset = 0;

        for (int recordNumber = 0; recordNumber < allRecords.size(); recordNumber++) {
            MappedByteArrayStorageIndexRecord record = allRecords.get(recordNumber);
            if (!record.isFree()) {

                int dataLength = record.getDataLength();

                mappedFile.readBytes(segmentOffset + record.getDataOffset(), tempBuffer, nextTempOffset, dataLength);

                record = record.relocateData(nextDataOffset);
                int recordOffset = getIndexRecordOffset(recordNumber);
                mappedFile.put(recordOffset, record, IndexRecordStorageAdapter);

                nextDataOffset += dataLength;
                nextTempOffset += dataLength;
            }
        }

        mappedFile.writeBytes(segmentOffset + firstDataOffset, tempBuffer, 0, allocatedSpace);

        header.setAllocatedSpace(allocatedSpace);
        header.setReleasedSpace(0);
        header.setUnallocatedSpace(
                segmentSize
                        - SegmentHeaderStorageAdapter.getRecordSize()
                        - header.getIndexRecordCount() * IndexRecordSize
                        - allocatedSpace);

        mappedFile.put(segmentOffset, header, SegmentHeaderStorageAdapter);

        logger.trace("Garbage collection completed within " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms.");
    }

    private int getHeapValueAt(int valueIndex) {
        return mappedFile.getInt(getHeapValueOffset(valueIndex));
    }

    private void putHeapValueAt(int valueIndex, int value) {
        mappedFile.putInt(getHeapValueOffset(valueIndex), value);
    }

    private int getHeapValueOffset(int recordNumber) {
        // heap value follows the index record
        return getIndexRecordOffset(recordNumber) + IndexRecordStorageAdapter.getRecordSize();
    }

    private int getIndexRecordOffset(int recordNumber) {
        return segmentOffset
                + SegmentHeaderStorageAdapter.getRecordSize()
                + recordNumber * IndexRecordSize;
    }
}
