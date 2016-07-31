package org.sharedmq.primitives;

import org.sharedmq.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/**
 * A storage for byte arrays mapped to a file.<br/>
 * <br/>
 * This class is not thread-safe.
 */
public class MappedByteArrayStorage implements Closeable {

    public static final int SegmentSize = 2 * 1024 * 1024;

    private static final long MaxFileSize = Integer.MAX_VALUE;

    private static final int FileMarker = 0x4D424153;
    private static final int FileMarkerOffset = 0;
    private static final int SegmentSizeOffset = FileMarkerOffset + 4;
    private static final int SegmentCountOffset = SegmentSizeOffset + 4;
    private static final int NextRecordIdOffset = SegmentCountOffset + 4;
    private static final int LastUsedSegmentOffset = NextRecordIdOffset + 8;
    private static final int HeaderSize = LastUsedSegmentOffset + 4;

    private final DataFile dataFile;

    public MappedByteArrayStorage(DataFile file) throws IOException {
        dataFile = file;
        if (file.length() == 0) {
            createFileHeader();
        } else {
            checkFileHeader();
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(dataFile);
    }

    private void createFileHeader() throws IOException {
        dataFile.ensureCapacity(HeaderSize);
        dataFile.putInt(FileMarkerOffset, FileMarker);
        dataFile.putInt(SegmentSizeOffset, SegmentSize);
        dataFile.putInt(SegmentCountOffset, 0);
        dataFile.putInt(LastUsedSegmentOffset, -1);
        dataFile.putInt(NextRecordIdOffset, 0);
    }

    private void checkFileHeader() throws IOException {

        long fileSize = dataFile.length();

        if (fileSize > MaxFileSize) {
            throw new IOException("The file is too big to be a MappedByteArrayStorage file.");
        }
        if (fileSize < HeaderSize) {
            throw new IOException("The file is too short to be a MappedByteArrayStorage file.");
        }

        dataFile.ensureCapacity((int) fileSize);

        int marker = dataFile.getInt(FileMarkerOffset);
        if (marker != FileMarker) {
            throw new IOException("The file does not contain the MappedByteArrayStorage file marker.");
        }

        int fileSegmentSize = dataFile.getInt(SegmentSizeOffset);
        if (SegmentSize != fileSegmentSize) {
            throw new IOException(
                    "The MappedByteArrayStorage file has a different segment size (" + fileSegmentSize + ").");
        }

        int segmentCount = dataFile.getInt(SegmentCountOffset);
        if (segmentCount < 0) {
            throw new IOException(
                    "The MappedByteArrayStorage file has an invalid segment count (" + segmentCount + ").");
        }

        long requiredSize = getRequiredSize(segmentCount);
        if (requiredSize > fileSize) {
            throw new IOException("The MappedByteArrayStorage file is incomplete.");
        }
    }

    public MappedByteArrayStorageKey add(byte[] array) throws IOException {
        ensureBufferCapacity(getSegmentCount());
        MappedByteArrayStorageSegment segment = findFreeSegment(array.length);
        if (segment == null) {
            segment = createSegment();
        }
        dataFile.putInt(LastUsedSegmentOffset, segment.getSegmentNumber());
        long recordId = getNextRecordId();
        return segment.addArray(recordId, array);
    }

    public byte[] get(MappedByteArrayStorageKey key) throws IOException {
        ensureBufferCapacity(getSegmentCount());
        checkSegmentKey(key);

        int segmentNumber = key.getSegmentNumber();
        MappedByteArrayStorageSegment segment = MappedByteArrayStorageSegment.read(
                dataFile,
                segmentNumber,
                getSegmentOffset(segmentNumber),
                SegmentSize);

        return segment.getArray(key);
    }

    public boolean delete(MappedByteArrayStorageKey key) throws IOException {
        ensureBufferCapacity(getSegmentCount());
        checkSegmentKey(key);

        int segmentNumber = key.getSegmentNumber();
        MappedByteArrayStorageSegment segment = MappedByteArrayStorageSegment.read(
                dataFile,
                segmentNumber,
                getSegmentOffset(segmentNumber),
                SegmentSize);

        return segment.deleteArray(key);
    }

    private MappedByteArrayStorageSegment findFreeSegment(int arrayLength) throws IOException {

        int lastUsedSegmentNumber = dataFile.getInt(LastUsedSegmentOffset);

        int segmentCount = getSegmentCount();
        for (int i = 0; i < segmentCount; i++) {
            // Usually, the most empty segment is either the last used segment or the segment next to it.
            int segmentNumber = (lastUsedSegmentNumber + i) % segmentCount;
            MappedByteArrayStorageSegment segment = MappedByteArrayStorageSegment.read(
                    dataFile,
                    segmentNumber,
                    getSegmentOffset(segmentNumber),
                    SegmentSize);
            if (segment.canAllocate(arrayLength)) {
                return segment;
            }
        }
        return null;
    }

    private MappedByteArrayStorageSegment createSegment() throws IOException {
        int segmentNumber = getSegmentCount();
        ensureBufferCapacity(segmentNumber + 1);
        MappedByteArrayStorageSegment segment = MappedByteArrayStorageSegment.create(
                dataFile,
                segmentNumber,
                getSegmentOffset(segmentNumber),
                SegmentSize);
        dataFile.putInt(SegmentCountOffset, segmentNumber + 1);
        return segment;
    }

    private void ensureBufferCapacity(int segmentCount) throws IOException {
        long requiredFileSize = getRequiredSize(segmentCount);
        if (requiredFileSize > MaxFileSize) {
            throw new IOException("The MappedByteArrayStorage cannot be bigger than " + MaxFileSize + " bytes.");
        }
        dataFile.ensureCapacity((int) requiredFileSize);
    }

    private void checkSegmentKey(MappedByteArrayStorageKey key) {
        int segmentNumber = key.getSegmentNumber();
        if (segmentNumber < 0 || segmentNumber >= getSegmentCount()) {
            // Currently the MappedByteArrayStorage never removes unused segments.
            // If it will remove segments,
            // then segmentNumber >= getSegmentCount() would be a valid situation.
            throw new IllegalArgumentException("The key has an invalid segmentNumber (" + segmentNumber + ").");
        }
    }

    private static int getSegmentOffset(int segmentNumber) {
        return HeaderSize + segmentNumber * SegmentSize;
    }

    private static long getRequiredSize(int segmentCount) {
        return HeaderSize + segmentCount * (long) SegmentSize;
    }

    private int getSegmentCount() {
        return dataFile.getInt(SegmentCountOffset);
    }

    private long getNextRecordId() throws IOException {
        // Even if we would generate 1000000000 id values per second,
        // it would take 584 years to reach a collision.
        long recordId = dataFile.getLong(NextRecordIdOffset);
        dataFile.putLong(NextRecordIdOffset, recordId + 1);
        return recordId;
    }
}
