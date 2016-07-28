package org.sharedmq.primitives;

import org.sharedmq.util.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

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
    private static final int SegmentSizeOffset = 4;
    private static final int SegmentCountOffset = 8;
    private static final int NextRecordIdOffset = 12;
    private static final int HeaderSize = 20;

    private MemoryMappedFile mappedFile;

    public MappedByteArrayStorage(File file) throws IOException {
        if (file.exists()) {
            mappedFile = openFile(file, file.length());
        } else {
            mappedFile = createFile(file);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(mappedFile);
    }

    private static MemoryMappedFile createFile(File file) throws IOException {
        MemoryMappedFile mappedFile = new MemoryMappedFile(file, (int) HeaderSize);
        try {
            mappedFile.putInt(FileMarkerOffset, FileMarker);
            mappedFile.putInt(SegmentSizeOffset, SegmentSize);
            mappedFile.putInt(SegmentCountOffset, 0);
        } catch (Throwable e) {
            IOUtils.closeOnError(e, mappedFile);
            throw e;
        }
        return mappedFile;
    }

    private static MemoryMappedFile openFile(File file, long fileSize) throws IOException {

        if (fileSize > MaxFileSize) {
            throw new IOException("The file is too big to be a MappedByteArrayStorage file.");
        }
        if (fileSize < HeaderSize) {
            throw new IOException("The file is too short to be a MappedByteArrayStorage file.");
        }

        MemoryMappedFile mappedFile = new MemoryMappedFile(file, (int) fileSize);
        try {
            int marker = mappedFile.getInt(FileMarkerOffset);
            if (marker != FileMarker) {
                throw new IOException("The file does not contain the MappedByteArrayStorage file marker.");
            }

            int fileSegmentSize = mappedFile.getInt(SegmentSizeOffset);
            if (SegmentSize != fileSegmentSize) {
                throw new IOException(
                        "The MappedByteArrayStorage file has a different segment size (" + fileSegmentSize + ").");
            }

            int segmentCount = mappedFile.getInt(SegmentCountOffset);
            if (segmentCount < 0) {
                throw new IOException(
                        "The MappedByteArrayStorage file has an invalid segment count (" + segmentCount + ").");
            }

            long requiredSize = getRequiredSize(segmentCount);
            if (requiredSize > fileSize) {
                throw new IOException("The MappedByteArrayStorage file is incomplete.");
            }
        } catch (Throwable e) {
            IOUtils.closeOnError(e, mappedFile);
            throw e;
        }
        return mappedFile;
    }

    public MappedByteArrayStorageKey add(byte[] array) throws IOException {
        ensureBufferCapacity(getSegmentCount());
        MappedByteArrayStorageSegment segment = findFreeSegment(array.length);
        if (segment == null) {
            segment = createSegment();
        }
        long recordId = getNextRecordId();
        return segment.addArray(recordId, array);
    }

    public byte[] get(MappedByteArrayStorageKey key) throws IOException {
        ensureBufferCapacity(getSegmentCount());
        checkSegmentKey(key);

        int segmentNumber = key.getSegmentNumber();
        MappedByteArrayStorageSegment segment = MappedByteArrayStorageSegment.read(
                mappedFile,
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
                mappedFile,
                segmentNumber,
                getSegmentOffset(segmentNumber),
                SegmentSize);

        return segment.deleteArray(key);
    }

    private MappedByteArrayStorageSegment findFreeSegment(int arrayLength) throws IOException {

        // This is not a very smart algorithm, and can be improved.
        // However, it is sufficient for now.

        int segmentCount = getSegmentCount();
        for (int i = 0; i < segmentCount; i++) {
            MappedByteArrayStorageSegment segment = MappedByteArrayStorageSegment.read(
                    mappedFile,
                    i,
                    getSegmentOffset(i),
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
                mappedFile,
                segmentNumber,
                getSegmentOffset(segmentNumber),
                SegmentSize);
        mappedFile.putInt(SegmentCountOffset, segmentNumber + 1);
        return segment;
    }

    private void ensureBufferCapacity(int segmentCount) throws IOException {
        long requiredFileSize = getRequiredSize(segmentCount);
        if (requiredFileSize > MaxFileSize) {
            throw new IOException("The MappedByteArrayStorage cannot be bigger than " + MaxFileSize + " bytes.");
        }
        mappedFile.ensureCapacity((int) requiredFileSize);
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
        return mappedFile.getInt(SegmentCountOffset);
    }

    private long getNextRecordId() {
        // Even if we would generate 1000000000 id values per second,
        // it would take 584 years to reach a collision.
        long recordId = mappedFile.getLong(NextRecordIdOffset);
        mappedFile.putLong(NextRecordIdOffset, recordId + 1);
        return recordId;
    }
}
