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

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer buffer;

    public MappedByteArrayStorage(File file) throws IOException {
        try {

            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();

            long fileSize = randomAccessFile.getChannel().size();
            boolean newFile = fileSize == 0;
            if (newFile) {
                buffer = createFile(fileChannel);
            } else {
                buffer = openFile(fileChannel, fileSize);
            }

        } catch (Throwable e) {
            buffer = null;
            IOUtils.closeOnError(e, fileChannel, randomAccessFile);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        IOUtils.close(fileChannel, randomAccessFile);
    }

    private static MappedByteBuffer createFile(FileChannel fileChannel) throws IOException {
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, HeaderSize);
        buffer.putInt(FileMarkerOffset, FileMarker);
        buffer.putInt(SegmentSizeOffset, SegmentSize);
        buffer.putInt(SegmentCountOffset, 0);
        return buffer;
    }

    private static MappedByteBuffer openFile(FileChannel fileChannel, long fileSize) throws IOException {

        if (fileSize > MaxFileSize) {
            throw new IOException("The file is too big to be a MappedByteArrayStorage file.");
        }
        if (fileSize < HeaderSize) {
            throw new IOException("The file is too short to be a MappedByteArrayStorage file.");
        }

        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

        int marker = buffer.getInt(FileMarkerOffset);
        if (marker != FileMarker) {
            throw new IOException("The file does not contain a MappedByteArrayStorage file marker.");
        }

        int fileSegmentSize = buffer.getInt(SegmentSizeOffset);
        if (SegmentSize != fileSegmentSize) {
            throw new IOException(
                    "The MappedByteArrayStorage file has a different segment size (" + fileSegmentSize + ").");
        }

        int segmentCount = buffer.getInt(SegmentCountOffset);
        if (segmentCount < 0) {
            throw new IOException(
                    "The MappedByteArrayStorage file has an invalid segment count (" + segmentCount + ").");
        }

        long requiredSize = getRequiredSize(segmentCount);
        if (requiredSize > fileSize) {
            throw new IOException("The MappedByteArrayStorage file is incomplete.");
        }

        return buffer;
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
        checkKeySegment(key);

        int segmentNumber = key.getSegmentNumber();
        MappedByteArrayStorageSegment segment = MappedByteArrayStorageSegment.read(
                buffer,
                segmentNumber,
                getSegmentOffset(segmentNumber),
                SegmentSize);

        return segment.getArray(key);
    }

    public boolean delete(MappedByteArrayStorageKey key) throws IOException {
        ensureBufferCapacity(getSegmentCount());
        checkKeySegment(key);

        int segmentNumber = key.getSegmentNumber();
        MappedByteArrayStorageSegment segment = MappedByteArrayStorageSegment.read(
                buffer,
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
                    buffer,
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
                buffer,
                segmentNumber,
                getSegmentOffset(segmentNumber),
                SegmentSize);
        buffer.putInt(SegmentCountOffset, segmentNumber + 1);
        return segment;
    }

    private void ensureBufferCapacity(int segmentCount) throws IOException {
        long requiredFileSize = getRequiredSize(segmentCount);
        if (requiredFileSize > MaxFileSize) {
            throw new IOException("The MappedByteArrayStorage cannot be bigger than " + MaxFileSize + " bytes.");
        }
        if (requiredFileSize > buffer.capacity()) {
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, requiredFileSize);
        }
    }

    private void checkKeySegment(MappedByteArrayStorageKey key) {
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
        return buffer.getInt(SegmentCountOffset);
    }

    private long getNextRecordId() {
        // Even if we would generate 1000000000 id values per second,
        // it would take 584 years to reach a collision.
        long recordId = buffer.getLong(NextRecordIdOffset);
        buffer.putLong(NextRecordIdOffset, recordId + 1);
        return recordId;
    }
}
