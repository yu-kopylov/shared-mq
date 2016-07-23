package org.sharedmq.primitives;

import org.sharedmq.primitives.StorageAdapter;
import org.sharedmq.util.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * An array list mapped to a file.<br/>
 * Null is an allowed value, if adapter allows it.<br/>
 * <br/>
 * This class is not thread-safe.
 */
public class MappedArrayList<TRecord> implements Closeable {

    private static final long MaxFileSize = Integer.MAX_VALUE;
    private static final int AllocationUnit = 4 * 1024;

    private static final int FileMarker = 0x4D4D414C;
    private static final int FileMarkerOffset = 0;
    private static final int RecordCountOffset = 4;
    private static final int RecordSizeOffset = 8;
    private static final int HeaderSize = 12;

    private final StorageAdapter<TRecord> adapter;
    private final int recordSize;

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer buffer;

    public MappedArrayList(File file, StorageAdapter<TRecord> adapter) throws IOException {

        this.adapter = adapter;

        try {

            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
            recordSize = adapter.getRecordSize();

            long fileSize = randomAccessFile.getChannel().size();
            boolean newFile = fileSize == 0;
            if (newFile) {
                buffer = createFile(fileChannel, adapter.getRecordSize());
            } else {
                buffer = openFile(fileChannel, fileSize, adapter.getRecordSize());
            }

        } catch (Throwable e) {
            buffer = null;
            FileUtils.closeOnError(e, fileChannel, randomAccessFile);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        FileUtils.close(fileChannel, randomAccessFile);
    }

    private static MappedByteBuffer createFile(FileChannel fileChannel, int recordSize) throws IOException {
        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, AllocationUnit);
        buffer.putInt(FileMarkerOffset, FileMarker);
        buffer.putInt(RecordSizeOffset, recordSize);
        buffer.putInt(RecordCountOffset, 0);
        return buffer;
    }

    private static MappedByteBuffer openFile(FileChannel fileChannel, long fileSize, int recordSize) throws IOException {

        if (fileSize > MaxFileSize) {
            throw new IOException("The file is too big to be a MappedArrayList file.");
        }
        if (fileSize < HeaderSize) {
            throw new IOException("The file is too short to be a MappedArrayList file.");
        }

        MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

        int marker = buffer.getInt(FileMarkerOffset);
        if (marker != FileMarker) {
            throw new IOException("The file does not contain a MappedArrayList file marker.");
        }

        int fileRecordSize = buffer.getInt(RecordSizeOffset);
        if (recordSize != fileRecordSize) {
            throw new IOException("The MappedArrayList file has a different record size (" + recordSize + ").");
        }

        int recordCount = buffer.getInt(RecordCountOffset);
        if (recordCount < 0) {
            throw new IOException("The MappedArrayList file has an invalid record count (" + recordCount + ").");
        }

        long requiredSize = getRequiredSize(recordSize, recordCount);
        if (requiredSize > fileSize) {
            throw new IOException("The MappedArrayList file is incomplete.");
        }

        return buffer;
    }

    public int size() {
        return buffer.getInt(RecordCountOffset);
    }

    public void add(TRecord value) throws IOException {
        int recordCount = size();
        int recordIndex = recordCount;
        recordCount++;

        ensureBufferCapacity(recordCount);

        int recordOffset = getRecordOffset(recordSize, recordIndex);
        buffer.position(recordOffset);
        adapter.store(buffer, value);

        buffer.putInt(RecordCountOffset, recordCount);
    }

    public TRecord get(int recordIndex) throws IOException {
        checkRecordIndex(recordIndex);
        // While no data are added, we have to make sure that current
        // buffer is sufficient to access all elements of the array.
        // Array size could have been changed by other array instance.
        ensureBufferCapacity(size());
        int recordOffset = getRecordOffset(recordSize, recordIndex);
        buffer.position(recordOffset);
        return adapter.load(buffer);
    }

    public void set(int recordIndex, TRecord value) throws IOException {
        checkRecordIndex(recordIndex);
        // While no data are added, we have to make sure that current
        // buffer is sufficient to access all elements of the array.
        // Array size could have been changed by other array instance.
        ensureBufferCapacity(size());
        int recordOffset = getRecordOffset(recordSize, recordIndex);
        buffer.position(recordOffset);
        adapter.store(buffer, value);
    }

    public TRecord removeLast() throws IOException {
        int recordCount = size();
        if (recordCount == 0) {
            throw new IllegalStateException("The MappedArrayList is empty.");
        }

        // While no data are added, we have to make sure that current
        // buffer is sufficient to access all elements of the array.
        // Array size could have been changed by other array instance.
        ensureBufferCapacity(recordCount);

        int recordIndex = recordCount - 1;

        buffer.position(getRecordOffset(recordSize, recordIndex));
        TRecord record = adapter.load(buffer);

        buffer.putInt(RecordCountOffset, recordIndex);
        return record;
    }

    public void clear() {
        buffer.putInt(RecordCountOffset, 0);
    }

    private void checkRecordIndex(int recordIndex) {
        if (recordIndex < 0) {
            throw new IllegalArgumentException("The recordIndex parameter cannot be negative.");
        }
        if (recordIndex >= size()) {
            throw new IllegalArgumentException(
                    "The recordIndex parameter cannot be greater or equal to the number of records in the list.");
        }
    }

    private void ensureBufferCapacity(int recordCount) throws IOException {
        long requiredFileSize = getRequiredSize(recordSize, recordCount);
        if (requiredFileSize > MaxFileSize) {
            throw new IOException("The MappedByteArrayStorage cannot be bigger than " + MaxFileSize + " bytes.");
        }

        if (requiredFileSize > buffer.capacity()) {
            // division with rounding up
            int unitsCount = (int) ((requiredFileSize + AllocationUnit - 1) / AllocationUnit);
            int fileSize = unitsCount * AllocationUnit;
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        }
    }

    private static long getRequiredSize(int recordSize, int recordCount) {
        return HeaderSize + recordSize * recordCount;
    }

    private static int getRecordOffset(int recordSize, int recordIndex) {
        return HeaderSize + recordSize * recordIndex;
    }
}
