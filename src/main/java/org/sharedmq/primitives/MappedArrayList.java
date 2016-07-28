package org.sharedmq.primitives;

import org.sharedmq.util.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * An array list mapped to a file.<br/>
 * Null is an allowed value, if adapter allows it.<br/>
 * <br/>
 * This class is not thread-safe.
 */
public class MappedArrayList<TRecord> implements Closeable {

    private static final long MaxFileSize = Integer.MAX_VALUE;
    private static final int AllocationUnit = 64 * 1024;

    private static final int FileMarker = 0x4D4D414C;
    private static final int FileMarkerOffset = 0;
    private static final int RecordCountOffset = 4;
    private static final int RecordSizeOffset = 8;
    private static final int HeaderSize = 12;

    private final StorageAdapter<TRecord> adapter;
    private final int recordSize;
    private final MemoryMappedFile mappedFile;

    public MappedArrayList(File file, StorageAdapter<TRecord> adapter) throws IOException {

        this.adapter = adapter;

        recordSize = adapter.getRecordSize();

        if (file.exists()) {
            mappedFile = openFile(file, file.length(), recordSize);
        } else {
            mappedFile = createFile(file, recordSize);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(mappedFile);
    }

    private static MemoryMappedFile createFile(File file, int recordSize) throws IOException {
        MemoryMappedFile mappedFile = new MemoryMappedFile(file, AllocationUnit);
        try {
            mappedFile.putInt(FileMarkerOffset, FileMarker);
            mappedFile.putInt(RecordSizeOffset, recordSize);
            mappedFile.putInt(RecordCountOffset, 0);
        } catch (Throwable e) {
            IOUtils.closeOnError(e, mappedFile);
            throw e;
        }
        return mappedFile;
    }

    private static MemoryMappedFile openFile(File file, long fileSize, int recordSize) throws IOException {

        if (fileSize > MaxFileSize) {
            throw new IOException("The file is too big to be a MappedArrayList file.");
        }
        if (fileSize < HeaderSize) {
            throw new IOException("The file is too short to be a MappedArrayList file.");
        }

        MemoryMappedFile mappedFile = new MemoryMappedFile(file, (int) fileSize);

        try {
            int marker = mappedFile.getInt(FileMarkerOffset);
            if (marker != FileMarker) {
                throw new IOException("The file does not contain a MappedArrayList file marker.");
            }

            int fileRecordSize = mappedFile.getInt(RecordSizeOffset);
            if (recordSize != fileRecordSize) {
                throw new IOException("The MappedArrayList file has a different record size (" + recordSize + ").");
            }

            int recordCount = mappedFile.getInt(RecordCountOffset);
            if (recordCount < 0) {
                throw new IOException("The MappedArrayList file has an invalid record count (" + recordCount + ").");
            }

            long requiredSize = getRequiredSize(recordSize, recordCount);
            if (requiredSize > fileSize) {
                throw new IOException("The MappedArrayList file is incomplete.");
            }
        } catch (Throwable e) {
            IOUtils.closeOnError(e, mappedFile);
            throw e;
        }

        return mappedFile;
    }

    public int size() {
        return mappedFile.getInt(RecordCountOffset);
    }

    public void add(TRecord value) throws IOException {
        int recordCount = size();
        int recordIndex = recordCount;
        recordCount++;

        ensureBufferCapacity(recordCount);

        int recordOffset = getRecordOffset(recordSize, recordIndex);
        mappedFile.put(recordOffset, value, adapter);

        mappedFile.putInt(RecordCountOffset, recordCount);
    }

    public TRecord get(int recordIndex) throws IOException {
        checkRecordIndex(recordIndex);
        // While no data are added, we have to make sure that current
        // buffer is sufficient to access all elements of the array.
        // Array size could have been changed by other array instance.
        ensureBufferCapacity(size());
        int recordOffset = getRecordOffset(recordSize, recordIndex);
        return mappedFile.get(recordOffset, adapter);
    }

    public void set(int recordIndex, TRecord value) throws IOException {
        checkRecordIndex(recordIndex);
        // While no data are added, we have to make sure that current
        // buffer is sufficient to access all elements of the array.
        // Array size could have been changed by other array instance.
        ensureBufferCapacity(size());
        int recordOffset = getRecordOffset(recordSize, recordIndex);
        mappedFile.put(recordOffset, value, adapter);
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

        int recordOffset = getRecordOffset(recordSize, recordIndex);
        TRecord record = mappedFile.get(recordOffset, adapter);

        mappedFile.putInt(RecordCountOffset, recordIndex);
        return record;
    }

    public void clear() {
        mappedFile.putInt(RecordCountOffset, 0);
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

        if (requiredFileSize > mappedFile.capacity()) {
            // division with rounding up
            int unitsCount = (int) ((requiredFileSize + AllocationUnit - 1) / AllocationUnit);
            int fileSize = unitsCount * AllocationUnit;
            mappedFile.ensureCapacity(fileSize);
        }
    }

    private static long getRequiredSize(int recordSize, int recordCount) {
        return HeaderSize + recordSize * recordCount;
    }

    private static int getRecordOffset(int recordSize, int recordIndex) {
        return HeaderSize + recordSize * recordIndex;
    }
}
