package org.sharedmq.primitives;

import org.sharedmq.util.IOUtils;

import java.io.IOException;

public class ProtectedFile implements DataFile {

    private final RollbackJournal journal;
    private final int fileId;
    private final MemoryMappedFile mappedFile;

    private byte[] shortBuffer = new byte[1024];

    public ProtectedFile(RollbackJournal journal, int fileId, MemoryMappedFile mappedFile) {
        this.journal = journal;
        this.fileId = fileId;
        this.mappedFile = mappedFile;
    }

    public MemoryMappedFile getUnprotected() {
        return mappedFile;
    }

    @Override
    public long length() throws IOException {
        return mappedFile.length();
    }

    @Override
    public int capacity() {
        return mappedFile.capacity();
    }

    @Override
    public void ensureCapacity(int capacity) throws IOException {
        mappedFile.ensureCapacity(capacity);
    }

    @Override
    public int getInt(int offset) {
        return mappedFile.getInt(offset);
    }

    @Override
    public long getLong(int offset) {
        return mappedFile.getLong(offset);
    }

    @Override
    public void readBytes(int fileOffset, byte[] buffer, int bufferOffset, int dataLength) {
        mappedFile.readBytes(fileOffset, buffer, bufferOffset, dataLength);
    }

    @Override
    public <TRecord> TRecord get(int offset, StorageAdapter<TRecord> adapter) throws IOException {
        return mappedFile.get(offset, adapter);
    }

    @Override
    public void putInt(int offset, int value) throws IOException {
        mappedFile.readBytes(offset, shortBuffer, 0, 4);
        journal.saveData(fileId, shortBuffer, offset, 4);
        mappedFile.putInt(offset, value);
    }

    @Override
    public void putLong(int offset, long value) throws IOException {
        mappedFile.readBytes(offset, shortBuffer, 0, 8);
        journal.saveData(fileId, shortBuffer, offset, 8);
        mappedFile.putLong(offset, value);
    }

    @Override
    public void writeBytes(int offset, byte[] buffer, int bufferOffset, int dataLength) throws IOException {
        byte[] tempBuffer = dataLength <= shortBuffer.length ? shortBuffer : new byte[dataLength];
        mappedFile.readBytes(offset, tempBuffer, 0, dataLength);
        journal.saveData(fileId, tempBuffer, offset, dataLength);
        mappedFile.writeBytes(offset, buffer, bufferOffset, dataLength);
    }

    @Override
    public <TRecord> void put(int offset, TRecord value, StorageAdapter<TRecord> adapter) throws IOException {
        int dataLength = adapter.getRecordSize();
        byte[] tempBuffer = dataLength <= shortBuffer.length ? shortBuffer : new byte[dataLength];
        mappedFile.readBytes(offset, tempBuffer, 0, dataLength);
        journal.saveData(fileId, tempBuffer, offset, dataLength);
        mappedFile.put(offset, value, adapter);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(mappedFile);
    }
}
