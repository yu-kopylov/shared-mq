package org.sharedmq.primitives;

import org.sharedmq.util.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * A rollback journal for memory-mapped files.
 */
public class RollbackJournal implements Closeable {

    private static final long MaxFileSize = Integer.MAX_VALUE;

    private static final int FileMarker = 0x52424A4E;

    private static final int FileMarkerOffset = 0;
    private static final int JournalSizeOffset = FileMarkerOffset + 4;

    private static final int HeaderSize = JournalSizeOffset + 4;
    private static final int RecordHeaderSize = 3 * 4;

    private HashMap<Integer, ProtectedFile> protectedFiles = new HashMap<>();

    private final MemoryMappedFile mappedFile;

    public RollbackJournal(File file) throws IOException {
        if (file.exists()) {
            mappedFile = openJournal(file);
        } else {
            mappedFile = createJournal(file);
        }
    }

    private MemoryMappedFile createJournal(File file) throws IOException {
        MemoryMappedFile mappedFile = new MemoryMappedFile(file);

        try {
            mappedFile.ensureCapacity(HeaderSize);
            mappedFile.putInt(FileMarkerOffset, FileMarker);
            mappedFile.putInt(JournalSizeOffset, 0);
        } catch (Throwable e) {
            IOUtils.closeOnError(e, mappedFile);
            throw e;
        }

        return mappedFile;
    }

    private MemoryMappedFile openJournal(File file) throws IOException {
        long fileSize = file.length();
        if (fileSize > MaxFileSize) {
            throw new IOException("The file is too big to be a RollbackJournal file.");
        }
        if (fileSize < HeaderSize) {
            throw new IOException("The file is too short to be a RollbackJournal file.");
        }

        MemoryMappedFile mappedFile = new MemoryMappedFile(file);

        try {
            mappedFile.ensureCapacity((int) fileSize);
            int fileMarker = mappedFile.getInt(FileMarkerOffset);
            if (fileMarker != FileMarker) {
                throw new IOException("The file does not contain the RollbackJournal file marker.");
            }
            int journalSize = mappedFile.getInt(JournalSizeOffset);
            if (journalSize + HeaderSize > fileSize) {
                throw new IOException("The rollback journal has an invalid journal size.");
            }
        } catch (Throwable e) {
            IOUtils.closeOnError(e, mappedFile);
            throw e;
        }

        return mappedFile;
    }

    public ProtectedFile openFile(int fileId, File file) throws IOException {
        if (protectedFiles.containsKey(fileId)) {
            throw new IOException("The file with id " + fileId + " already registered within the RollbackJournal.");
        }
        MemoryMappedFile mappedFile = new MemoryMappedFile(file);
        ProtectedFile protectedFile = new ProtectedFile(this, fileId, mappedFile);
        protectedFiles.put(fileId, protectedFile);
        return protectedFile;
    }

    public void commit() {
        mappedFile.putInt(JournalSizeOffset, 0);
    }

    public void rollback() throws IOException {
        int journalSize = mappedFile.getInt(JournalSizeOffset);
        while (journalSize > 0) {
            int recordHeaderOffset = HeaderSize + journalSize - RecordHeaderSize;
            int fileId = mappedFile.getInt(recordHeaderOffset);
            int dataOffset = mappedFile.getInt(recordHeaderOffset + 4);
            int dataLength = mappedFile.getInt(recordHeaderOffset + 8);

            int journalDataOffset = recordHeaderOffset - dataLength;
            if (journalDataOffset < HeaderSize) {
                throw new IOException("The rollback journal has an invalid record.");
            }

            byte[] buffer = new byte[dataLength];
            mappedFile.readBytes(journalDataOffset, buffer, 0, dataLength);

            ProtectedFile protectedFile = protectedFiles.get(fileId);
            protectedFile.getUnprotected().ensureCapacity(dataOffset + dataLength);
            protectedFile.getUnprotected().writeBytes(dataOffset, buffer, 0, dataLength);

            journalSize = journalDataOffset - HeaderSize;
        }
        mappedFile.putInt(JournalSizeOffset, 0);
    }

    public void saveData(int fileId, byte[] data, int dataOffset, int dataLength) throws IOException {

        int journalSize = mappedFile.getInt(JournalSizeOffset);

        int journalDataOffset = HeaderSize + journalSize;
        int recordHeaderOffset = journalDataOffset + dataLength;

        journalSize += dataLength + RecordHeaderSize;

        mappedFile.ensureCapacity(HeaderSize + journalSize);

        mappedFile.writeBytes(journalDataOffset, data, 0, dataLength);
        mappedFile.putInt(recordHeaderOffset, fileId);
        mappedFile.putInt(recordHeaderOffset + 4, dataOffset);
        mappedFile.putInt(recordHeaderOffset + 8, dataLength);
        mappedFile.putInt(JournalSizeOffset, journalSize);
    }

    @Override
    public void close() throws IOException {
        List<AutoCloseable> resources = new ArrayList<>();
        resources.addAll(protectedFiles.values());
        resources.add(mappedFile);
        IOUtils.close(resources);
    }
}
