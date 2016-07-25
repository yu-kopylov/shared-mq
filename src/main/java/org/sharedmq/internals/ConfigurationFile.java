package org.sharedmq.internals;

import org.sharedmq.SharedMessageQueue;
import org.sharedmq.primitives.MappedByteBufferLock;
import org.sharedmq.util.FileUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Represents a configuration file for the {@link SharedMessageQueue}.<br/>
 * It also serves as a lock container.<br/>
 * <br/>
 * This class is not thread-safe, except for the constructors.<br/>
 * The {@link #acquireLock()} method can be used to synchronize access
 * to memory mapped files between threads and processes.
 */
public class ConfigurationFile implements Closeable {

    private static final int FileMarker = 0x4D514346;
    private static final int FileMarkerOffset = 0;
    private static final int LockOffsetOffset = FileMarkerOffset + 4;
    private static final int VisibilityTimeoutOffset = LockOffsetOffset + MappedByteBufferLock.LockSize;
    private static final int RetentionPeriodOffset = VisibilityTimeoutOffset + 8;
    private static final int NextMessageIdOffset = RetentionPeriodOffset + 8;

    private static final int FileSize = NextMessageIdOffset + 8;

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer buffer;

    public ConfigurationFile(File file) throws IOException, InterruptedException {

        MappedByteBufferLock lock = null;

        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);

            lock = acquireLock();
            int fileMarker = buffer.getInt(FileMarkerOffset);
            if (fileMarker != FileMarker) {
                throw new IOException("The file '" + file.getAbsolutePath() + "' is not a SharedMessageQueue configuration file.");
            }
        } catch (Throwable e) {
            buffer = null;
            FileUtils.closeOnError(e, fileChannel, randomAccessFile);
            throw e;
        } finally {
            FileUtils.close(lock);
        }
    }

    public ConfigurationFile(File file, Configuration configuration) throws IOException, InterruptedException {

        MappedByteBufferLock lock = null;

        try {
            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);

            lock = acquireLock();

            int fileMarker = buffer.getInt(FileMarkerOffset);
            if (fileMarker != FileMarker) {
                // assuming that this is a new file
                buffer.putInt(FileMarkerOffset, FileMarker);
                buffer.putLong(VisibilityTimeoutOffset, configuration.getVisibilityTimeout());
                buffer.putLong(RetentionPeriodOffset, configuration.getRetentionPeriod());
                buffer.putLong(NextMessageIdOffset, 0);
            } else {
                Configuration existingConfiguration = readConfiguration(buffer);
                if (!existingConfiguration.equals(configuration)) {
                    throw new IOException("A queue configuration file already exists and have different parameters.");
                }
            }

        } catch (Throwable e) {
            buffer = null;
            FileUtils.closeOnError(e, fileChannel, randomAccessFile);
            throw e;
        } finally {
            FileUtils.close(lock);
        }
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        FileUtils.close(fileChannel, randomAccessFile);
    }

    public MappedByteBufferLock acquireLock() throws InterruptedException {
        return new MappedByteBufferLock(buffer, LockOffsetOffset);
    }

    //todo: this method requires lock
    public long getNextMessageId() {
        long id = buffer.getLong(NextMessageIdOffset);
        buffer.putLong(NextMessageIdOffset, id + 1);
        return id;
    }

    //todo: this method requires lock
    public Configuration getConfiguration() {
        return readConfiguration(buffer);
    }

    private static Configuration readConfiguration(MappedByteBuffer buffer) {
        long visibilityTimeout = buffer.getLong(VisibilityTimeoutOffset);
        long retentionPeriod = buffer.getLong(RetentionPeriodOffset);
        return new Configuration(visibilityTimeout, retentionPeriod);
    }
}
