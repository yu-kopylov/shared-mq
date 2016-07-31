package org.sharedmq.internals;

import org.sharedmq.SharedMessageQueue;
import org.sharedmq.primitives.MappedByteBufferLock;
import org.sharedmq.util.IOUtils;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Represents a configuration file for the {@link SharedMessageQueue}.<br/>
 * It also serves as a lock container.<br/>
 * <br/>
 * This class is not thread-safe.<br/>
 * The {@link #acquireLock()} method can be used to synchronize access between threads and processes.
 */
public class ConfigurationFile implements Closeable {

    private static final int FileMarker = 0x4D514346;
    private static final int FormatVersion = 1;

    private static final int FileMarkerOffset = 0;
    private static final int FormatVersionOffset = FileMarkerOffset + 4;
    private static final int LockOffsetOffset = FormatVersionOffset + 4;
    private static final int VisibilityTimeoutOffset = LockOffsetOffset + MappedByteBufferLock.LockSize;
    private static final int RetentionPeriodOffset = VisibilityTimeoutOffset + 8;
    private static final int NextMessageIdOffset = RetentionPeriodOffset + 8;

    private static final int FileSize = NextMessageIdOffset + 8;

    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private MappedByteBuffer buffer;

    private ConfigurationFile(RandomAccessFile randomAccessFile, FileChannel fileChannel, MappedByteBuffer buffer) {
        this.randomAccessFile = randomAccessFile;
        this.fileChannel = fileChannel;
        this.buffer = buffer;
    }

    /**
     * Creates or opens a configuration file with the given name.
     * Method fails if the configuration file with different queue parameters already exists.
     *
     * @param file          The location of the configuration file.
     * @param configuration The queue parameters.
     * @return A new instance of {@link ConfigurationFile}.
     * @throws IOException          If an I/O error occur.
     * @throws InterruptedException If current operation was interrupted.
     */
    public static ConfigurationFile create(File file, Configuration configuration) throws IOException, InterruptedException {

        ConfigurationFile configurationFile = null;
        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        MappedByteBuffer buffer;

        try {

            // The createNewFile is an atomic operation. Only one thread can create new file.
            // All other threads will attempt to read existing file.
            if (!file.createNewFile()) {
                configurationFile = open(file);
                try (MappedByteBufferLock lock = configurationFile.acquireLock()) {
                    Configuration existingConfiguration = configurationFile.getConfiguration();
                    if (!existingConfiguration.equals(configuration)) {
                        throw new IOException("A queue configuration file already exists and have different parameters.");
                    }
                }
                return configurationFile;
            }

            randomAccessFile = new RandomAccessFile(file, "rw");
            fileChannel = randomAccessFile.getChannel();
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);

            configurationFile = new ConfigurationFile(randomAccessFile, fileChannel, buffer);

            try (MappedByteBufferLock lock = configurationFile.acquireLock()) {
                buffer.putInt(FileMarkerOffset, FileMarker);
                buffer.putInt(FormatVersionOffset, FormatVersion);
                buffer.putLong(VisibilityTimeoutOffset, configuration.getVisibilityTimeout());
                buffer.putLong(RetentionPeriodOffset, configuration.getRetentionPeriod());
                buffer.putLong(NextMessageIdOffset, 0);
            }

            // Flushing changes to disk, so that open method can check file structure with RandomAccessFile only.
            buffer.force();

        } catch (Throwable e) {
            buffer = null;
            IOUtils.closeOnError(e, configurationFile, fileChannel, randomAccessFile);
            throw e;
        }

        return configurationFile;
    }

    /**
     * Opens an existing configuration file.
     *
     * @param file The location of the file.
     * @return A new instance of {@link ConfigurationFile}.
     * @throws IOException          If an I/O error occur.
     * @throws InterruptedException If current operation was interrupted.
     */
    public static ConfigurationFile open(File file) throws IOException, InterruptedException {

        if (!file.exists()) {
            throw new IOException("Configuration file '" + file.getAbsolutePath() + "' does not exist.");
        }

        RandomAccessFile randomAccessFile = null;
        FileChannel fileChannel = null;
        MappedByteBuffer buffer;

        try {
            randomAccessFile = new RandomAccessFile(file, "rw");

            // check the marker first, to avoid corruption of unrelated file
            if (!checkMarker(randomAccessFile)) {
                // Maybe other thread did not finish creation of the file.
                Thread.sleep(250);
                // Reopen file to avoid reading cached data.
                randomAccessFile.close();
                randomAccessFile = new RandomAccessFile(file, "rw");
            }
            if (!checkMarker(randomAccessFile)) {
                throw new IOException("The file '" + file.getAbsolutePath() + "'" +
                        " is not a configuration file or has different format version.");
            }

            fileChannel = randomAccessFile.getChannel();
            buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, FileSize);

        } catch (Throwable e) {
            buffer = null;
            IOUtils.closeOnError(e, fileChannel, randomAccessFile);
            throw e;
        }

        return new ConfigurationFile(randomAccessFile, fileChannel, buffer);
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        IOUtils.close(fileChannel, randomAccessFile);
    }

    public MappedByteBufferLock acquireLock() throws InterruptedException {
        return new MappedByteBufferLock(buffer, LockOffsetOffset);
    }

    /**
     * Returns the next message id and increments internal counter.
     *
     * @return The next message id.
     */
    public long getNextMessageId() {
        long id = buffer.getLong(NextMessageIdOffset);
        buffer.putLong(NextMessageIdOffset, id + 1);
        return id;
    }

    /**
     * Returns queue configuration parameters.
     */
    public Configuration getConfiguration() {
        long visibilityTimeout = buffer.getLong(VisibilityTimeoutOffset);
        long retentionPeriod = buffer.getLong(RetentionPeriodOffset);
        return new Configuration(visibilityTimeout, retentionPeriod);
    }

    private static boolean checkMarker(RandomAccessFile randomAccessFile) throws IOException {
        if (randomAccessFile.length() < FileSize) {
            return false;
        }
        randomAccessFile.seek(FileMarkerOffset);
        int fileMarker = randomAccessFile.readInt();
        int formatVersion = randomAccessFile.readInt();
        return fileMarker == FileMarker && formatVersion == FormatVersion;
    }
}
