package org.sharedmq.primitives;

import java.io.Closeable;
import java.io.IOException;

/**
 * An interface for data files with random access.
 */
public interface DataFile extends Closeable {

    /**
     * @return The size of the file in the filesystem.
     * @throws IOException If an I/O error occurs.
     */
    long length() throws IOException;

    /**
     * @return The capacity of the memory buffer mapped to the file.
     */
    int capacity();

    /**
     * Increases the capacity of the memory buffer mapped to the file if it is lower than the given value.
     *
     * @param capacity The minimum capacity of the memory buffer mapped to the file.
     * @throws IOException If an I/O error occurs.
     */
    void ensureCapacity(int capacity) throws IOException;

    /**
     * Reads a 4-byte integer value stored at the given offset in the file.
     *
     * @param offset The offset within the file.
     */
    int getInt(int offset);

    /**
     * Reads an 8-byte integer value stored at the given offset in the file.
     *
     * @param offset The offset within the file.
     */
    long getLong(int offset);

    /**
     * Reads bytes from the file into the given buffer.
     *
     * @param fileOffset   The offset within the file.
     * @param buffer       The buffer to read bytes into.
     * @param bufferOffset The starting offset within the buffer for read bytes.
     * @param dataLength   The amount of bytes to read.
     */
    void readBytes(int fileOffset, byte[] buffer, int bufferOffset, int dataLength);

    /**
     * Reads an object from the file.
     *
     * @param offset    The offset within the file.
     * @param adapter   The adapter than can convert bytes into an object.
     * @param <TRecord> The type of the object.
     * @return An object that was read from file by the adapter.
     * @throws IOException If an I/O error occurs.
     */
    <TRecord> TRecord get(int offset, StorageAdapter<TRecord> adapter) throws IOException;

    /**
     * Writes a 4-byte integer value to the file at the given offset.
     *
     * @param offset The offset within the file.
     * @param value  The value to write.
     */
    void putInt(int offset, int value) throws IOException;

    /**
     * Writes an 8-byte integer value to the file at the given offset.
     *
     * @param offset The offset within the file.
     * @param value  The value to write.
     */
    void putLong(int offset, long value) throws IOException;

    /**
     * Writes bytes from the given buffer into the file.
     *
     * @param fileOffset   The offset within the file.
     * @param buffer       The buffer with the bytes to write.
     * @param bufferOffset The starting offset within the buffer for the bytes to be written.
     * @param dataLength   The amount of bytes to write.
     */
    void writeBytes(int fileOffset, byte[] buffer, int bufferOffset, int dataLength) throws IOException;

    /**
     * Writes an object to the file.
     *
     * @param offset    The offset within the file.
     * @param value     The object to write.
     * @param adapter   The adapter than can convert the object to bytes.
     * @param <TRecord> The type of the object.
     * @throws IOException If an I/O error occurs.
     */
    <TRecord> void put(int offset, TRecord value, StorageAdapter<TRecord> adapter) throws IOException;
}
