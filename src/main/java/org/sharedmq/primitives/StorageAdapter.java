package org.sharedmq.primitives;

import java.nio.ByteBuffer;

/**
 * An adapter that can store records of the given type in a ByteBuffer.
 */
public interface StorageAdapter<TRecord> {
    /**
     * Returns the size of one record in bytes.
     */
    int getRecordSize();

    /**
     * Writes the given record to the buffer at current buffer position.
     *
     * @param buffer The buffer to write the record to.
     * @param record The record to store.
     */
    void store(ByteBuffer buffer, TRecord record);

    /**
     * Reads a record from the buffer at current buffer position.
     *
     * @param buffer The buffer to read from.
     * @return The record at the given buffer position.
     */
    TRecord load(ByteBuffer buffer);
}
