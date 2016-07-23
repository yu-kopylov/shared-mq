package org.sharedmq.mapped;

import java.nio.ByteBuffer;

/**
 * A storage adapter for the {@link MappedQueueMessageHeader}.
 */
public class MappedQueueMessageHeaderStorageAdapter implements StorageAdapter<MappedQueueMessageHeader> {

    // We assume that such timestamp can never occur (292269055 BC).
    private static final long TimestampNullValue = Long.MIN_VALUE;

    private static final byte NullMarker = 0;
    private static final byte NotNullMarker = 1;

    private static final MappedQueueMessageHeaderStorageAdapter instance = new MappedQueueMessageHeaderStorageAdapter();

    private MappedByteArrayStorageKeyStorageAdapter keyStorageAdapter
            = MappedByteArrayStorageKeyStorageAdapter.getInstance();

    public static StorageAdapter<MappedQueueMessageHeader> getInstance() {
        return instance;
    }

    @Override
    public int getRecordSize() {
        return 1 + 2 * 4 + 4 * 8 + keyStorageAdapter.getRecordSize();
    }

    @Override
    public void store(ByteBuffer buffer, MappedQueueMessageHeader header) {

        if (header == null) {
            buffer.put(NullMarker);
            return;
        }

        buffer.put(NotNullMarker);

        buffer.putLong(header.getMessageId());
        buffer.putInt(header.getMessageNumber());

        buffer.putLong(header.getSentTime());
        buffer.putLong(header.getDelay());
        buffer.putLong(header.getReceivedTime() == null ? TimestampNullValue : header.getReceivedTime());

        buffer.putInt(header.getHeapIndex());
        keyStorageAdapter.store(buffer, header.getBodyKey());
    }

    @Override
    public MappedQueueMessageHeader load(ByteBuffer buffer) {

        byte hasValue = buffer.get();
        if (hasValue == NullMarker) {
            return null;
        }

        long messageId = buffer.getLong();
        int messageNumber = buffer.getInt();

        MappedQueueMessageHeader header = new MappedQueueMessageHeader(messageId, messageNumber);

        header.setSentTime(buffer.getLong());
        header.setDelay(buffer.getLong());
        long receivedTime = buffer.getLong();
        header.setReceivedTime(receivedTime == TimestampNullValue ? null : receivedTime);

        header.setHeapIndex(buffer.getInt());
        header.setBodyKey(keyStorageAdapter.load(buffer));

        return header;
    }
}
