package org.sharedmq.internals;

import org.sharedmq.primitives.MappedByteArrayStorageKey;

/**
 * This class contains message properties and references.
 */
public class MessageHeader {
    private final long messageId;
    private final int messageNumber;

    private long sentTime;
    private long delay;
    private Long receivedTime;

    private int heapIndex;
    private MappedByteArrayStorageKey bodyKey;

    public MessageHeader(long messageId, int messageNumber) {
        this.messageId = messageId;
        this.messageNumber = messageNumber;
    }

    public long getMessageId() {
        return messageId;
    }

    public int getMessageNumber() {
        return messageNumber;
    }

    public long getSentTime() {
        return sentTime;
    }

    public void setSentTime(long sentTime) {
        this.sentTime = sentTime;
    }

    public long getDelay() {
        return delay;
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    public Long getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(Long receivedTime) {
        this.receivedTime = receivedTime;
    }

    public int getHeapIndex() {
        return heapIndex;
    }

    public void setHeapIndex(int heapIndex) {
        this.heapIndex = heapIndex;
    }

    public MappedByteArrayStorageKey getBodyKey() {
        return bodyKey;
    }

    public void setBodyKey(MappedByteArrayStorageKey bodyKey) {
        this.bodyKey = bodyKey;
    }
}
