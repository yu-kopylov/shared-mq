package org.sharedmq.internals;

import org.sharedmq.MappedQueue;

/**
 * A record within the heap used by {@link MappedQueue}.
 */
public class MappedQueueHeapRecord {
    private int messageNumber;
    private long visibleSince;

    public MappedQueueHeapRecord(int messageNumber, long visibleSince) {
        this.messageNumber = messageNumber;
        this.visibleSince = visibleSince;
    }

    public int getMessageNumber() {
        return messageNumber;
    }

    public long getVisibleSince() {
        return visibleSince;
    }

    public static int compareVisibility(MappedQueueHeapRecord record1, MappedQueueHeapRecord record2) {
        return Long.compare(record1.getVisibleSince(), record2.getVisibleSince());
    }
}
