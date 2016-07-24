package org.sharedmq.internals;

import org.sharedmq.SharedMessageQueue;

/**
 * A record within the heap used by {@link SharedMessageQueue}.
 */
public class PriorityQueueRecord {
    private int messageNumber;
    private long visibleSince;

    public PriorityQueueRecord(int messageNumber, long visibleSince) {
        this.messageNumber = messageNumber;
        this.visibleSince = visibleSince;
    }

    public int getMessageNumber() {
        return messageNumber;
    }

    public long getVisibleSince() {
        return visibleSince;
    }

    public static int compareVisibility(PriorityQueueRecord record1, PriorityQueueRecord record2) {
        return Long.compare(record1.getVisibleSince(), record2.getVisibleSince());
    }
}
