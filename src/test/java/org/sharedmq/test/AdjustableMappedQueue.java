package org.sharedmq.test;

import org.sharedmq.MappedQueueService;
import org.sharedmq.MappedQueue;

import java.io.File;
import java.io.IOException;

/**
 * A wrapper around {@link MappedQueue} that allows to change queue time.
 */
public class AdjustableMappedQueue extends MappedQueue {

    private final Object timeShiftMonitor = new Object();

    private long timeShift;

    public AdjustableMappedQueue(
            File rootFolder,
            long visibilityTimeout,
            long retentionPeriod
    ) throws IOException, InterruptedException {
        super(rootFolder, visibilityTimeout, retentionPeriod);
    }

    public AdjustableMappedQueue(File rootFolder) throws IOException, InterruptedException {
        super(rootFolder);
    }

    @Override
    protected long getTime() {
        return super.getTime() + getTimeShift();
    }

    public long getTimeShift() {
        // synchronization is required here to avoid the read of stale values by other threads
        synchronized (timeShiftMonitor) {
            return timeShift;
        }
    }

    public void setTimeShift(long shift) {
        // synchronization is required here to avoid the read of stale values by other threads
        synchronized (timeShiftMonitor) {
            timeShift = shift;
        }
    }
}
