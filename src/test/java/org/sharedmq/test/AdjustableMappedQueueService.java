package org.sharedmq.test;

import org.sharedmq.MappedQueueService;
import org.sharedmq.MappedQueue;

import java.io.File;
import java.io.IOException;

/**
 * A wrapper around {@link MappedQueueService} that allows to change queue time.
 */
public class AdjustableMappedQueueService extends MappedQueueService {

    private final Object timeShiftMonitor = new Object();

    private long timeShift;

    public AdjustableMappedQueueService(File testFolder) throws IOException {
        super(testFolder);
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

    @Override
    protected MappedQueue createQueue(
            File rootFolder,
            long visibilityTimeout,
            long retentionPeriod
    ) throws IOException, InterruptedException {
        return new AdjustableMappedQueue(rootFolder, visibilityTimeout, retentionPeriod);
    }

    @Override
    protected MappedQueue createQueue(File rootFolder) throws IOException, InterruptedException {
        return new AdjustableMappedQueue(rootFolder);
    }

    private class AdjustableMappedQueue extends MappedQueue {

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
    }
}
