package org.sharedmq.test;

import org.sharedmq.MappedQueueService;
import org.sharedmq.mapped.MappedQueue;

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
            String queueUrl,
            long visibilityTimeout,
            long retentionPeriod
    ) throws IOException, InterruptedException {
        return new AdjustableMappedQueue(rootFolder, queueUrl, visibilityTimeout, retentionPeriod);
    }

    @Override
    protected MappedQueue createQueue(File rootFolder, String queueUrl) throws IOException, InterruptedException {
        return new AdjustableMappedQueue(rootFolder, queueUrl);
    }

    private class AdjustableMappedQueue extends MappedQueue {

        public AdjustableMappedQueue(
                File rootFolder,
                String queueUrl,
                long visibilityTimeout,
                long retentionPeriod
        ) throws IOException, InterruptedException {
            super(rootFolder, queueUrl, visibilityTimeout, retentionPeriod);
        }

        public AdjustableMappedQueue(
                File rootFolder,
                String queueUrl
        ) throws IOException, InterruptedException {
            super(rootFolder, queueUrl);
        }

        @Override
        protected long getTime() {
            return super.getTime() + getTimeShift();
        }
    }
}
