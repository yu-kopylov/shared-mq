package org.sharedmq;

import org.sharedmq.internals.*;
import org.sharedmq.primitives.*;
import org.sharedmq.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * This class serves one queue defined by a queue URL within {@link MappedQueueService}.
 */
public class MappedQueue implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(MappedQueue.class);

    private static final Charset encoding = StandardCharsets.UTF_8;

    private static final String ConfigFilename = "config.dat";
    private static final String MessageHeadersFilename = "headers.dat";
    private static final String FreeHeadersFilename = "free-headers.dat";
    private static final String PriorityQueueFilename = "priority-queue.dat";
    private static final String MessageContentsFilename = "content.dat";

    private final String queueUrl;
    private final File rootFolder;

    /**
     * This is a configuration file.<br/>
     * Is stores queue parameters, and is also used for locks.
     */
    private MappedQueueConfigFile config;

    /**
     * This file contains message parameters and references to the priority queue and content storage.
     */
    private MappedArrayList<MappedQueueMessageHeader> headers;

    /**
     * This file contains a list of free entries from the headers file.
     */
    private MappedArrayList<Integer> freeHeaders;

    /**
     * This is a priority queue.<br/>
     * For each message it stores a visibleSince value and a reference to the headers file.
     */
    private MappedHeap<MappedQueueHeapRecord> priorityQueue;

    /**
     * This file stores message bodies.
     */
    private MappedByteArrayStorage messageContents;

    /**
     * Creates an instance of a {@link MappedQueue}.<br/>
     * Creates all necessary files and folders for a queue in the given folder,
     * including the root folder, if they do not exist.
     *
     * @param rootFolder        The root folder for the queue.
     * @param queueUrl          The URL of the queue.
     * @param visibilityTimeout The amount of time (in milliseconds) that a message received from a queue
     *                          will be invisible to other receiving components.
     *                          Value must be between 0 seconds and 12 hours.
     * @param retentionPeriod   The amount of time (in milliseconds) that the queue will retain a message
     *                          if it does not get deleted.
     *                          Value must be between 1 minute and 14 days.
     * @throws IOException          If the queue cannot be created in the given folder;
     *                              or if folder already has a queue with different parameters.
     * @throws InterruptedException If the current operation was interrupted.
     */
    public MappedQueue(
            File rootFolder,
            String queueUrl,
            long visibilityTimeout,
            long retentionPeriod
    ) throws IOException, InterruptedException {

        this.rootFolder = rootFolder.getAbsoluteFile();
        this.queueUrl = queueUrl;

        FileUtils.createFolder(rootFolder);

        try {
            config = new MappedQueueConfigFile(new File(rootFolder, ConfigFilename), visibilityTimeout, retentionPeriod);

            try (MappedByteBufferLock lock = config.acquireLock()) {

                headers = new MappedArrayList<>(
                        new File(rootFolder, MessageHeadersFilename),
                        MappedQueueMessageHeaderStorageAdapter.getInstance());

                freeHeaders = new MappedArrayList<>(
                        new File(rootFolder, FreeHeadersFilename),
                        IntegerStorageAdapter.getInstance());

                priorityQueue = new MappedHeap<MappedQueueHeapRecord>(
                        new File(rootFolder, PriorityQueueFilename),
                        MappedQueueHeapRecordStorageAdapter.getInstance(),
                        MappedQueueHeapRecord::compareVisibility);

                messageContents = new MappedByteArrayStorage(new File(rootFolder, MessageContentsFilename));
            }

            priorityQueue.register(this::updateHeapIndex);
        } catch (Throwable e) {
            FileUtils.closeOnError(e, config, headers, freeHeaders, priorityQueue, messageContents);
            throw e;
        }
    }

    /**
     * Creates an instance of a {@link MappedQueue}.<be/>
     * Expects that a queue already exist in that folder.<br/>
     * Reads the configuration from the existing queue.
     *
     * @param rootFolder The root folder for the queue.
     * @param queueUrl   The URL of the queue.
     * @throws IOException          If the queue does not exist in the given folder.
     * @throws InterruptedException If the current operation was interrupted.
     */
    public MappedQueue(
            File rootFolder,
            String queueUrl
    ) throws IOException, InterruptedException {

        this.rootFolder = rootFolder.getAbsoluteFile();
        this.queueUrl = queueUrl;

        try {
            config = new MappedQueueConfigFile(new File(rootFolder, ConfigFilename));

            try (MappedByteBufferLock lock = config.acquireLock()) {

                headers = new MappedArrayList<>(
                        new File(rootFolder, MessageHeadersFilename),
                        MappedQueueMessageHeaderStorageAdapter.getInstance());

                freeHeaders = new MappedArrayList<>(
                        new File(rootFolder, FreeHeadersFilename),
                        IntegerStorageAdapter.getInstance());

                priorityQueue = new MappedHeap<>(
                        new File(rootFolder, PriorityQueueFilename),
                        MappedQueueHeapRecordStorageAdapter.getInstance(),
                        MappedQueueHeapRecord::compareVisibility);

                messageContents = new MappedByteArrayStorage(new File(rootFolder, MessageContentsFilename));
            }

            priorityQueue.register(this::updateHeapIndex);
        } catch (Throwable e) {
            FileUtils.closeOnError(e, config, headers, freeHeaders, priorityQueue, messageContents);
            throw e;
        }
    }


    @Override
    public void close() throws IOException {
        FileUtils.close(config, headers, freeHeaders, messageContents, priorityQueue);
    }

    /**
     * Pushes a new message to the queue.
     *
     * @param delay The amount of time in milliseconds to delay the first delivery of this message.
     * @param body  The body of the message.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the current operation was interrupted.
     */
    public void push(long delay, String body) throws IOException, InterruptedException {

        try (MappedByteBufferLock lock = config.acquireLock()) {

            long now = getTime();

            long messageId = config.getNextMessageId();

            int messageNumber;
            if (freeHeaders.size() > 0) {
                messageNumber = freeHeaders.removeLast();
            } else {
                messageNumber = headers.size();
            }

            MappedQueueMessageHeader header = new MappedQueueMessageHeader(messageId, messageNumber);

            header.setSentTime(now);
            header.setDelay(delay);
            header.setReceivedTime(null);

            long visibleSince = getVisibleSince(header);

            MappedQueueHeapRecord heapRecord = new MappedQueueHeapRecord(messageNumber, visibleSince);
            header.setHeapIndex(priorityQueue.add(heapRecord));

            header.setBodyKey(messageContents.add(body.getBytes(encoding)));

            if (messageNumber >= headers.size()) {
                headers.add(header);
            } else {
                headers.set(messageNumber, header);
            }
        }
    }

    public Message pull(long timeout) throws IOException, InterruptedException {

        long start = getTime();

        MappedQueueMessage message = pollMessage();

        long remainingTimeout = getRemainingTimeout(start, timeout);
        while (message == null && remainingTimeout > 0) {
            long timeTillNextMessage = getTimeTillNextMessage();
            long waitTime = Math.min(remainingTimeout, timeTillNextMessage);
            waitForMessage(waitTime);
            message = pollMessage();
            remainingTimeout = getRemainingTimeout(start, timeout);
        }

        return message;
    }

    private void waitForMessage(long timeout) throws InterruptedException {
        if (timeout <= 0) {
            return;
        }
        // For now, we are simply waiting for a small fixed amount of time.
        Thread.sleep(Math.min(timeout, 50));
    }

    /**
     * Returns the remaining timeout based on the start time and the initial timeout.<br/>
     * Returned value can be negative.
     *
     * @param start   the start time
     * @param timeout the initial timeout
     */
    private long getRemainingTimeout(long start, long timeout) {

        long now = getTime();
        long spentTime = now - start;
        if (spentTime > Integer.MAX_VALUE) {
            return 0;
        }
        if (spentTime < 0) {
            // If time was shifted backwards, we do not know how much time is left.
            // So we assume that the timeout already passed.
            return 0;
        }

        return timeout - (int) spentTime;
    }

    /**
     * Returns the amount of time to wait for the next message in the queue to become available.<br/>
     * If there is no next message, then {@link Long#MAX_VALUE} is returned.
     */
    public long getTimeTillNextMessage() throws IOException, InterruptedException {
        // We are checking, when the next message will become visible.
        // We do not want to miss that moment.

        MappedQueueHeapRecord nextRecord;
        try (MappedByteBufferLock lock = config.acquireLock()) {
            nextRecord = priorityQueue.peek();
        }
        if (nextRecord == null) {
            return Long.MAX_VALUE;
        }

        long now = getTime();

        // one millisecond is added to be sure,
        // that we would wake up after message becomes available
        return nextRecord.getVisibleSince() - now + 1;
    }

    private MappedQueueMessage pollMessage() throws IOException, InterruptedException {
        try (MappedByteBufferLock lock = config.acquireLock()) {

            long now = getTime();

            MappedQueueMessageHeader header = peekNextMessageHeader();

            while (header != null && isExpired(header, now)) {
                deleteMessage(header);
                header = peekNextMessageHeader();
            }

            if (header == null) {
                return null;
            }

            if (getVisibleSince(header) > now) {
                // This message has the minimum visibleSince value.
                // That means that there are no other messages that would match visibility requirement.
                return null;
            }

            int messageNumber = header.getMessageNumber();

            header.setReceivedTime(now);
            long visibleSince = getVisibleSince(header);

            priorityQueue.removeAt(header.getHeapIndex());
            MappedQueueHeapRecord heapRecord = new MappedQueueHeapRecord(messageNumber, visibleSince);
            header.setHeapIndex(priorityQueue.add(heapRecord));

            headers.set(messageNumber, header);

            byte[] bodyBytes = messageContents.get(header.getBodyKey());
            String body = new String(bodyBytes, encoding);

            return new MappedQueueMessage(queueUrl, header, body);
        }
    }

    private MappedQueueMessageHeader peekNextMessageHeader() throws IOException, InterruptedException {
        MappedQueueHeapRecord firstHeapRecord = priorityQueue.peek();
        return firstHeapRecord == null ? null : headers.get(firstHeapRecord.getMessageNumber());
    }

    public void delete(Message message) throws IOException, InterruptedException {
        if (!(message instanceof MappedQueueMessage)) {
            throw new IllegalArgumentException("This message was not received with MappedQueueService.");
        }

        MappedQueueMessage queueMessage = (MappedQueueMessage) message;

        if (!queueUrl.equals(queueMessage.getQueueUrl())) {
            throw new IllegalArgumentException(
                    "This message was not received from this queue (" +
                            "Queue URL: '" + queueUrl + "', " +
                            "Message URL: '" + queueMessage.getQueueUrl() + "').");
        }

        try (MappedByteBufferLock lock = config.acquireLock()) {

            int messageNumber = queueMessage.getHeader().getMessageNumber();
            if (messageNumber >= headers.size()) {
                // currently this should be impossible, because the headers list is never truncated
                throw new IllegalStateException("Invalid message number (" + messageNumber + ").");
            }
            MappedQueueMessageHeader currentHeader = headers.get(messageNumber);
            if (currentHeader == null || currentHeader.getMessageId() != queueMessage.getHeader().getMessageId()) {
                // message was already deleted
                return;
            }
            deleteMessage(currentHeader);
        }
    }

    /**
     * Returns the current time in milliseconds.
     */
    protected long getTime() {
        return System.currentTimeMillis();
    }

    private void updateHeapIndex(MappedQueueHeapRecord heapRecord, int index) throws IOException {
        int messageNumber = heapRecord.getMessageNumber();
        MappedQueueMessageHeader header = headers.get(messageNumber);
        header.setHeapIndex(index);
        headers.set(messageNumber, header);
    }

    private void deleteMessage(MappedQueueMessageHeader header) throws IOException {
        int messageNumber = header.getMessageNumber();
        messageContents.delete(header.getBodyKey());
        priorityQueue.removeAt(header.getHeapIndex());
        headers.set(messageNumber, null);
        freeHeaders.add(messageNumber);
    }

    private boolean isExpired(MappedQueueMessageHeader header, long now) {
        return now >= header.getSentTime() + config.getRetentionPeriod();
    }

    private long getVisibleSince(MappedQueueMessageHeader header) {
        Long receivedTime = header.getReceivedTime();
        if (receivedTime != null) {
            // No overflow here, because visibilityTimeout is limited by 12 hours.
            return receivedTime + config.getVisibilityTimeout();
        }
        // No overflow here, because delay is limited by 900 seconds.
        return header.getSentTime() + header.getDelay();
    }
}
