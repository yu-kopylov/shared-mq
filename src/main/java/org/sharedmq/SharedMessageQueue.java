package org.sharedmq;

import org.sharedmq.internals.*;
import org.sharedmq.primitives.*;
import org.sharedmq.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A message queue based on memory-mapped files.<br/>
 * <br/>
 * This class is thread-safe.<br/>
 * The same message queue on disk can be safely accessed from different processes.<br/>
 * The {@link QueueTester} utility can be used to test the inter-process safety.
 */
public class SharedMessageQueue implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(SharedMessageQueue.class);

    private static final Charset encoding = StandardCharsets.UTF_8;

    private static final String ConfigFilename = "config.dat";
    private static final String RollbackJournalFilename = "rollback.dat";
    private static final String MessageHeadersFilename = "headers.dat";
    private static final String FreeHeadersFilename = "free-headers.dat";
    static final String PriorityQueueFilename = "priority-queue.dat";
    private static final String MessageContentsFilename = "content.dat";

    private static final int MessageHeadersFileId = 10;
    private static final int FreeHeadersFileId = 20;
    private static final int PriorityQueueFileId = 30;
    private static final int MessageContentsFileId = 40;

    /**
     * The number of messages deleted within one lock.<br/>
     * It should be small enough for cleanup iteration not to exceed the maximum lock duration.<br/>
     * It does not have to be too big, because normally only one message is processed within lock.
     */
    static final int CleanupBatchSize = 100;

    private final File rootFolder;

    /**
     * This field stores parameters of the queue.
     */
    private Configuration config;

    /**
     * This is a configuration file.<br/>
     * Is stores queue parameters, and also is used for locks.
     */
    private ConfigurationFile configFile;

    /**
     * This is a rolback journal for the queue.
     */
    private RollbackJournal rollbackJournal;

    /**
     * This file contains message parameters and references to the priority queue and content storage.
     */
    private MappedArrayList<MessageHeader> headers;

    /**
     * This file contains a list of free entries from the headers file.
     */
    private MappedArrayList<Integer> freeHeaders;

    /**
     * This is a priority queue.<br/>
     * For each message it stores a visibleSince value and a reference to the headers file.
     */
    private MappedHeap<PriorityQueueRecord> priorityQueue;

    /**
     * This file stores message bodies.
     */
    private MappedByteArrayStorage messageContents;

    private SharedMessageQueue(File rootFolder, ConfigurationFile configFile) throws IOException {

        this.rootFolder = rootFolder;
        this.configFile = configFile;

        config = configFile.getConfiguration();

        try {
            rollbackJournal = new RollbackJournal(new File(rootFolder, RollbackJournalFilename));

            ProtectedFile messageHeadersFile = rollbackJournal.openFile(
                    MessageHeadersFileId,
                    new File(rootFolder, MessageHeadersFilename));

            ProtectedFile FreeHeadersFile = rollbackJournal.openFile(
                    FreeHeadersFileId,
                    new File(rootFolder, FreeHeadersFilename));

            ProtectedFile priorityQueueFile = rollbackJournal.openFile(
                    PriorityQueueFileId,
                    new File(rootFolder, PriorityQueueFilename));

            ProtectedFile messageContentsFile = rollbackJournal.openFile(
                    MessageContentsFileId,
                    new File(rootFolder, MessageContentsFilename));

            // This constructor is always called within a lock.
            // So we can assume that the previous operation is either committed or broken.
            rollbackJournal.rollback();

            headers = new MappedArrayList<>(
                    messageHeadersFile,
                    MessageHeaderStorageAdapter.getInstance());

            freeHeaders = new MappedArrayList<>(
                    FreeHeadersFile,
                    IntegerStorageAdapter.getInstance());

            priorityQueue = new MappedHeap<>(
                    priorityQueueFile,
                    PriorityQueueRecordStorageAdapter.getInstance(),
                    PriorityQueueRecord::compareVisibility);

            messageContents = new MappedByteArrayStorage(
                    messageContentsFile);

            rollbackJournal.commit();

            priorityQueue.register(this::updateHeapIndex);
        } catch (Throwable e) {
            IOUtils.closeOnError(e, headers, freeHeaders, priorityQueue, messageContents, rollbackJournal);
            throw e;
        }
    }

    /**
     * Creates or opens a message queue in the given folder.<br/>
     * Method creates the root folder for the queue if it does not exist.<br/>
     * Method fails if there is a queue with different parameters in the given folder.
     *
     * @param rootFolder        The folder where queue should be created.
     * @param visibilityTimeout The amount of time in milliseconds that a message received from a queue
     *                          will be invisible to other receiving components.
     *                          Value must be between 0 seconds and 12 hours.
     * @param retentionPeriod   The amount of time in milliseconds that the queue will retain a message
     *                          if it does not get deleted.
     *                          Value must be between 15 seconds and 14 days.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If the queue cannot be created in the given folder;
     *                                  or if folder already has a queue with different parameters.
     * @throws InterruptedException     If the current operation was interrupted.
     */
    public static SharedMessageQueue createQueue(
            File rootFolder,
            long visibilityTimeout,
            long retentionPeriod
    ) throws IOException, InterruptedException {

        QueueParametersValidator.validateCreateQueue(rootFolder, visibilityTimeout, retentionPeriod);

        rootFolder = rootFolder.getCanonicalFile();

        IOUtils.createFolder(rootFolder);

        Configuration configuration = new Configuration(visibilityTimeout, retentionPeriod);
        ConfigurationFile configFile = ConfigurationFile.create(new File(rootFolder, ConfigFilename), configuration);

        try {
            try (MappedByteBufferLock lock = configFile.acquireLock()) {
                return new SharedMessageQueue(rootFolder, configFile);
            }
        } catch (Throwable e) {
            IOUtils.closeOnError(e, configFile);
            throw e;
        }
    }

    /**
     * Opens an existing message queue in the given folder.
     *
     * @param rootFolder The folder where queue is located.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If the queue does not exist in the given folder.
     * @throws InterruptedException     If the current operation was interrupted.
     */
    public static SharedMessageQueue openQueue(File rootFolder) throws IOException, InterruptedException {

        QueueParametersValidator.validateOpenQueue(rootFolder);

        rootFolder = rootFolder.getCanonicalFile();

        ConfigurationFile configFile = ConfigurationFile.open(new File(rootFolder, ConfigFilename));

        try {
            try (MappedByteBufferLock lock = configFile.acquireLock()) {
                return new SharedMessageQueue(rootFolder, configFile);
            }
        } catch (Throwable e) {
            IOUtils.closeOnError(e, configFile);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(configFile, headers, freeHeaders, messageContents, priorityQueue, rollbackJournal);
    }

    /**
     * Pushes a new message to the queue.
     *
     * @param delay   The amount of time in milliseconds to delay the first delivery of this message.
     *                Value must be between 0 seconds and 15 minutes.
     * @param message The message to push.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If an I/O error occurs.
     * @throws InterruptedException     If the current operation was interrupted.
     */
    public void push(long delay, String message) throws IOException, InterruptedException {

        QueueParametersValidator.validatePush(delay, message);

        final long now = getTime();

        cleanupQueue();

        final byte[] messageBytes = message.getBytes(encoding);

        try (MappedByteBufferLock lock = acquireLock()) {

            long messageId = configFile.getNextMessageId();

            int messageNumber;
            if (freeHeaders.size() > 0) {
                messageNumber = freeHeaders.removeLast();
            } else {
                messageNumber = headers.size();
            }

            MessageHeader header = new MessageHeader(messageId, messageNumber);

            header.setSentTime(now);
            header.setDelay(delay);
            header.setReceivedTime(null);

            long visibleSince = getVisibleSince(header);

            PriorityQueueRecord heapRecord = new PriorityQueueRecord(messageNumber, visibleSince);
            header.setHeapIndex(priorityQueue.add(heapRecord));

            header.setBodyKey(messageContents.add(messageBytes));

            if (messageNumber >= headers.size()) {
                headers.add(header);
            } else {
                headers.set(messageNumber, header);
            }

            commit();
        }
    }

    /**
     * Retrieves a single message from the queue.
     *
     * @param timeout Timeout for this operation in milliseconds.
     *                Value must be between 0 and 20 seconds.
     *                If timeout is equal to zero, then pull operation does not wait for new messages.
     * @return The received message; or null if queue does not have visible messages.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If an I/O error occurs.
     * @throws InterruptedException     If operation was interrupted.
     */
    public Message pull(long timeout) throws IOException, InterruptedException {

        QueueParametersValidator.validatePull(timeout);

        long start = getTime();

        SharedQueueMessage message = pollMessage();

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

    /**
     * Deletes a message, that was received by pull, from the queue.
     *
     * @param message The message to delete.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If an I/O error occurs.
     * @throws InterruptedException     If operation was interrupted.
     */
    public void delete(Message message) throws IOException, InterruptedException {

        cleanupQueue();

        QueueParametersValidator.validateDelete(message);

        SharedQueueMessage queueMessage = (SharedQueueMessage) message;

        if (!rootFolder.equals(queueMessage.getQueueFolder())) {
            throw new IllegalArgumentException(
                    "This message was not received from this queue (" +
                            "Queue Folder: '" + rootFolder + "', " +
                            "Message Folder: '" + queueMessage.getQueueFolder() + "').");
        }

        try (MappedByteBufferLock lock = acquireLock()) {

            int messageNumber = queueMessage.getHeader().getMessageNumber();
            if (messageNumber >= headers.size()) {
                // currently this should be impossible, because the headers list is never truncated
                throw new IllegalStateException("Invalid message number (" + messageNumber + ").");
            }
            MessageHeader currentHeader = headers.get(messageNumber);
            if (currentHeader == null || currentHeader.getMessageId() != queueMessage.getHeader().getMessageId()) {
                // message was already deleted
                return;
            }
            deleteMessage(currentHeader);

            commit();
        }
    }

    /**
     * @return The total number of messages in the queue, including messages that are not yet visible.
     * @throws InterruptedException If current operation was interrupted.
     */
    public int size() throws InterruptedException, IOException {

        cleanupQueue();

        try (MappedByteBufferLock lock = acquireLock()) {
            return priorityQueue.size();
        }
    }

    /**
     * Removes expired messages from the queue.
     */
    private void cleanupQueue() throws InterruptedException, IOException {

        final long start = getTime();

        boolean shouldCleanup = true;
        int deletedMessagesTotal = 0;

        while (shouldCleanup) {

            shouldCleanup = false;
            int deletedMessages = 0;

            try (MappedByteBufferLock lock = acquireLock()) {

                long now = getTime();

                MessageHeader header = peekNextMessageHeader();

                while (header != null && isExpired(header, now)) {
                    deleteMessage(header);
                    deletedMessages++;
                    if (deletedMessages >= CleanupBatchSize) {
                        shouldCleanup = true;
                        break;
                    }
                    header = peekNextMessageHeader();
                }

                deletedMessagesTotal += deletedMessages;

                commit();
            }
        }

        if (deletedMessagesTotal > 0) {
            long timeSpent = getTime() - start;
            String message = "The cleanupQueue method" +
                    " removed " + deletedMessagesTotal + " messages" +
                    " within " + timeSpent + "ms.";
            if (timeSpent < 1000) {
                logger.trace(message);
            } else {
                logger.debug(message);
            }
        }
    }

    private MappedByteBufferLock acquireLock() throws InterruptedException, IOException {
        MappedByteBufferLock lock = configFile.acquireLock();
        // after we acquire the lock, we first rollback the previous operation if it was not committed
        try {
            rollbackJournal.rollback();
        } catch (Throwable e) {
            IOUtils.closeOnError(e, lock);
        }
        return lock;
    }

    void commit() {
        rollbackJournal.commit();
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
    private long getTimeTillNextMessage() throws IOException, InterruptedException {
        // We are checking, when the next message will become visible.
        // We do not want to miss that moment.

        PriorityQueueRecord nextRecord;
        try (MappedByteBufferLock lock = acquireLock()) {
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

    private SharedQueueMessage pollMessage() throws IOException, InterruptedException {

        long now = getTime();

        cleanupQueue();

        try (MappedByteBufferLock lock = acquireLock()) {

            MessageHeader header = peekNextMessageHeader();

            // we assume that the cleanupQueue method removed all expired messages

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
            PriorityQueueRecord heapRecord = new PriorityQueueRecord(messageNumber, visibleSince);
            header.setHeapIndex(priorityQueue.add(heapRecord));

            headers.set(messageNumber, header);

            byte[] bodyBytes = messageContents.get(header.getBodyKey());
            String body = new String(bodyBytes, encoding);

            commit();

            return new SharedQueueMessage(rootFolder, header, body);
        }
    }

    private MessageHeader peekNextMessageHeader() throws IOException, InterruptedException {
        PriorityQueueRecord firstHeapRecord = priorityQueue.peek();
        return firstHeapRecord == null ? null : headers.get(firstHeapRecord.getMessageNumber());
    }

    /**
     * Returns the current time in milliseconds.
     */
    protected long getTime() {
        return System.currentTimeMillis();
    }

    private void updateHeapIndex(PriorityQueueRecord heapRecord, int index) throws IOException {
        int messageNumber = heapRecord.getMessageNumber();
        MessageHeader header = headers.get(messageNumber);
        header.setHeapIndex(index);
        headers.set(messageNumber, header);
    }

    private void deleteMessage(MessageHeader header) throws IOException {
        int messageNumber = header.getMessageNumber();
        messageContents.delete(header.getBodyKey());
        priorityQueue.removeAt(header.getHeapIndex());
        headers.set(messageNumber, null);
        freeHeaders.add(messageNumber);
    }

    private boolean isExpired(MessageHeader header, long now) {
        return now >= header.getSentTime() + config.getRetentionPeriod();
    }

    private long getVisibleSince(MessageHeader header) {
        Long receivedTime = header.getReceivedTime();
        if (receivedTime != null) {
            // No overflow here, because visibilityTimeout is limited by 12 hours.
            return receivedTime + config.getVisibilityTimeout();
        }
        // No overflow here, because delay is limited by 900 seconds.
        return header.getSentTime() + header.getDelay();
    }
}
