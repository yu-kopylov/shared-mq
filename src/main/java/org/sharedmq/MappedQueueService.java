package org.sharedmq;

import org.sharedmq.internals.MappedQueueMessage;
import org.sharedmq.internals.QueueServiceParametersValidator;
import org.sharedmq.util.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A message queue service based on memory-mapped files.<br/>
 * <br/>
 * In this implementation the queue name matches the queue URL.<br/>
 * Queue names are case insensitive,
 * because it should work the same way on Windows and Linux.
 */
public class MappedQueueService implements Closeable {

    //todo: check close

    private final File baseFolder;

    private final Object queuesMonitor = new Object();
    private final Map<String, MappedQueue> queuesByUrl = new HashMap<>();

    public MappedQueueService(File baseFolder) throws IOException {

        this.baseFolder = baseFolder;

        FileUtils.createFolder(baseFolder);
    }

    /**
     * Creates a queue with the given name and configuration.
     *
     * @param queueName         Queue names must be 1-80 characters in length and be composed
     *                          of alphanumeric characters, hyphens (-), and underscores (_).
     * @param visibilityTimeout The amount of time (in seconds) that a message received from a queue
     *                          will be invisible to other receiving components.
     *                          Value must be between 0 seconds and 12 hours.
     * @param retentionPeriod   The amount of time (in seconds) that the queue will retain a message
     *                          if it does not get deleted.
     *                          Value must be between 1 minute and 14 days.
     * @return An URL of a new queue.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If an I/O error occurs.
     */
    public String createQueue(String queueName, int visibilityTimeout, int retentionPeriod) throws IOException, InterruptedException {

        QueueServiceParametersValidator.validateCreateQueue(queueName, visibilityTimeout, retentionPeriod);

        String queueUrl = normalizeQueueName(queueName);

        synchronized (queuesMonitor) {

            MappedQueue queue = queuesByUrl.get(queueUrl);
            File file = new File(baseFolder, queueUrl);
            // Currently, queue deletion is not supported.
            // So, if queuesByUrl contains the queue, then the queue is certainly exist.
            if (queue != null || file.exists()) {
                throw new IOException("A queue with the name '" + queueName + "' already exists.");
            }

            queue = createQueue(file, visibilityTimeout * 1000L, retentionPeriod * 1000L);
            queuesByUrl.put(queueUrl, queue);
        }

        return queueUrl;
    }

    /**
     * Pushes a message onto a queue with the given delay.
     *
     * @param queueUrl The URL of the queue.
     * @param delay    The amount of time in seconds to delay the first delivery of this message.
     *                 Must be greater than or equal to 0, and less than or equal to 900.
     * @param message  The message to push.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If an I/O error occurs.
     * @throws InterruptedException     If operation was interrupted.
     */
    public void push(String queueUrl, int delay, String message) throws InterruptedException, IOException {

        QueueServiceParametersValidator.validatePush(queueUrl, delay, message);
        // In the MappedQueueService the queue URL matches the queue name.
        // So the same restrictions apply to queue URL.
        QueueServiceParametersValidator.validateQueueName(queueUrl);

        MappedQueue queue = getOrCreateQueue(queueUrl);
        queue.push(delay * 1000L, message);
    }

    /**
     * Retrieves a single message from a queue.
     *
     * @param queueUrl The URL of the queue.
     * @param timeout  Timeout for this operation in seconds.
     *                 Value must be between 0 and 20 seconds.
     *                 If timeout is equal to zero, then pull operation does not wait for new messages.
     * @return Received message; or null if there were no available messages.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If an I/O error occurs.
     * @throws InterruptedException     If operation was interrupted.
     */
    public Message pull(String queueUrl, int timeout) throws InterruptedException, IOException {

        QueueServiceParametersValidator.validatePull(queueUrl, timeout);
        // In the MappedQueueService the queue URL matches the queue name.
        // So the same restrictions apply to queue URL.
        QueueServiceParametersValidator.validateQueueName(queueUrl);

        MappedQueue queue = getOrCreateQueue(queueUrl);
        return queue.pull(timeout * 1000L);
    }

    /**
     * Deletes a message, that was received by pull, from the queue.
     *
     * @param queueUrl The URL of the queue.
     * @param message  The message to delete.
     * @throws IllegalArgumentException If parameters are invalid.
     * @throws IOException              If an I/O error occurs.
     * @throws InterruptedException     If operation was interrupted.
     */
    public void delete(String queueUrl, Message message) throws InterruptedException, IOException {

        QueueServiceParametersValidator.validateDelete(queueUrl, message, MappedQueueMessage.class);
        // In the MappedQueueService the queue URL matches the queue name.
        // So the same restrictions apply to queue URL.
        QueueServiceParametersValidator.validateQueueName(queueUrl);

        MappedQueue queue = getOrCreateQueue(queueUrl);
        queue.delete(message);
    }

    private MappedQueue getOrCreateQueue(String queueUrl) throws IOException, InterruptedException {
        queueUrl = normalizeQueueName(queueUrl);
        synchronized (queuesMonitor) {
            MappedQueue queue = queuesByUrl.get(queueUrl);
            if (queue == null) {
                File file = new File(baseFolder, queueUrl);
                queue = createQueue(file);
                queuesByUrl.put(queueUrl, queue);
            }
            return queue;
        }
    }

    protected MappedQueue createQueue(
            File rootFolder,
            long visibilityTimeout,
            long retentionPeriod
    ) throws IOException, InterruptedException {
        return new MappedQueue(rootFolder, visibilityTimeout, retentionPeriod);
    }

    protected MappedQueue createQueue(File rootFolder) throws IOException, InterruptedException {
        return new MappedQueue(rootFolder);
    }

    /**
     * Queue names in the {@link MappedQueueService} are case insensitive,
     * because it should work the same way on Windows and Linux.
     *
     * @param queueName The name of the queue.
     * @return A name of the queue in the lower case.
     */
    private String normalizeQueueName(String queueName) {
        if (queueName == null) {
            return null;
        }
        return queueName.toLowerCase();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void close() throws IOException {
        synchronized (queuesMonitor) {
            FileUtils.close((Collection) queuesByUrl.values());
        }
    }
}
