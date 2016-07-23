package org.sharedmq.internals;

import org.sharedmq.MappedQueueService;
import org.sharedmq.Message;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * This class contains restrictions for parameters of {@link MappedQueueService} methods.
 */
public class QueueServiceParametersValidator {

    private static final int MaxQueueNameLength = 80;
    private static final Pattern QueueNamePattern = Pattern.compile("^[0-9a-zA-Z_-]+$");

    private static final int MinVisibilityTimeout = 0;
    private static final int MaxVisibilityTimeout = 12 * 60 * 60;

    private static final int MinRetentionPeriod = 0;
    private static final int MaxRetentionPeriod = 14 * 24 * 60 * 60;

    private static final Charset MessageEncoding = StandardCharsets.UTF_8;
    private static final int MaxMessageSize = 256 * 1024;

    private static final int MinDelay = 0;
    private static final int MaxDelay = 900;

    private static final int MinPullTimeout = 0;
    private static final int MaxPullTimeout = 20;

    /**
     * Validates the parameters of the {@link MappedQueueService#createQueue(String, int, int)} method.
     *
     * @param queueName         Queue names must be 1-80 characters in length and be composed
     *                          of alphanumeric characters, hyphens (-) and underscores (_).
     * @param visibilityTimeout The amount of time (in seconds) that a message received from a queue
     *                          will be invisible to other receiving components.
     *                          Value must be between 0 seconds and 12 hours.
     * @param retentionPeriod   The amount of time (in seconds) that the queue will retain a message
     *                          if it does not get deleted.
     */
    public static void validateCreateQueue(String queueName, int visibilityTimeout, int retentionPeriod) {
        // queueName validation
        validateQueueName(queueName);

        // visibilityTimeout validation
        if (visibilityTimeout < MinVisibilityTimeout || visibilityTimeout > MaxVisibilityTimeout) {
            throw new IllegalArgumentException(
                    "The visibilityTimeout in seconds must be" +
                            " between " + MinVisibilityTimeout +
                            " and " + MaxVisibilityTimeout + ".");
        }

        // retentionPeriod validation
        if (retentionPeriod < MinRetentionPeriod || retentionPeriod > MaxRetentionPeriod) {
            throw new IllegalArgumentException(
                    "The retentionPeriod in seconds must be" +
                            " between " + MinRetentionPeriod +
                            " and " + MaxRetentionPeriod + ".");
        }
    }

    /**
     * Validates the given queue name.
     *
     * @param queueName A name of a queue.
     */
    public static void validateQueueName(String queueName) {
        if (queueName == null) {
            throw new IllegalArgumentException("The queue name cannot be null.");
        }
        if (queueName.length() == 0) {
            throw new IllegalArgumentException("The queue name cannot be empty.");
        }
        if (queueName.length() > MaxQueueNameLength) {
            throw new IllegalArgumentException("The queue name cannot be longer than 80 characters.");
        }
        if (!QueueNamePattern.matcher(queueName).matches()) {
            throw new IllegalArgumentException(
                    "The queue name can contain only alphanumeric characters, hyphens and underscores.");
        }
    }

    /**
     * Validates the parameters of the {@link MappedQueueService#push(String, int, String)} method.
     *
     * @param queueUrl The URL of the queue.
     * @param delay    The amount of time in seconds to delay the first delivery of this message.
     *                 Must be greater than or equal to 0, and less than or equal to 900.
     * @param message  The message to push.
     */
    public static void validatePush(String queueUrl, int delay, String message) {
        if (queueUrl == null) {
            throw new IllegalArgumentException("The queueUrl parameter cannot be null.");
        }
        if (delay < MinDelay || delay > MaxDelay) {
            throw new IllegalArgumentException(
                    "The delay in seconds must be" +
                            " between " + MinDelay +
                            " and " + MaxDelay + ".");
        }
        if (message == null) {
            throw new IllegalArgumentException("The message parameter cannot be null.");
        }
        if (message.getBytes(MessageEncoding).length > MaxMessageSize) {
            throw new IllegalArgumentException("The message cannot be longer than 256KB in the UTF-8 encoding.");
        }
    }

    /**
     * Validates the parameters of the {@link MappedQueueService#pull(String, int)} method.
     *
     * @param queueUrl The URL of the queue.
     * @param timeout  Timeout for this operation in seconds.
     *                 Value must be between 0 and 20 seconds.
     *                 If timeout is equal to zero, then pull operation does not wait for new messages.
     * @throws IllegalArgumentException If parameters are invalid.
     */
    public static void validatePull(String queueUrl, int timeout) {
        // queueUrl validation
        if (queueUrl == null) {
            throw new IllegalArgumentException("The queueUrl parameter cannot be null.");
        }

        // timeout validation
        if (timeout < MinPullTimeout || timeout > MaxPullTimeout) {
            throw new IllegalArgumentException(
                    "The timeout in seconds must be" +
                            " between " + MinPullTimeout +
                            " and " + MaxPullTimeout + ".");
        }
    }

    /**
     * Validates the parameters of the {@link MappedQueueService#delete(String, Message)} method.
     *
     * @param queueUrl             The URL of the queue.
     * @param message              The message to delete.
     * @param expectedMessageClass The type of the message used by this service.
     * @throws IllegalArgumentException If parameters are invalid.
     */
    public static void validateDelete(String queueUrl, Message message, Class expectedMessageClass) {
        if (queueUrl == null) {
            throw new IllegalArgumentException("The queueUrl parameter cannot be null.");
        }
        if (message == null) {
            throw new IllegalArgumentException("The message parameter cannot be null.");
        }
        if (expectedMessageClass == null) {
            throw new IllegalArgumentException("The expectedMessageClass parameter cannot be null.");
        }
        if (!expectedMessageClass.isInstance(message)) {
            throw new IllegalArgumentException("This message does not belong to this type of a message queue.");
        }
    }
}
