package org.sharedmq.internals;

import org.sharedmq.SharedMessageQueue;
import org.sharedmq.Message;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * This class contains restrictions for parameters of {@link SharedMessageQueue} methods.
 */
public class QueueParametersValidator {

    private static final long MinVisibilityTimeout = 0;
    private static final long MaxVisibilityTimeout = 12 * 60 * 60 * 1000L;

    private static final long MinRetentionPeriod = 15 * 1000L;
    private static final long MaxRetentionPeriod = 14 * 24 * 60 * 60 * 1000L;

    private static final Charset MessageEncoding = StandardCharsets.UTF_8;
    private static final int MaxMessageSize = 256 * 1024;

    private static final long MinDelay = 0;
    private static final long MaxDelay = 15 * 60 * 1000L;

    private static final long MinPullTimeout = 0;
    private static final long MaxPullTimeout = 20 * 1000L;

    /**
     * Validates the parameters of the {@link SharedMessageQueue#createQueue(File, long, long)} method.
     *
     * @param rootFolder        The folder where queue should be created.
     * @param visibilityTimeout The amount of time in milliseconds that a message received from a queue
     *                          will be invisible to other receiving components.
     *                          Value must be between 0 seconds and 12 hours.
     * @param retentionPeriod   The amount of time in milliseconds that the queue will retain a message
     *                          if it does not get deleted.
     *                          Value must be between 15 seconds and 14 days.
     * @throws IllegalArgumentException If parameters are invalid.
     */
    public static void validateCreateQueue(File rootFolder, long visibilityTimeout, long retentionPeriod) {
        // rootFolder validation
        if (rootFolder == null) {
            throw new IllegalArgumentException("The rootFolder parameter cannot be null.");
        }

        // visibilityTimeout validation
        if (visibilityTimeout < MinVisibilityTimeout || visibilityTimeout > MaxVisibilityTimeout) {
            throw new IllegalArgumentException(
                    "The visibilityTimeout in milliseconds must be" +
                            " between " + MinVisibilityTimeout +
                            " and " + MaxVisibilityTimeout + ".");
        }

        // retentionPeriod validation
        if (retentionPeriod < MinRetentionPeriod || retentionPeriod > MaxRetentionPeriod) {
            throw new IllegalArgumentException(
                    "The retentionPeriod in milliseconds must be" +
                            " between " + MinRetentionPeriod +
                            " and " + MaxRetentionPeriod + ".");
        }
    }

    /**
     * Validates the parameters of the {@link SharedMessageQueue#openQueue(File)} method.
     *
     * @param rootFolder The folder where queue is located.
     * @throws IllegalArgumentException If parameters are invalid.
     */
    public static void validateOpenQueue(File rootFolder) {
        // rootFolder validation
        if (rootFolder == null) {
            throw new IllegalArgumentException("The rootFolder parameter cannot be null.");
        }
    }

    /**
     * Validates the parameters of the {@link SharedMessageQueue#push(long, String)} method.
     *
     * @param delay   The amount of time in milliseconds to delay the first delivery of this message.
     *                Value must be between 0 seconds and 15 minutes.
     * @param message The message to push.
     * @throws IllegalArgumentException If parameters are invalid.
     */
    public static void validatePush(long delay, String message) {
        if (delay < MinDelay || delay > MaxDelay) {
            throw new IllegalArgumentException(
                    "The delay in milliseconds must be" +
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
     * Validates the parameters of the {@link SharedMessageQueue#pull(long)} method.
     *
     * @param timeout Timeout for this operation in milliseconds.
     *                Value must be between 0 and 20 seconds.
     *                If timeout is equal to zero, then pull operation does not wait for new messages.
     * @throws IllegalArgumentException If parameters are invalid.
     */
    public static void validatePull(long timeout) {
        // timeout validation
        if (timeout < MinPullTimeout || timeout > MaxPullTimeout) {
            throw new IllegalArgumentException(
                    "The timeout in milliseconds must be" +
                            " between " + MinPullTimeout +
                            " and " + MaxPullTimeout + ".");
        }
    }

    /**
     * Validates the parameters of the {@link SharedMessageQueue#delete(Message)} method.
     *
     * @param message The message to delete.
     * @throws IllegalArgumentException If parameters are invalid.
     */
    public static void validateDelete(Message message) {
        if (message == null) {
            throw new IllegalArgumentException("The message parameter cannot be null.");
        }
        if (!(message instanceof SharedQueueMessage)) {
            throw new IllegalArgumentException("This message type does not belong to this message queue.");
        }
    }
}
