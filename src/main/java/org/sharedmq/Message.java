package org.sharedmq;

/**
 * Service queue message.
 */
public interface Message {
    /**
     * @return The message body as a string.
     */
    public String asString();
}
