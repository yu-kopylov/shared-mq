package org.sharedmq;

/**
 * A message returned by the {@link SharedMessageQueue}.<br/>
 * This interface hides implementation details of messages within the {@link SharedMessageQueue}.<br/>
 * Currently, only string messages are supported.
 */
public interface Message {
    /**
     * @return The message body as a string.
     */
    String asString();
}
