package org.sharedmq.internals;

import org.sharedmq.Message;
import org.sharedmq.SharedMessageQueue;

import java.io.File;

/**
 * An implementation of the {@link Message} returned by the {@link SharedMessageQueue}.
 */
public class SharedQueueMessage implements Message {

    private final File queueFolder;
    private final MessageHeader header;
    private final String body;

    public SharedQueueMessage(File queueFolder, MessageHeader header, String body) {
        this.queueFolder = queueFolder;
        this.header = header;
        this.body = body;
    }

    public File getQueueFolder() {
        return queueFolder;
    }

    public MessageHeader getHeader() {
        return header;
    }

    public String getBody() {
        return body;
    }

    @Override
    public String asString() {
        return body;
    }
}
