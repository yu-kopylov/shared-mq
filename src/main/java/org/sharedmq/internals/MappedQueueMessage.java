package org.sharedmq.internals;

import org.sharedmq.Message;

import java.io.File;

/**
 * A message returned by the {@link org.sharedmq.MappedQueue}.
 */
public class MappedQueueMessage implements Message {

    private final File queueFolder;
    private final MappedQueueMessageHeader header;
    private final String body;

    public MappedQueueMessage(File queueFolder, MappedQueueMessageHeader header, String body) {
        this.queueFolder = queueFolder;
        this.header = header;
        this.body = body;
    }

    public File getQueueFolder() {
        return queueFolder;
    }

    public MappedQueueMessageHeader getHeader() {
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
