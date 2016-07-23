package org.sharedmq.mapped;

import org.sharedmq.Message;
import org.sharedmq.MappedQueueService;

/**
 * A message returned by the {@link MappedQueueService}.
 */
public class MappedQueueMessage implements Message {

    private final String queueUrl;
    private final MappedQueueMessageHeader header;
    private final String body;

    public MappedQueueMessage(String queueUrl, MappedQueueMessageHeader header, String body) {
        this.queueUrl = queueUrl;
        this.header = header;
        this.body = body;
    }

    public String getQueueUrl() {
        return queueUrl;
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
