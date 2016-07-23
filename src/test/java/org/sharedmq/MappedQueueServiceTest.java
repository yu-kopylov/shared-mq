package org.sharedmq;

import org.sharedmq.internals.MappedQueueMessage;
import org.sharedmq.internals.QueueServiceParametersValidator;
import org.sharedmq.internals.QueueServiceParametersValidatorTest;
import org.sharedmq.test.AdjustableMappedQueueService;
import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.sharedmq.test.TestUtils.assertThrows;
import static org.junit.Assert.*;

@Category(CommonTests.class)
public class MappedQueueServiceTest {

    private static final int VisibilityTimeout = 30;
    private static final int RetentionPeriod = 600;

    private static final int ShortPullTimeout = 0;
    private static final int LongPullTimeout = 20;

    /**
     * Tests basic queue service operations.
     */
    @Test
    public void testPushPullDelete() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testPushPullDelete")) {
            try (AdjustableMappedQueueService service = new AdjustableMappedQueueService(testFolder.getRoot())) {

                final String queueUrl = service.createQueue("testPushPullDelete", VisibilityTimeout, RetentionPeriod);

                // check prerequisite
                assertNull(service.pull(queueUrl, ShortPullTimeout));

                service.push(queueUrl, 0, "Test Message 1");
                service.push(queueUrl, 0, "Test Message 2");

                Message message1 = service.pull(queueUrl, LongPullTimeout);
                Message message2 = service.pull(queueUrl, LongPullTimeout);

                assertNotNull(message1);
                assertNotNull(message2);

                assertEquals(queueUrl, "Test Message 1", message1.asString());
                assertEquals(queueUrl, "Test Message 2", message2.asString());

                service.delete(queueUrl, message1);
                service.delete(queueUrl, message2);

                service.setTimeShift((VisibilityTimeout + 10) * 1000L);

                assertNull(service.pull(queueUrl, ShortPullTimeout));

            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    /**
     * Tests that queue service respects message delay.
     */
    @Test
    public void testDelay() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testDelay")) {
            try (AdjustableMappedQueueService service = new AdjustableMappedQueueService(testFolder.getRoot())) {
                final String queueUrl = service.createQueue("testDelay", VisibilityTimeout, RetentionPeriod);

                // check prerequisite
                assertNull(service.pull(queueUrl, ShortPullTimeout));

                // immediate delivery
                service.push(queueUrl, 0, "Test Message 1");
                // delivery after 10 seconds
                service.push(queueUrl, 10, "Test Message 2");

                Message message1 = service.pull(queueUrl, ShortPullTimeout);
                Message message2 = service.pull(queueUrl, ShortPullTimeout);

                assertNotNull(message1);
                assertEquals("Test Message 1", message1.asString());
                assertNull(message2);

                service.setTimeShift(9000);

                Message message3 = service.pull(queueUrl, ShortPullTimeout);
                assertNull(message3);

                service.setTimeShift(11000);

                Message message4 = service.pull(queueUrl, ShortPullTimeout);
                assertNotNull(message4);
                assertEquals("Test Message 2", message4.asString());
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    /**
     * Tests that a message can be deleted successfully
     * even after its visibility timeout expired and it was received by another consumer.
     */
    @Test
    public void testDeleteAfterVisibilityTimeout() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testDeleteAfterVisibilityTimeout")) {
            try (
                    AdjustableMappedQueueService service1 = new AdjustableMappedQueueService(testFolder.getRoot());
                    AdjustableMappedQueueService service2 = new AdjustableMappedQueueService(testFolder.getRoot())
            ) {
                final String queueUrl = service1.createQueue("testDeleteAfterVisibilityTimeout", 5, 60);

                // check prerequisite
                assertNull(service1.pull(queueUrl, ShortPullTimeout));
                assertNull(service2.pull(queueUrl, ShortPullTimeout));

                service1.push(queueUrl, 0, "Test Message 1");
                Message message1 = service1.pull(queueUrl, ShortPullTimeout);

                service1.push(queueUrl, 0, "Test Message 2");
                Message message2 = service1.pull(queueUrl, ShortPullTimeout);

                assertNotNull(message1);
                assertNotNull(message2);
                assertEquals("Test Message 1", message1.asString());
                assertEquals("Test Message 2", message2.asString());

                service1.setTimeShift(6000);
                service2.setTimeShift(6000);

                // deleting the message after its visibility timeout is expired
                service1.delete(queueUrl, message1);

                Message message3 = service2.pull(queueUrl, ShortPullTimeout);
                Message message4 = service2.pull(queueUrl, ShortPullTimeout);
                assertNotNull(message3);
                assertEquals("Test Message 2", message3.asString());
                assertNull(message4);

                service1.setTimeShift(12000);
                service2.setTimeShift(12000);

                // deleting the message after its visibility timeout expired and it was received by another consumer
                service1.delete(queueUrl, message2);
                Message message5 = service2.pull(queueUrl, ShortPullTimeout);
                assertNull(message5);
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    /**
     * Tests that pull with long timeout will receive a delayed message soon after it becomes available.
     */
    @Test
    public void testDelayWithLongWait() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testDelayWithLongWait")) {
            try (AdjustableMappedQueueService service = new AdjustableMappedQueueService(testFolder.getRoot())) {
                String queueUrl = service.createQueue("TestQueue", VisibilityTimeout, RetentionPeriod);
                // check prerequisite
                assertNull(service.pull(queueUrl, ShortPullTimeout));

                service.push(queueUrl, 1, "Test Message");
                long startTime = System.currentTimeMillis();
                Message message = service.pull(queueUrl, LongPullTimeout);
                long pullTime = System.currentTimeMillis() - startTime;
                assertNotNull(message);
                assertEquals("Test Message", message.asString());
                assertTrue(pullTime > 800);
                assertTrue(pullTime < 1200);
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    /**
     * Tests than queue supports visibility timeout.
     */
    @Test
    public void testVisibilityTimeout() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testVisibilityTimeout")) {
            try (AdjustableMappedQueueService service = new AdjustableMappedQueueService(testFolder.getRoot())) {
                String queueUrl = service.createQueue("TestQueue", VisibilityTimeout, RetentionPeriod);
                // check prerequisite
                assertNull(service.pull(queueUrl, ShortPullTimeout));

                service.push(queueUrl, 0, "Test Message");
                Message message1 = service.pull(queueUrl, ShortPullTimeout);
                assertNotNull(message1);

                // receive message is invisible
                assertNull(service.pull(queueUrl, ShortPullTimeout));

                // receive message is invisible after some time
                service.setTimeShift(VisibilityTimeout * 1000L - 500);
                assertNull(service.pull(queueUrl, ShortPullTimeout));

                // receive message becomes visible after VisibilityTimeout
                service.setTimeShift(VisibilityTimeout * 1000L + 500);
                Message message2 = service.pull(queueUrl, ShortPullTimeout);
                assertNotNull(message2);
                assertEquals(message1.asString(), message2.asString());
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    /**
     * Tests than queue supports retention period.
     */
    @Test
    public void testRetentionPeriod() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testRetentionPeriod")) {
            try (AdjustableMappedQueueService service = new AdjustableMappedQueueService(testFolder.getRoot())) {
                String queueUrl = service.createQueue("TestQueue", VisibilityTimeout, RetentionPeriod);
                // check prerequisite
                assertNull(service.pull(queueUrl, ShortPullTimeout));

                service.push(queueUrl, 0, "Test Message 1");
                service.push(queueUrl, 0, "Test Message 2");

                // after some time messages are still available
                service.setTimeShift(RetentionPeriod * 1000L - 500);
                assertNotNull(service.pull(queueUrl, ShortPullTimeout));

                // after RetentionPeriod messages are unavailable
                service.setTimeShift(RetentionPeriod * 1000L + 500);
                assertNull(service.pull(queueUrl, ShortPullTimeout));
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    @Test
    public void testQueueNameCase() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testQueueNameCase")) {
            try (
                    AdjustableMappedQueueService service1 = new AdjustableMappedQueueService(testFolder.getRoot());
                    AdjustableMappedQueueService service2 = new AdjustableMappedQueueService(testFolder.getRoot())
            ) {
                String queueUrl1 = "QUEUE-NAME";
                String queueUrl2 = "queue-name";

                service1.createQueue(queueUrl1, 5, 60);

                service1.push(queueUrl1, 0, "Queue Name Test Message 1");
                service1.push(queueUrl2, 0, "Queue Name Test Message 2");

                Message message1 = service2.pull(queueUrl1, 0);
                Message message2 = service2.pull(queueUrl2, 0);

                assertNotNull(message1);
                assertNotNull(message2);

                assertEquals(message1.asString(), "Queue Name Test Message 1");
                assertEquals(message2.asString(), "Queue Name Test Message 2");
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    /**
     * This tests only checks that service methods call {@link QueueServiceParametersValidator}.<br/>
     * More rigorous parameter testing is implemented in {@link QueueServiceParametersValidatorTest}.
     */
    @Test
    public void testParameterValidation() throws IOException {

        class FakeMessage implements Message {
            @Override
            public String asString() {
                return null;
            }
        }

        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testQueueNameCase")) {
            try (AdjustableMappedQueueService service = new AdjustableMappedQueueService(testFolder.getRoot())) {
                assertThrows(
                        IllegalArgumentException.class,
                        "queue name can contain only alphanumeric characters, hyphens and underscores",
                        () -> service.createQueue("!WrongQueeName", 0, 120));
                assertThrows(
                        IllegalArgumentException.class,
                        "delay in seconds must be between 0 and 900",
                        () -> service.push("QueueURL", -1, "Message"));
                assertThrows(
                        IllegalArgumentException.class,
                        "timeout in seconds must be between 0 and 20",
                        () -> service.pull("QueueURL", -1));
                assertThrows(
                        IllegalArgumentException.class,
                        "message does not belong to this type of a message queue",
                        () -> service.delete("QueueURL", new FakeMessage()));
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }


    @Test
    public void testMessageIdGeneration() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("MappedQueueServiceTest", "testMessageIdGeneration")) {
            try (
                    AdjustableMappedQueueService service = new AdjustableMappedQueueService(testFolder.getRoot());
            ) {
                String queueUrl = service.createQueue("test", 5, 60);

                // we use some delay between messages to guarantee exact message order
                service.push(queueUrl, 0, "Test Message 1");
                Thread.sleep(2);
                service.push(queueUrl, 0, "Test Message 2");
                Thread.sleep(2);
                service.push(queueUrl, 0, "Test Message 3");

                MappedQueueMessage message1 = (MappedQueueMessage) service.pull(queueUrl, 5);
                MappedQueueMessage message2 = (MappedQueueMessage) service.pull(queueUrl, 5);
                MappedQueueMessage message3 = (MappedQueueMessage) service.pull(queueUrl, 5);

                assertEquals("Test Message 1", message1.asString());
                assertEquals("Test Message 2", message2.asString());
                assertEquals("Test Message 3", message3.asString());

                assertEquals(0, message1.getHeader().getMessageNumber());
                assertEquals(1, message2.getHeader().getMessageNumber());
                assertEquals(2, message3.getHeader().getMessageNumber());

                assertEquals(0, message1.getHeader().getMessageId());
                assertEquals(1, message2.getHeader().getMessageId());
                assertEquals(2, message3.getHeader().getMessageId());

                // deleting all messages, and then replacing them

                service.delete(queueUrl, message1);
                service.delete(queueUrl, message2);
                service.delete(queueUrl, message3);

                // we use some delay between messages to guarantee exact message order
                service.push(queueUrl, 0, "Test Message 4");
                Thread.sleep(2);
                service.push(queueUrl, 0, "Test Message 5");
                Thread.sleep(2);
                service.push(queueUrl, 0, "Test Message 6");

                MappedQueueMessage message4 = (MappedQueueMessage) service.pull(queueUrl, 5);
                MappedQueueMessage message5 = (MappedQueueMessage) service.pull(queueUrl, 5);
                MappedQueueMessage message6 = (MappedQueueMessage) service.pull(queueUrl, 5);

                assertEquals("Test Message 4", message4.asString());
                assertEquals("Test Message 5", message5.asString());
                assertEquals("Test Message 6", message6.asString());

                assertEquals(2, message4.getHeader().getMessageNumber());
                assertEquals(1, message5.getHeader().getMessageNumber());
                assertEquals(0, message6.getHeader().getMessageNumber());

                assertEquals(3, message4.getHeader().getMessageId());
                assertEquals(4, message5.getHeader().getMessageId());
                assertEquals(5, message6.getHeader().getMessageId());

            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }
}
