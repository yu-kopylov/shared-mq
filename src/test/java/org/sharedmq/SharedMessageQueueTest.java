package org.sharedmq;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.internals.SharedQueueMessage;
import org.sharedmq.internals.QueueParametersValidator;
import org.sharedmq.internals.QueueParametersValidatorTest;
import org.sharedmq.test.AdjustableSharedMessageQueue;
import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.sharedmq.test.TestUtils.assertThrows;

@Category(CommonTests.class)
public class SharedMessageQueueTest {

    private static final long VisibilityTimeout = 30 * 1000L;
    private static final long RetentionPeriod = 600 * 1000L;

    private static final long ShortPullTimeout = 0;
    private static final long LongPullTimeout = 20 * 1000L;

    /**
     * Tests basic queue service operations.
     */
    @Test
    public void testPushPullDelete() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testPushPullDelete")) {
            try (AdjustableSharedMessageQueue queue = new AdjustableSharedMessageQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)) {

                // check prerequisite
                assertNull(queue.pull(ShortPullTimeout));

                queue.push(0, "Test Message 1");
                queue.push(0, "Test Message 2");

                Message message1 = queue.pull(LongPullTimeout);
                Message message2 = queue.pull(LongPullTimeout);

                assertNotNull(message1);
                assertNotNull(message2);

                assertEquals("Test Message 1", message1.asString());
                assertEquals("Test Message 2", message2.asString());

                queue.delete(message1);
                queue.delete(message2);

                queue.setTimeShift(VisibilityTimeout + 10);

                assertNull(queue.pull(ShortPullTimeout));

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
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testDelay")) {
            try (AdjustableSharedMessageQueue queue = new AdjustableSharedMessageQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)) {

                // check prerequisite
                assertNull(queue.pull(ShortPullTimeout));

                // immediate delivery
                queue.push(0, "Test Message 1");
                // delivery after 10 seconds
                queue.push(10 * 1000L, "Test Message 2");

                Message message1 = queue.pull(ShortPullTimeout);
                Message message2 = queue.pull(ShortPullTimeout);

                assertNotNull(message1);
                assertEquals("Test Message 1", message1.asString());
                assertNull(message2);

                queue.setTimeShift(9000);

                Message message3 = queue.pull(ShortPullTimeout);
                assertNull(message3);

                queue.setTimeShift(11000);

                Message message4 = queue.pull(ShortPullTimeout);
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
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testDeleteAfterVisibilityTimeout")) {
            try (
                    //todo: define constants
                    AdjustableSharedMessageQueue queue1 = new AdjustableSharedMessageQueue(testFolder.getRoot(), 5 * 1000L, 60 * 1000L);
                    AdjustableSharedMessageQueue queue2 = new AdjustableSharedMessageQueue(testFolder.getRoot(), 5 * 1000L, 60 * 1000L)
            ) {
                // check prerequisite
                assertNull(queue1.pull(ShortPullTimeout));
                assertNull(queue2.pull(ShortPullTimeout));

                queue1.push(0, "Test Message 1");
                Message message1 = queue1.pull(ShortPullTimeout);

                queue1.push(0, "Test Message 2");
                Message message2 = queue1.pull(ShortPullTimeout);

                assertNotNull(message1);
                assertNotNull(message2);
                assertEquals("Test Message 1", message1.asString());
                assertEquals("Test Message 2", message2.asString());

                queue1.setTimeShift(6000);
                queue2.setTimeShift(6000);

                // deleting the message after its visibility timeout is expired
                queue1.delete(message1);

                Message message3 = queue2.pull(ShortPullTimeout);
                Message message4 = queue2.pull(ShortPullTimeout);
                assertNotNull(message3);
                assertEquals("Test Message 2", message3.asString());
                assertNull(message4);

                queue1.setTimeShift(12000);
                queue2.setTimeShift(12000);

                // deleting the message after its visibility timeout expired and it was received by another consumer
                queue1.delete(message2);
                Message message5 = queue2.pull(ShortPullTimeout);
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
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testDelayWithLongWait")) {
            try (AdjustableSharedMessageQueue queue = new AdjustableSharedMessageQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)) {

                // check prerequisite
                assertNull(queue.pull(ShortPullTimeout));

                queue.push(1000, "Test Message");
                long startTime = System.currentTimeMillis();
                Message message = queue.pull(LongPullTimeout);
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
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testVisibilityTimeout")) {
            try (AdjustableSharedMessageQueue queue = new AdjustableSharedMessageQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)) {

                // check prerequisite
                assertNull(queue.pull(ShortPullTimeout));

                queue.push(0, "Test Message");
                Message message1 = queue.pull(ShortPullTimeout);
                assertNotNull(message1);

                // receive message is invisible
                assertNull(queue.pull(ShortPullTimeout));

                // receive message is invisible after some time
                queue.setTimeShift(VisibilityTimeout - 500);
                assertNull(queue.pull(ShortPullTimeout));

                // receive message becomes visible after VisibilityTimeout
                queue.setTimeShift(VisibilityTimeout + 500);
                Message message2 = queue.pull(ShortPullTimeout);
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
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testRetentionPeriod")) {
            try (AdjustableSharedMessageQueue service = new AdjustableSharedMessageQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)) {

                // check prerequisite
                assertNull(service.pull(ShortPullTimeout));

                service.push(0, "Test Message 1");
                service.push(0, "Test Message 2");

                // after some time messages are still available
                service.setTimeShift(RetentionPeriod - 500);
                assertNotNull(service.pull(ShortPullTimeout));

                // after RetentionPeriod messages are unavailable
                service.setTimeShift(RetentionPeriod + 500);
                assertNull(service.pull(ShortPullTimeout));
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    @Test
    public void testRelativePaths() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testRelativePaths")) {

            File subFolder = testFolder.getFile("subfolder");
            File alternateRootPath = new File(subFolder, "..");

            try (
                    SharedMessageQueue queue1 = new SharedMessageQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                    SharedMessageQueue queue2 = new SharedMessageQueue(alternateRootPath, VisibilityTimeout, RetentionPeriod);
                    SharedMessageQueue queue3 = new SharedMessageQueue(subFolder, VisibilityTimeout, RetentionPeriod)
            ) {

                // push message to queue1, and receive it with queue2
                queue1.push(0, "Test Message 1");
                Message message1 = queue2.pull(ShortPullTimeout);
                assertNotNull(message1);
                assertEquals("Test Message 1", message1.asString());

                // push message to queue1, and receive it with queue2
                queue1.push(0, "Test Message 2");
                Message message2 = queue2.pull(ShortPullTimeout);
                assertNotNull(message2);
                assertEquals("Test Message 2", message2.asString());

                // queue3 points to another path and cannot delete messages received with queue2
                assertThrows(
                        IllegalArgumentException.class,
                        "message was not received from this queue",
                        () -> queue3.delete(message1));

                assertThrows(
                        IllegalArgumentException.class,
                        "message was not received from this queue",
                        () -> queue3.delete(message2));

                // queue1 and queue2 point to the same path and can delete messages received with queue2
                queue1.delete(message1);
                queue2.delete(message2);
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    /**
     * This test only checks that public methods of the {@link SharedMessageQueue}
     * call {@link QueueParametersValidator}.<br/>
     * More rigorous parameter testing is implemented in {@link QueueParametersValidatorTest}.
     */
    @Test
    public void testParameterValidation() throws IOException, InterruptedException {

        class FakeMessage implements Message {
            @Override
            public String asString() {
                return null;
            }
        }

        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testQueueNameCase")) {

            assertThrows(
                    IllegalArgumentException.class,
                    "rootFolder parameter cannot be null",
                    () -> new SharedMessageQueue(null, 0, 120));

            try (SharedMessageQueue service = new SharedMessageQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)) {
                assertThrows(
                        IllegalArgumentException.class,
                        "delay in milliseconds must be between 0 and 900000",
                        () -> service.push(-1, "Message"));
                assertThrows(
                        IllegalArgumentException.class,
                        "timeout in milliseconds must be between 0 and 20000",
                        () -> service.pull(-1));
                assertThrows(
                        IllegalArgumentException.class,
                        "message type does not belong to this message queue",
                        () -> service.delete(new FakeMessage()));
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    @Test
    public void testMessageIdGeneration() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testMessageIdGeneration")) {
            try (
                    SharedMessageQueue queue = new SharedMessageQueue(testFolder.getRoot(), 5000, 60 * 1000);
            ) {
                // we use some delay between messages to guarantee exact message order
                queue.push(0, "Test Message 1");
                Thread.sleep(2);
                queue.push(0, "Test Message 2");
                Thread.sleep(2);
                queue.push(0, "Test Message 3");

                //todo: define constants
                SharedQueueMessage message1 = (SharedQueueMessage) queue.pull(5000);
                SharedQueueMessage message2 = (SharedQueueMessage) queue.pull(5000);
                SharedQueueMessage message3 = (SharedQueueMessage) queue.pull(5000);

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

                queue.delete(message1);
                queue.delete(message2);
                queue.delete(message3);

                // we use some delay between messages to guarantee exact message order
                queue.push(0, "Test Message 4");
                Thread.sleep(2);
                queue.push(0, "Test Message 5");
                Thread.sleep(2);
                queue.push(0, "Test Message 6");

                SharedQueueMessage message4 = (SharedQueueMessage) queue.pull(5000);
                SharedQueueMessage message5 = (SharedQueueMessage) queue.pull(5000);
                SharedQueueMessage message6 = (SharedQueueMessage) queue.pull(5000);

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
