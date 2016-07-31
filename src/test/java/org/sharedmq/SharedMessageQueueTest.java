package org.sharedmq;

import com.google.common.base.Stopwatch;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.sharedmq.internals.QueueParametersValidator;
import org.sharedmq.internals.QueueParametersValidatorTest;
import org.sharedmq.internals.SharedQueueMessage;
import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
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
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testPushPullDelete");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
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

            assertEquals(0, queue.size());
        }
    }

    /**
     * Tests that queue correctly handles delete of the message from reused message slot.
     */
    @Test
    public void testDeleteReused() throws IOException, InterruptedException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testDeleteReused");
                SharedMessageQueue queue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
        ) {
            // check prerequisite
            assertEquals(0, queue.size());

            queue.push(0, "Test Message 1");
            SharedQueueMessage message1 = (SharedQueueMessage) queue.pull(LongPullTimeout);
            assertNotNull(message1);
            assertEquals("Test Message 1", message1.asString());

            queue.delete(message1);

            queue.push(0, "Test Message 2");
            SharedQueueMessage message2 = (SharedQueueMessage) queue.pull(LongPullTimeout);
            assertNotNull(message2);
            assertEquals("Test Message 2", message2.asString());

            // second message should reuse same message number, but have different id
            assertEquals(message1.getHeader().getMessageNumber(), message2.getHeader().getMessageNumber());
            assertNotEquals(message1.getHeader().getMessageId(), message2.getHeader().getMessageId());

            // deletion of the old message should not affect the queue
            queue.delete(message1);
            assertEquals(1, queue.size());

            // but the new message is deleted correctly
            queue.delete(message2);
            assertEquals(0, queue.size());
        }
    }

    /**
     * Tests that an opened queue has same properties as a created queue.
     */
    @Test
    public void testReopenQueue() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testReopenQueue")) {

            File queueFolder = testFolder.getRoot();

            try (SharedMessageQueue queue = SharedMessageQueue.createQueue(queueFolder, VisibilityTimeout, RetentionPeriod)) {
                // check prerequisite
                assertEquals(0, queue.size());

                queue.push(0, "Test Message 1");
                queue.push(0, "Test Message 2");
            }

            try (
                    SharedMessageQueue realQueue = SharedMessageQueue.openQueue(queueFolder);
                    SharedMessageQueue queue = spy(realQueue)
            ) {
                Message message1 = queue.pull(LongPullTimeout);
                Message message2 = queue.pull(LongPullTimeout);

                assertNotNull(message1);
                assertNotNull(message2);

                assertEquals("Test Message 1", message1.asString());
                assertEquals("Test Message 2", message2.asString());

                setTimeShift(queue, VisibilityTimeout + 10);

                // If parameters was read correctly, then messages should become visible now.
                message1 = queue.pull(LongPullTimeout);
                message2 = queue.pull(LongPullTimeout);

                assertNotNull(message1);
                assertNotNull(message2);

                assertEquals("Test Message 1", message1.asString());
                assertEquals("Test Message 2", message2.asString());

                setTimeShift(queue, RetentionPeriod + 20);

                // If parameters was read correctly, then messages should become expired now.
                assertNull(queue.pull(ShortPullTimeout));
                assertEquals(0, queue.size());
            }
        }
    }

    /**
     * Tests that queue service respects message delay.
     */
    @Test
    public void testPushDelay() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testPushDelay");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
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

            setTimeShift(queue, 9000);

            Message message3 = queue.pull(ShortPullTimeout);
            assertNull(message3);

            setTimeShift(queue, 11000);

            Message message4 = queue.pull(ShortPullTimeout);
            assertNotNull(message4);
            assertEquals("Test Message 2", message4.asString());
        }
    }

    /**
     * Tests that pull waits for a message when queue is empty.
     */
    @Test
    public void testPullDelay() throws IOException, InterruptedException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testPullDelay");
                SharedMessageQueue queue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
        ) {
            // check prerequisite
            assertEquals(0, queue.size());

            Stopwatch sw = Stopwatch.createStarted();

            assertNull(queue.pull(100));

            long elapsed = sw.elapsed(TimeUnit.MILLISECONDS);

            assertTrue("Elapsed: " + elapsed, elapsed >= 99);
        }
    }

    /**
     * Tests that a message can be deleted successfully
     * even after its visibility timeout expired and it was received by another consumer.
     */
    @Test
    public void testDeleteAfterVisibilityTimeout() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testDeleteAfterVisibilityTimeout");
                //todo: define constants
                SharedMessageQueue realQueue1 = SharedMessageQueue.createQueue(testFolder.getRoot(), 5 * 1000L, 60 * 1000L);
                SharedMessageQueue realQueue2 = SharedMessageQueue.createQueue(testFolder.getRoot(), 5 * 1000L, 60 * 1000L);
                SharedMessageQueue queue1 = spy(realQueue1);
                SharedMessageQueue queue2 = spy(realQueue2)
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

            setTimeShift(queue1, 6000);
            setTimeShift(queue2, 6000);

            // deleting the message after its visibility timeout is expired
            queue1.delete(message1);

            Message message3 = queue2.pull(ShortPullTimeout);
            Message message4 = queue2.pull(ShortPullTimeout);
            assertNotNull(message3);
            assertEquals("Test Message 2", message3.asString());
            assertNull(message4);

            setTimeShift(queue1, 12000);
            setTimeShift(queue2, 12000);

            // deleting the message after its visibility timeout expired and it was received by another consumer
            queue1.delete(message2);
            Message message5 = queue2.pull(ShortPullTimeout);
            assertNull(message5);
        }
    }

    /**
     * Tests that pull with long timeout will receive a delayed message soon after it becomes available.
     */
    @Test
    public void testDelayWithLongWait() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testDelayWithLongWait");
                SharedMessageQueue queue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)
        ) {
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
        }
    }

    /**
     * Tests than queue supports visibility timeout.
     */
    @Test
    public void testVisibilityTimeout() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testVisibilityTimeout");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
            // check prerequisite
            assertNull(queue.pull(ShortPullTimeout));

            queue.push(0, "Test Message");
            Message message1 = queue.pull(ShortPullTimeout);
            assertNotNull(message1);

            // receive message is invisible
            assertNull(queue.pull(ShortPullTimeout));

            // receive message is invisible after some time
            setTimeShift(queue, VisibilityTimeout - 500);
            assertNull(queue.pull(ShortPullTimeout));

            // receive message becomes visible after VisibilityTimeout
            setTimeShift(queue, VisibilityTimeout + 500);
            Message message2 = queue.pull(ShortPullTimeout);
            assertNotNull(message2);
            assertEquals(message1.asString(), message2.asString());
        }
    }

    /**
     * Tests than queue supports retention period.
     */
    @Test
    public void testRetentionPeriod() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testRetentionPeriod");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
            // check prerequisite
            assertNull(queue.pull(ShortPullTimeout));

            queue.push(0, "Test Message 1");
            queue.push(0, "Test Message 2");

            // after some time messages are still available
            setTimeShift(queue, RetentionPeriod - 500);
            assertNotNull(queue.pull(ShortPullTimeout));

            // after RetentionPeriod messages are unavailable
            setTimeShift(queue, RetentionPeriod + 500);
            assertNull(queue.pull(ShortPullTimeout));
        }
    }


    /**
     * Tests than queue supports retention period.
     */
    @Test
    public void testCleanup() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testCleanup");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
            // check prerequisite
            assertNull(queue.pull(ShortPullTimeout));

            int messageCount = SharedMessageQueue.CleanupBatchSize * 2;

            for (int i = 0; i < messageCount; i++) {
                queue.push(0, "Test Message");
            }

            assertEquals(messageCount, queue.size());

            setTimeShift(queue, RetentionPeriod + 10);

            assertEquals(0, queue.size());
        }
    }

    @Test
    public void testRelativePaths() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testRelativePaths")) {

            File subFolder = testFolder.getFile("subfolder");
            File alternateRootPath = new File(subFolder, "..");

            try (
                    SharedMessageQueue queue1 = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                    SharedMessageQueue queue2 = SharedMessageQueue.createQueue(alternateRootPath, VisibilityTimeout, RetentionPeriod);
                    SharedMessageQueue queue3 = SharedMessageQueue.createQueue(subFolder, VisibilityTimeout, RetentionPeriod)
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
            }
        }
    }

    /**
     * Tests that queue throws reasonable exceptions when the path to queue folder is wrong.
     */
    @Test
    public void testWrongPath() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testWrongPath")) {

            File file = testFolder.getFile("somefile");
            assertTrue(file.createNewFile());

            assertThrows(
                    IOException.class,
                    "not a directory",
                    () -> SharedMessageQueue.createQueue(file, VisibilityTimeout, RetentionPeriod));

            assertThrows(
                    IOException.class,
                    "does not exist",
                    () -> SharedMessageQueue.openQueue(file));

            // Sanity check: file is still there
            assertTrue(file.exists());
            assertTrue(file.isFile());
        }
    }

    /**
     * Tests that queue throws reasonable exceptions when folder already has files
     * with same name as files required for queue to work.
     */
    @Test
    public void testCorruptedFileStructure() throws InterruptedException, IOException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testWrongPath")) {

            File queueFolder = testFolder.getFile("queue");
            File priorityQueueFile = new File(queueFolder, SharedMessageQueue.PriorityQueueFilename);
            int fakeFileLength = 2;

            assertTrue(queueFolder.mkdir());
            try (FileOutputStream stream = new FileOutputStream(priorityQueueFile)) {
                stream.write(new byte[fakeFileLength]);
            }

            assertThrows(
                    IOException.class,
                    "file is too short to be a MappedArrayList file",
                    () -> SharedMessageQueue.createQueue(queueFolder, VisibilityTimeout, RetentionPeriod));

            assertThrows(
                    IOException.class,
                    "file does not contain the MappedArrayList file marker",
                    () -> SharedMessageQueue.openQueue(queueFolder));

            // Sanity check: files are still there and not modified
            assertTrue(queueFolder.exists());
            assertTrue(queueFolder.isDirectory());
            assertTrue(priorityQueueFile.exists());
            assertTrue(priorityQueueFile.isFile());
            assertEquals(fakeFileLength, priorityQueueFile.length());
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
                    () -> SharedMessageQueue.createQueue(null, 0, 120));

            try (SharedMessageQueue service = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)) {
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
            }
        }
    }

    @Test
    public void testMessageIdGeneration() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testMessageIdGeneration");
                SharedMessageQueue queue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod)
        ) {
            // we use some delay between messages to guarantee exact message order
            queue.push(0, "Test Message 1");
            Thread.sleep(2);
            queue.push(0, "Test Message 2");
            Thread.sleep(2);
            queue.push(0, "Test Message 3");

            SharedQueueMessage message1 = (SharedQueueMessage) queue.pull(ShortPullTimeout);
            SharedQueueMessage message2 = (SharedQueueMessage) queue.pull(ShortPullTimeout);
            SharedQueueMessage message3 = (SharedQueueMessage) queue.pull(ShortPullTimeout);

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

            SharedQueueMessage message4 = (SharedQueueMessage) queue.pull(ShortPullTimeout);
            SharedQueueMessage message5 = (SharedQueueMessage) queue.pull(ShortPullTimeout);
            SharedQueueMessage message6 = (SharedQueueMessage) queue.pull(ShortPullTimeout);

            assertEquals("Test Message 4", message4.asString());
            assertEquals("Test Message 5", message5.asString());
            assertEquals("Test Message 6", message6.asString());

            assertEquals(2, message4.getHeader().getMessageNumber());
            assertEquals(1, message5.getHeader().getMessageNumber());
            assertEquals(0, message6.getHeader().getMessageNumber());

            assertEquals(3, message4.getHeader().getMessageId());
            assertEquals(4, message5.getHeader().getMessageId());
            assertEquals(5, message6.getHeader().getMessageId());
        }
    }

    @Test
    public void testPullAfterPushRollback() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testPushRollback");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
            // 1st call is in cleanupQueue
            // 2nd call is in push
            doCallRealMethod().doThrow(RuntimeException.class).when(queue).commit();

            assertThrows(RuntimeException.class, null, () -> queue.push(0, "Test Message"));

            doCallRealMethod().when(queue).commit();

            assertNull(queue.pull(0));
            assertEquals(queue.size(), 0);
        }
    }

    @Test
    public void testPushAfterPushRollback() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testPushRollback");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
            // 1st call is in cleanupQueue
            // 2nd call is in push
            doCallRealMethod().doThrow(RuntimeException.class).when(queue).commit();

            assertThrows(RuntimeException.class, null, () -> queue.push(0, "Test Message"));

            doCallRealMethod().when(queue).commit();

            queue.push(0, "Test Message");
            assertEquals(queue.size(), 1);

            Message message = queue.pull(0);
            assertNotNull(message);
            assertEquals("Test Message", message.asString());

            queue.delete(message);
            assertNull(queue.pull(0));
            assertEquals(queue.size(), 0);
        }
    }

    @Test
    public void testPullAfterPullRollback() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testPullRollback");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
            queue.push(0, "Test Message");

            // 1st call is in cleanupQueue
            // 2nd call is in pollMessage
            doCallRealMethod().doThrow(RuntimeException.class).when(queue).commit();

            assertThrows(RuntimeException.class, null, () -> queue.pull(0));

            doCallRealMethod().when(queue).commit();

            Message message = queue.pull(0);
            assertNotNull(message);
            assertEquals("Test Message", message.asString());
            queue.delete(message);
            assertEquals(0, queue.size());
        }
    }

    @Test
    public void testPushAfterPullRollback() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testPullRollback");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
            queue.push(0, "Test Message 1");

            // 1st call is in cleanupQueue
            // 2nd call is in pollMessage
            doCallRealMethod().doThrow(RuntimeException.class).when(queue).commit();

            assertThrows(RuntimeException.class, null, () -> queue.pull(0));

            doCallRealMethod().when(queue).commit();

            queue.push(0, "Test Message 2");
            assertEquals(2, queue.size());

            Message message1 = queue.pull(0);
            Message message2 = queue.pull(0);
            assertNotNull(message1);
            assertNotNull(message2);
            assertEquals("Test Message 1", message1.asString());
            assertEquals("Test Message 2", message2.asString());
            queue.delete(message1);
            queue.delete(message2);
            assertEquals(0, queue.size());
        }
    }

    @Test
    public void testDeleteRollback() throws InterruptedException, IOException {
        try (
                TestFolder testFolder = new TestFolder("SharedMessageQueueTest", "testDeleteRollback");
                SharedMessageQueue realQueue = SharedMessageQueue.createQueue(testFolder.getRoot(), VisibilityTimeout, RetentionPeriod);
                SharedMessageQueue queue = spy(realQueue)
        ) {
            queue.push(0, "Test Message");

            Message message1 = queue.pull(0);
            assertNotNull(message1);
            assertEquals("Test Message", message1.asString());

            // 1st call is in cleanupQueue
            // 2nd call is in pollMessage
            doCallRealMethod().doThrow(RuntimeException.class).when(queue).commit();

            assertThrows(RuntimeException.class, null, () -> queue.delete(message1));

            doCallRealMethod().when(queue).commit();

            assertEquals(1, queue.size());

            setTimeShift(queue, VisibilityTimeout + 10);

            Message message2 = queue.pull(0);
            assertNotNull(message2);
            assertEquals("Test Message", message2.asString());

            queue.delete(message1);

            setTimeShift(queue, 2 * VisibilityTimeout + 20);

            assertNull(queue.pull(0));
            assertEquals(queue.size(), 0);
        }
    }

    /**
     * Overrides the {@link SharedMessageQueue#getTime()} method to return altered values.
     *
     * @param queueSpy  The Mockito spy of the {@link SharedMessageQueue}.
     * @param timeShift The value that should be added to the
     *                  original result of the {@link SharedMessageQueue#getTime()} method.
     */
    private void setTimeShift(SharedMessageQueue queueSpy, long timeShift) {
        when(queueSpy.getTime()).thenAnswer(inv -> ((long) inv.callRealMethod()) + timeShift);
    }
}
