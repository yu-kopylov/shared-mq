package org.sharedmq;

import com.google.common.base.Strings;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.test.PerformanceTests;
import org.sharedmq.test.TestFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(PerformanceTests.class)
public class SharedMessageQueuePerformanceTest {

    private static final long PullTimeout = 5000;
    private static final long Hours12 = 12 * 60 * 60 * 1000L;

    @Test
    public void testPushPullDelete() throws IOException, InterruptedException {
        //todo: shorten folder name
        try (TestFolder testFolder = new TestFolder("SharedMessageQueuePerformanceTest", "testPushPullDelete")) {
            testPushPullDelete(testFolder.getRoot(), 1, 5000000, 32);
        }
    }

    @Test
    public void testPushPullDeleteWith2Threads() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueuePerformanceTest", "testPushPullDeleteWith2Threads")) {
            testPushPullDelete(testFolder.getRoot(), 2, 2500000, 32);
        }
    }

    @Test
    public void testPushPullDeleteWith10Threads() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueuePerformanceTest", "testPushPullDeleteWith10Threads")) {
            testPushPullDelete(testFolder.getRoot(), 10, 500000, 32);
        }
    }

    @Test
    public void testPushAllPullAllDeleteAll() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueuePerformanceTest", "testPushAllPullAllDeleteAll")) {
            testPushAllPullAllDeleteAll(testFolder.getRoot(), 1, 500000, 32);
        }
    }

    @Test
    public void testPushAllPullAllDeleteWith2Threads() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueuePerformanceTest", "testPushAllPullAllDeleteWith2Threads")) {
            testPushAllPullAllDeleteAll(testFolder.getRoot(), 2, 250000, 32);
        }
    }

    @Test
    public void testPushAllPullAllDeleteWith10Threads() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueuePerformanceTest", "testPushAllPullAllDeleteWith10Threads")) {
            testPushAllPullAllDeleteAll(testFolder.getRoot(), 10, 50000, 32);
        }
    }

    @Test
    public void testPushAllPullAllDeleteAllWithBigMessages() throws IOException, InterruptedException {
        // Note: this test requires around 640Mb on the disc.
        try (TestFolder testFolder = new TestFolder("SharedMessageQueuePerformanceTest", "testPushAllPullAllDeleteAllWithBigMessages")) {
            testPushAllPullAllDeleteAll(testFolder.getRoot(), 2, 5000, 64 * 1024);
        }
    }

    private static void testPushPullDelete(
            File queueFolder,
            int threadCount,
            int messagesPerThread,
            int messageSize
    ) throws IOException, InterruptedException {

        System.out.println("testPushPullDelete started" +
                " (" + threadCount + " threads" +
                ", " + messagesPerThread + " messages per thread" +
                ", " + messageSize + " bytes per message)");

        final String messageBody = Strings.repeat("x", messageSize);
        final int totalMessageCount = threadCount * messagesPerThread;

        final AtomicInteger errorCount = new AtomicInteger(0);

        final List<Thread> threads = new ArrayList<>();
        final CountDownLatch startLatch = new CountDownLatch(threadCount);

        try (SharedMessageQueue queue = SharedMessageQueue.createQueue(queueFolder, Hours12, Hours12)) {
            for (int threadNum = 0; threadNum < threadCount; threadNum++) {
                threads.add(new Thread(() -> {
                    try {
                        // Thread startup can be slow.
                        // We want all threads to start their job approximately at same time.
                        startLatch.countDown();
                        startLatch.await();

                        for (int i = 0; i < messagesPerThread; i++) {
                            queue.push(0, messageBody);
                            Message message = queue.pull(PullTimeout);
                            assertNotNull(message);
                            queue.delete(message);
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    }
                }));
            }

            long started = System.currentTimeMillis();
            startThreads(threads);
            joinThreads(threads);
            long ended = System.currentTimeMillis();

            printResult("testPushPullDelete: ", ended - started, totalMessageCount);

            // Sanity check. No messages should remain in the queue.
            assertEquals(0, queue.size());
            assertEquals(0, errorCount.get());
        }
    }

    private static void testPushAllPullAllDeleteAll(
            File queueFolder,
            int threadCount,
            int messagesPerThread,
            int messageSize
    ) throws IOException, InterruptedException {

        System.out.println("testPushAllPullAllDeleteAll started" +
                " (" + threadCount + " threads" +
                ", " + messagesPerThread + " messages per thread" +
                ", " + messageSize + " bytes per message)");

        final Random random = new Random();
        final int maxDelay = 2000;
        final String messageBody = Strings.repeat("x", messageSize);
        final int totalMessageCount = threadCount * messagesPerThread;

        final AtomicInteger errorCount = new AtomicInteger(0);

        final List<Thread> pushThreads = new ArrayList<>();
        final CountDownLatch pushStartLatch = new CountDownLatch(threadCount);

        try (SharedMessageQueue queue = SharedMessageQueue.createQueue(queueFolder, Hours12, Hours12)) {
            for (int threadNum = 0; threadNum < threadCount; threadNum++) {
                pushThreads.add(new Thread(() -> {
                    try {
                        // Thread startup can be slow.
                        // We want all threads to start their job approximately at same time.
                        pushStartLatch.countDown();
                        pushStartLatch.await();

                        for (int i = 0; i < messagesPerThread; i++) {
                            // random is thread-safe
                            int delay = random.nextInt(maxDelay + 1);
                            queue.push(delay, messageBody);
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    }
                }));
            }

            long pushStarted = System.currentTimeMillis();
            startThreads(pushThreads);
            joinThreads(pushThreads);
            long pushEnded = System.currentTimeMillis();

            long pushTime = pushEnded - pushStarted;
            printResult("testPushAllPullAllDeleteAll: push", pushTime, totalMessageCount);

            // Waiting for all messages to become visible.
            // Performance tests usually take a long time, so thread sleeping is ok here.
            Thread.sleep(maxDelay);

            final List<Thread> pullThreads = new ArrayList<>();
            final CountDownLatch pullStartLatch = new CountDownLatch(threadCount);

            final List<List<Message>> messages = new ArrayList<>();

            for (int threadNum = 0; threadNum < threadCount; threadNum++) {
                List<Message> threadMessages = new ArrayList<>();
                messages.add(threadMessages);
                pullThreads.add(new Thread(() -> {
                    try {
                        // Thread startup can be slow.
                        // We want all threads to start their job approximately at same time.
                        pullStartLatch.countDown();
                        pullStartLatch.await();

                        for (int i = 0; i < messagesPerThread; i++) {
                            Message message = queue.pull(PullTimeout);
                            assertNotNull(message);
                            threadMessages.add(message);
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    }
                }));
            }


            long pullStarted = System.currentTimeMillis();
            startThreads(pullThreads);
            joinThreads(pullThreads);
            long pullEnded = System.currentTimeMillis();

            long pullTime = pullEnded - pullStarted;
            printResult("testPushAllPullAllDeleteAll: pull", pullTime, totalMessageCount);

            List<Thread> deleteThreads = new ArrayList<>();
            final CountDownLatch deleteStartLatch = new CountDownLatch(threadCount);

            for (int threadNum = 0; threadNum < threadCount; threadNum++) {
                List<Message> threadMessages = messages.get(threadNum);
                messages.add(threadMessages);
                deleteThreads.add(new Thread(() -> {
                    try {
                        // Thread startup can be slow.
                        // We want all threads to start their job approximately at same time.
                        deleteStartLatch.countDown();
                        deleteStartLatch.await();

                        for (Message message : threadMessages) {
                            queue.delete(message);
                        }
                    } catch (Exception e) {
                        errorCount.incrementAndGet();
                        e.printStackTrace();
                    }
                }));
            }

            long deleteStarted = System.currentTimeMillis();
            startThreads(deleteThreads);
            joinThreads(deleteThreads);
            long deleteEnded = System.currentTimeMillis();

            // Sanity check. No messages should remain in the queue.
            assertEquals(0, queue.size());
            assertEquals(0, errorCount.get());

            long deleteTime = deleteEnded - deleteStarted;
            printResult("testPushAllPullAllDeleteAll: delete", deleteTime, totalMessageCount);

            long totalTime = pushTime + pullTime + deleteTime;
            printResult("testPushAllPullAllDeleteAll: push+pull+delete", totalTime, totalMessageCount);
        }
    }

    private static void startThreads(List<Thread> threads) {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    private static void joinThreads(List<Thread> threads) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join();
        }
    }

    //todo: merge with printResult in IOPerformanceTest
    private static void printResult(String prefix, long timeSpent, int messageCount) {
        String messagesPerSecond = timeSpent == 0 ? "unknown" : String.valueOf(messageCount * 1000L / timeSpent);
        System.out.println(prefix +
                " completed in " + timeSpent + "ms" +
                " (" + messagesPerSecond + " messages per second).");

    }
}
