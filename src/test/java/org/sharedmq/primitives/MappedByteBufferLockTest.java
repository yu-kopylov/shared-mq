package org.sharedmq.primitives;

import com.google.common.base.Stopwatch;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;
import org.sharedmq.util.IOUtils;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test checks the behaviour of locks within one JVM.<br/>
 * For the inter-process testing the {@link org.sharedmq.QueueTester} utility can be used.
 */
@Category(CommonTests.class)
public class MappedByteBufferLockTest {

    @Test
    public void testSmoke() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("MappedByteBufferLockTest", "testSmoke")) {

            MappedByteBuffer buffer = IOUtils.createMappedByteBuffer(
                    testFolder.getFile("test.dat"),
                    MappedByteBufferLock.LockSize + 1);

            try {

                for (int lockOffset = 0; lockOffset < 2; lockOffset++) {

                    // filling the file with some garbage that does not represent a valid lock
                    byte[] initialContent = new byte[MappedByteBufferLock.LockSize + 1];
                    for (int i = 0; i < initialContent.length; i++) {
                        initialContent[i] = (byte) 0xA3;
                    }

                    buffer.position(0);
                    buffer.put(initialContent);

                    Stopwatch sw = Stopwatch.createStarted();

                    try (MappedByteBufferLock lock = new MappedByteBufferLock(buffer, lockOffset)) {
                        // just checking that locks are not broken completely
                    }
                    try (MappedByteBufferLock lock = new MappedByteBufferLock(buffer, lockOffset)) {
                        // just checking that locks are not broken completely
                    }

                    long elapsed = sw.elapsed(TimeUnit.MILLISECONDS);
                    assertTrue("elapsed = " + elapsed + "ms", elapsed < 50);
                }

            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                buffer = null;
            }
        }
    }

    @Test
    public void testBlocking() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("MappedByteBufferLockTest", "testSmoke")) {

            MappedByteBuffer buffer = IOUtils.createMappedByteBuffer(
                    testFolder.getFile("test.dat"),
                    MappedByteBufferLock.LockSize + 1);

            try {

                for (int lockOffset = 0; lockOffset < 2; lockOffset++) {

                    CountDownLatch threadReady = new CountDownLatch(1);

                    AtomicLong waitTime = new AtomicLong(0);
                    AtomicBoolean hasError = new AtomicBoolean(false);

                    final MappedByteBuffer localBuffer = buffer;
                    final int localLockOffset = lockOffset;

                    Thread secondThread = new Thread(() -> {
                        try {
                            threadReady.countDown();
                            long lockWaitStarted = System.currentTimeMillis();
                            try (MappedByteBufferLock lock2 = new MappedByteBufferLock(localBuffer, localLockOffset)) {
                                waitTime.set(System.currentTimeMillis() - lockWaitStarted);
                            }
                        } catch (Throwable e) {
                            e.printStackTrace();
                            hasError.set(true);
                        }
                    });

                    try (MappedByteBufferLock lock1 = new MappedByteBufferLock(localBuffer, localLockOffset)) {
                        secondThread.start();
                        threadReady.await();
                        Thread.sleep(100);
                    }

                    secondThread.join();

                    assertFalse(hasError.get());
                    assertTrue("waitTime = " + waitTime.get() + "ms", waitTime.get() >= 90);
                }

            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                buffer = null;
            }
        }
    }
}
