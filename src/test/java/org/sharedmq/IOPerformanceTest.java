package org.sharedmq;

import org.sharedmq.test.PerformanceTests;
import org.sharedmq.test.TestFolder;
import org.sharedmq.util.FileUtils;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Tests performance of basic I/O operations.
 */
@Category(PerformanceTests.class)
public class IOPerformanceTest {

    @Test
    public void testFolderOperations() throws IOException {

        final int iterations = 10;
        final int batchSize = 1000;

        List<File> folders = new ArrayList<>();

        try (TestFolder testFolder = new TestFolder("IOPerformanceTest", "testFolderOperations")) {

            for (int i = 0; i < batchSize; i++) {
                folders.add(testFolder.getFile("Folder" + i));
            }

            Stopwatch createTimer = Stopwatch.createUnstarted();
            Stopwatch createExistingTimer = Stopwatch.createUnstarted();
            Stopwatch deleteTimer = Stopwatch.createUnstarted();

            for (int i = 0; i < iterations; i++) {
                createTimer.start();
                for (File folder : folders) {
                    assertTrue(folder.mkdir());
                }
                createTimer.stop();

                createExistingTimer.start();
                for (File folder : folders) {
                    assertFalse(folder.mkdir());
                }
                createExistingTimer.stop();

                deleteTimer.start();
                for (File folder : folders) {
                    assertTrue(folder.delete());
                }
                deleteTimer.stop();
            }

            final int operationCount = batchSize * iterations;
            printResult("Create Folder (New)", createTimer, operationCount);
            printResult("Create Folder (Already Exists)", createExistingTimer, operationCount);
            printResult("Delete Folder", deleteTimer, operationCount);
        }
    }

    @Test
    public void testFileOperations() throws IOException {

        final int iterations = 10;
        final int batchSize = 1000;

        List<File> files = new ArrayList<>();

        try (TestFolder testFolder = new TestFolder("IOPerformanceTest", "testFileOperations")) {

            for (int i = 0; i < batchSize; i++) {
                files.add(testFolder.getFile("Folder" + i));
            }

            Stopwatch createTimer = Stopwatch.createUnstarted();
            Stopwatch createExistingTimer = Stopwatch.createUnstarted();
            Stopwatch deleteTimer = Stopwatch.createUnstarted();

            for (int i = 0; i < iterations; i++) {
                createTimer.start();
                for (File file : files) {
                    assertTrue(file.createNewFile());
                }
                createTimer.stop();

                createExistingTimer.start();
                for (File file : files) {
                    assertFalse(file.createNewFile());
                }
                createExistingTimer.stop();

                deleteTimer.start();
                for (File file : files) {
                    assertTrue(file.delete());
                }
                deleteTimer.stop();
            }

            final int operationCount = batchSize * iterations;
            printResult("Create Empty File", createTimer, operationCount);
            printResult("Create Empty File (Already Exists)", createExistingTimer, operationCount);
            printResult("Delete Empty File", deleteTimer, operationCount);
        }
    }

    @Test
    public void testFileStream() throws IOException {
        testFileStream(64);
        testFileStream(64 * 1024);
    }

    private void testFileStream(final int fileSize) throws IOException {

        final int iterations = 10;
        final int batchSize = 1000;

        final byte[] text = new byte[fileSize];
        final byte[] text2 = new byte[fileSize];
        final byte[] text3 = new byte[fileSize];

        for (int i = 0; i < fileSize; i++) {
            text2[i] = 0x7F;
        }

        List<File> files = new ArrayList<>();

        try (TestFolder testFolder = new TestFolder("IOPerformanceTest", "testFileStream")) {

            for (int i = 0; i < batchSize; i++) {
                files.add(testFolder.getFile("File" + i));
            }

            Stopwatch createTimer = Stopwatch.createUnstarted();
            Stopwatch overwriteTimer = Stopwatch.createUnstarted();
            Stopwatch readTimer = Stopwatch.createUnstarted();

            for (int i = 0; i < iterations; i++) {
                createTimer.start();
                for (File file : files) {
                    try (FileOutputStream stream = new FileOutputStream(file);
                         BufferedOutputStream bufferedStream = new BufferedOutputStream(stream);
                    ) {
                        bufferedStream.write(text);
                    }
                }
                createTimer.stop();

                overwriteTimer.start();
                for (File file : files) {
                    try (FileOutputStream stream = new FileOutputStream(file);
                         BufferedOutputStream bufferedStream = new BufferedOutputStream(stream);
                    ) {
                        bufferedStream.write(text2);
                    }
                }
                overwriteTimer.stop();

                readTimer.start();
                for (File file : files) {
                    try (FileInputStream stream = new FileInputStream(file);
                         BufferedInputStream bufferedStream = new BufferedInputStream(stream);
                    ) {
                        assertEquals(fileSize, bufferedStream.read(text3));
                    }
                }
                readTimer.stop();

                for (File file : files) {
                    assertTrue(file.delete());
                }
            }

            final int operationCount = batchSize * iterations;
            printResult("FileOutputStream (" + formatSize(fileSize) + ", Create)", createTimer, operationCount);
            printResult("FileOutputStream (" + formatSize(fileSize) + ", Overwrite)", overwriteTimer, operationCount);
            printResult("FileInputStream  (" + formatSize(fileSize) + ", Read)", readTimer, operationCount);
        }
    }

    @Test
    public void testRandomAccessFile() throws IOException {
        testRandomAccessFile(64);
        testRandomAccessFile(64 * 1024);
    }

    private void testRandomAccessFile(final int fileSize) throws IOException {

        final int iterations = 10;
        final int batchSize = 1000;

        final byte[] text = new byte[fileSize];
        final byte[] text2 = new byte[fileSize];
        final byte[] text3 = new byte[fileSize];

        for (int i = 0; i < fileSize; i++) {
            text2[i] = 0x7F;
        }

        List<File> files = new ArrayList<>();

        try (TestFolder testFolder = new TestFolder("IOPerformanceTest", "testRandomAccessFile")) {

            for (int i = 0; i < batchSize; i++) {
                files.add(testFolder.getFile("File" + i));
            }

            Stopwatch createTimer = Stopwatch.createUnstarted();
            Stopwatch overwriteTimer = Stopwatch.createUnstarted();
            Stopwatch updateTimer = Stopwatch.createUnstarted();
            Stopwatch readTimer = Stopwatch.createUnstarted();

            for (int i = 0; i < iterations; i++) {
                createTimer.start();
                for (File file : files) {
                    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
                        randomAccessFile.write(text);
                    }
                }
                createTimer.stop();

                overwriteTimer.start();
                for (File file : files) {
                    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
                        randomAccessFile.write(text2);
                    }
                }
                overwriteTimer.stop();

                updateTimer.start();
                for (File file : files) {
                    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
                        randomAccessFile.seek(fileSize / 2);
                        randomAccessFile.writeByte(0x22);
                    }
                }
                updateTimer.stop();

                readTimer.start();
                for (File file : files) {
                    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
                        assertEquals(fileSize, randomAccessFile.read(text3));
                    }
                }
                readTimer.stop();

                for (File file : files) {
                    assertTrue(file.delete());
                }
            }

            final int operationCount = batchSize * iterations;
            printResult("RandomAccessFile (" + formatSize(fileSize) + ", Create)", createTimer, operationCount);
            printResult("RandomAccessFile (" + formatSize(fileSize) + ", Overwrite)", overwriteTimer, operationCount);
            printResult("RandomAccessFile (" + formatSize(fileSize) + ", Small Update)", updateTimer, operationCount);
            printResult("RandomAccessFile (" + formatSize(fileSize) + ", Read)", readTimer, operationCount);
        }
    }

    @Test
    public void testMappedByteBufferCreation() throws IOException {

        final int fileSize = 64 * 1024;
        final int iterations = 200000;

        try (
                TestFolder testFolder = new TestFolder("IOPerformanceTest", "testMappedByteBufferSequential");
                RandomAccessFile randomAccessFile = new RandomAccessFile(testFolder.getFile("test.dat"), "rw");
                FileChannel channel = randomAccessFile.getChannel()
        ) {

            Stopwatch timer = Stopwatch.createStarted();
            for (int i = 0; i < iterations; i++) {
                channel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            }
            timer.stop();

            printResult("MappedByteBuffer (" + formatSize(fileSize) + ", Create)", timer, iterations);
        }
    }

    @Test
    public void testMappedByteBufferSequential() throws IOException {

        final int entriesCount = 10 * 1024 * 1024;
        final int entrySize = 8;
        final int fileSize = entrySize * entriesCount;

        try (TestFolder testFolder = new TestFolder("IOPerformanceTest", "testMappedByteBufferSequential")) {

            MappedByteBuffer buffer1 = FileUtils.createMappedByteBuffer(testFolder.getFile("test.dat"), fileSize);
            MappedByteBuffer buffer2 = FileUtils.createMappedByteBuffer(testFolder.getFile("test.dat"), fileSize);

            Stopwatch sequentialWriteTimer = Stopwatch.createStarted();
            for (int i = 0; i < entriesCount; i++) {
                buffer1.putLong(i * entrySize, i);
            }
            sequentialWriteTimer.stop();

            Stopwatch sequentialReadTimer = Stopwatch.createStarted();
            for (int i = 0; i < entriesCount; i++) {
                assertEquals(buffer2.getLong(i * entrySize), i);
            }
            sequentialReadTimer.stop();

            // A mapped byte buffer and the file mapping that it represents
            // remain valid until the buffer itself is garbage-collected.
            buffer1 = null;
            buffer2 = null;

            printResult("MappedByteBuffer (" + formatSize(fileSize) + ", Sequential Write)", sequentialWriteTimer, entriesCount);
            printResult("MappedByteBuffer (" + formatSize(fileSize) + ", Sequential Read)", sequentialReadTimer, entriesCount);
        }
    }

    @Test
    public void testMappedByteBufferRandom() throws IOException {

        final int entriesCount = 10 * 1024 * 1024;
        final int entrySize = 8;
        final int fileSize = entrySize * entriesCount;

        try (TestFolder testFolder = new TestFolder("IOPerformanceTest", "testMappedByteBufferRandom")) {

            MappedByteBuffer buffer1 = FileUtils.createMappedByteBuffer(testFolder.getFile("test.dat"), fileSize);
            MappedByteBuffer buffer2 = FileUtils.createMappedByteBuffer(testFolder.getFile("test.dat"), fileSize);

            try {

                Stopwatch randomWriteTimer = Stopwatch.createStarted();
                for (int i = 0; i < entriesCount; i++) {
                    int entryNum = (int) ((i * 11071L) % entriesCount);
                    buffer1.putLong(entryNum * entrySize, i);
                }
                randomWriteTimer.stop();

                Stopwatch randomReadTimer = Stopwatch.createStarted();
                for (int i = 0; i < entriesCount; i++) {
                    int entryNum = (int) ((i * 11071L) % entriesCount);
                    assertEquals(buffer2.getLong(entryNum * entrySize), i);
                }
                randomReadTimer.stop();

                printResult("MappedByteBuffer (" + formatSize(fileSize) + ", Random Write)", randomWriteTimer, entriesCount);
                printResult("MappedByteBuffer (" + formatSize(fileSize) + ", Random Read)", randomReadTimer, entriesCount);
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                buffer1 = null;
                buffer2 = null;
            }
        }
    }

    @Test
    public void testThreadSleep() throws IOException, InterruptedException {

        final int operationCount = 1000;

        Stopwatch sw;

        sw = Stopwatch.createStarted();
        for (int i = 0; i < operationCount; i++) {
            Thread.sleep(0);
        }
        printResult("Thread.sleep(0)", sw, operationCount);

        sw = Stopwatch.createStarted();
        for (int i = 0; i < operationCount; i++) {
            Thread.yield();
        }
        printResult("Thread.yield", sw, operationCount);

        sw = Stopwatch.createStarted();
        for (int i = 0; i < operationCount; i++) {
            Thread.sleep(1);
        }
        printResult("Thread.sleep(1)", sw, operationCount);

        sw = Stopwatch.createStarted();
        for (int i = 0; i < operationCount; i++) {
            Thread.sleep(5);
        }
        printResult("Thread.sleep(5)", sw, operationCount);
    }

    @Test
    public void testSystemTime() throws IOException, InterruptedException {

        final int operationCount = 100000000;

        Stopwatch sw;

        sw = Stopwatch.createStarted();
        for (int i = 0; i < operationCount; i++) {
            System.currentTimeMillis();
        }
        printResult("System.currentTimeMillis()", sw, operationCount);

        sw = Stopwatch.createStarted();
        for (int i = 0; i < operationCount; i++) {
            System.nanoTime();
        }
        printResult("System.nanoTime()", sw, operationCount);
    }

    private String formatSize(int size) {
        return Strings.padStart(size + "B", 6, ' ');
    }

    private static void printResult(String prefix, Stopwatch timer, int operationCount) {
        long timeSpent = timer.elapsed(TimeUnit.MILLISECONDS);
        String messagesPerSecond = timeSpent == 0 ? "unknown" : String.valueOf(operationCount * 1000L / timeSpent);

        long operationTime = timeSpent * 1000L / operationCount;

        System.out.println(Strings.padEnd(prefix, 39, ' ') +
                ": " + operationCount + " operations" +
                ", " + Strings.padStart(String.valueOf(timeSpent), 5, ' ') + "ms" +
                " (" + Strings.padStart(String.valueOf(operationTime), 5, ' ') + "\u00B5s per op." +
                ", " + Strings.padStart(messagesPerSecond, 4, ' ') + " op/second).");
    }
}
