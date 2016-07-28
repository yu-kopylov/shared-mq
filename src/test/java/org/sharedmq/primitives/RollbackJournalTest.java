package org.sharedmq.primitives;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category(CommonTests.class)
public class RollbackJournalTest {

    @Test
    public void testMultipleFiles() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("RollbackJournalTest", "testMultipleFiles");
                RollbackJournal rollbackJournal = new RollbackJournal(testFolder.getFile("rollback.dat"));
                ProtectedFile file1 = rollbackJournal.openFile(10, testFolder.getFile("file1.dat"));
                ProtectedFile file2 = rollbackJournal.openFile(20, testFolder.getFile("file2.dat"));
        ) {
            file1.ensureCapacity(1024);
            file2.ensureCapacity(1024);

            file1.putInt(0, 0x12345678);
            file1.putInt(4, -0x12345678);
            file2.putLong(0, 0x1234567890ABCDEFL);
            file2.putLong(8, -0x1234567890ABCDEFL);

            assertEquals(0x12345678, file1.getInt(0));
            assertEquals(-0x12345678, file1.getInt(4));
            assertEquals(0x1234567890ABCDEFL, file2.getLong(0));
            assertEquals(-0x1234567890ABCDEFL, file2.getLong(8));

            rollbackJournal.commit();

            assertEquals(0x12345678, file1.getInt(0));
            assertEquals(-0x12345678, file1.getInt(4));
            assertEquals(0x1234567890ABCDEFL, file2.getLong(0));
            assertEquals(-0x1234567890ABCDEFL, file2.getLong(8));

            file1.putInt(0, 0x23456789);
            file1.putInt(4, -0x23456789);
            file2.putLong(0, 0x234567890ABCDEF1L);
            file2.putLong(8, -0x234567890ABCDEF1L);

            assertEquals(0x23456789, file1.getInt(0));
            assertEquals(-0x23456789, file1.getInt(4));
            assertEquals(0x234567890ABCDEF1L, file2.getLong(0));
            assertEquals(-0x234567890ABCDEF1L, file2.getLong(8));

            rollbackJournal.rollback();

            assertEquals(0x12345678, file1.getInt(0));
            assertEquals(-0x12345678, file1.getInt(4));
            assertEquals(0x1234567890ABCDEFL, file2.getLong(0));
            assertEquals(-0x1234567890ABCDEFL, file2.getLong(8));
        }
    }

    @Test
    public void testInt() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("RollbackJournalTest", "testInt");
                RollbackJournal rollbackJournal = new RollbackJournal(testFolder.getFile("rollback.dat"));
                ProtectedFile protectedFile = rollbackJournal.openFile(10, testFolder.getFile("protectedFile.dat"));
        ) {
            protectedFile.ensureCapacity(1024);

            protectedFile.putInt(0, 0x12345678);
            protectedFile.putInt(4, -0x12345678);

            assertEquals(0x12345678, protectedFile.getInt(0));
            assertEquals(-0x12345678, protectedFile.getInt(4));

            rollbackJournal.commit();

            assertEquals(0x12345678, protectedFile.getInt(0));
            assertEquals(-0x12345678, protectedFile.getInt(4));

            protectedFile.putInt(0, 0x23456789);
            protectedFile.putInt(4, -0x23456789);

            assertEquals(0x23456789, protectedFile.getInt(0));
            assertEquals(-0x23456789, protectedFile.getInt(4));

            rollbackJournal.rollback();

            assertEquals(0x12345678, protectedFile.getInt(0));
            assertEquals(-0x12345678, protectedFile.getInt(4));
        }
    }

    @Test
    public void testLong() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("RollbackJournalTest", "testLong");
                RollbackJournal rollbackJournal = new RollbackJournal(testFolder.getFile("rollback.dat"));
                ProtectedFile protectedFile = rollbackJournal.openFile(10, testFolder.getFile("protectedFile.dat"));
        ) {
            protectedFile.ensureCapacity(1024);

            protectedFile.putLong(0, 0x1234567890ABCDEFL);
            protectedFile.putLong(8, -0x1234567890ABCDEFL);

            assertEquals(0x1234567890ABCDEFL, protectedFile.getLong(0));
            assertEquals(-0x1234567890ABCDEFL, protectedFile.getLong(8));

            rollbackJournal.commit();

            assertEquals(0x1234567890ABCDEFL, protectedFile.getLong(0));
            assertEquals(-0x1234567890ABCDEFL, protectedFile.getLong(8));

            protectedFile.putLong(0, 0x234567890ABCDEF1L);
            protectedFile.putLong(8, -0x234567890ABCDEF1L);

            assertEquals(0x234567890ABCDEF1L, protectedFile.getLong(0));
            assertEquals(-0x234567890ABCDEF1L, protectedFile.getLong(8));

            rollbackJournal.rollback();

            assertEquals(0x1234567890ABCDEFL, protectedFile.getLong(0));
            assertEquals(-0x1234567890ABCDEFL, protectedFile.getLong(8));
        }
    }

    @Test
    public void testByteArray() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("RollbackJournalTest", "testByteArray");
                RollbackJournal rollbackJournal = new RollbackJournal(testFolder.getFile("rollback.dat"));
                ProtectedFile protectedFile = rollbackJournal.openFile(10, testFolder.getFile("protectedFile.dat"));
        ) {
            byte[] buffer = new byte[32];
            for (int i = 0; i < buffer.length; i++) {
                buffer[i] = (byte) i;
            }
            byte[] tempBuffer1 = new byte[16];
            byte[] tempBuffer2 = new byte[16];

            protectedFile.ensureCapacity(1024);

            protectedFile.writeBytes(0, buffer, 0, 16);
            protectedFile.writeBytes(16, buffer, 1, 16);

            protectedFile.readBytes(0, tempBuffer1, 0, 16);
            protectedFile.readBytes(16, tempBuffer2, 0, 16);

            assertArrayEquals(Arrays.copyOfRange(buffer, 0, 16), tempBuffer1);
            assertArrayEquals(Arrays.copyOfRange(buffer, 1, 17), tempBuffer2);

            rollbackJournal.commit();

            protectedFile.readBytes(0, tempBuffer1, 0, 16);
            protectedFile.readBytes(16, tempBuffer2, 0, 16);

            assertArrayEquals(Arrays.copyOfRange(buffer, 0, 16), tempBuffer1);
            assertArrayEquals(Arrays.copyOfRange(buffer, 1, 17), tempBuffer2);

            protectedFile.writeBytes(0, buffer, 15, 16);
            protectedFile.writeBytes(16, buffer, 16, 16);

            protectedFile.readBytes(0, tempBuffer1, 0, 16);
            protectedFile.readBytes(16, tempBuffer2, 0, 16);

            assertArrayEquals(Arrays.copyOfRange(buffer, 15, 31), tempBuffer1);
            assertArrayEquals(Arrays.copyOfRange(buffer, 16, 32), tempBuffer2);

            rollbackJournal.rollback();

            protectedFile.readBytes(0, tempBuffer1, 0, 16);
            protectedFile.readBytes(16, tempBuffer2, 0, 16);

            assertArrayEquals(Arrays.copyOfRange(buffer, 0, 16), tempBuffer1);
            assertArrayEquals(Arrays.copyOfRange(buffer, 1, 17), tempBuffer2);
        }
    }

    @Test
    public void testAdapter() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("RollbackJournalTest", "testAdapter");
                RollbackJournal rollbackJournal = new RollbackJournal(testFolder.getFile("rollback.dat"));
                ProtectedFile protectedFile = rollbackJournal.openFile(10, testFolder.getFile("protectedFile.dat"));
        ) {
            protectedFile.ensureCapacity(1024);

            protectedFile.put(0, 0x1234567890ABCDEFL, LongStorageAdapter.getInstance());
            protectedFile.put(8, -0x1234567890ABCDEFL, LongStorageAdapter.getInstance());

            assertEquals(0x1234567890ABCDEFL, (long) protectedFile.get(0, LongStorageAdapter.getInstance()));
            assertEquals(-0x1234567890ABCDEFL, (long) protectedFile.get(8, LongStorageAdapter.getInstance()));

            rollbackJournal.commit();

            assertEquals(0x1234567890ABCDEFL, (long) protectedFile.get(0, LongStorageAdapter.getInstance()));
            assertEquals(-0x1234567890ABCDEFL, (long) protectedFile.get(8, LongStorageAdapter.getInstance()));

            protectedFile.put(0, 0x234567890ABCDEF1L, LongStorageAdapter.getInstance());
            protectedFile.put(8, -0x234567890ABCDEF1L, LongStorageAdapter.getInstance());

            assertEquals(0x234567890ABCDEF1L, (long) protectedFile.get(0, LongStorageAdapter.getInstance()));
            assertEquals(-0x234567890ABCDEF1L, (long) protectedFile.get(8, LongStorageAdapter.getInstance()));

            rollbackJournal.rollback();

            assertEquals(0x1234567890ABCDEFL, (long) protectedFile.get(0, LongStorageAdapter.getInstance()));
            assertEquals(-0x1234567890ABCDEFL, (long) protectedFile.get(8, LongStorageAdapter.getInstance()));
        }
    }
}