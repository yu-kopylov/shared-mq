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
public class MemoryMappedFileTest {
    @Test
    public void testDataTypes() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("MemoryMappedFileTest", "testDataTypes");
                MemoryMappedFile mappedFile = new MemoryMappedFile(testFolder.getFile("test.dat"))
        ) {
            mappedFile.ensureCapacity(4096);

            // test int
            mappedFile.putInt(0, 0x12345678);
            mappedFile.putInt(4, -0x12345678);
            assertEquals(0x12345678, mappedFile.getInt(0));
            assertEquals(-0x12345678, mappedFile.getInt(4));

            // test long
            mappedFile.putLong(0, 0x1234567890ABCDEFL);
            mappedFile.putLong(8, -0x1234567890ABCDEFL);
            assertEquals(0x1234567890ABCDEFL, mappedFile.getLong(0));
            assertEquals(-0x1234567890ABCDEFL, mappedFile.getLong(8));

            // test byte array
            byte[] buffer = new byte[17];
            for (int i = 0; i < buffer.length; i++) {
                buffer[i] = (byte) i;
            }
            byte[] tempBuffer1 = new byte[16];
            byte[] tempBuffer2 = new byte[16];

            mappedFile.writeBytes(0, buffer, 0, 16);
            mappedFile.writeBytes(16, buffer, 1, 16);

            mappedFile.readBytes(0, tempBuffer1, 0, 16);
            mappedFile.readBytes(16, tempBuffer2, 0, 16);

            assertArrayEquals(Arrays.copyOfRange(buffer, 0, 16), tempBuffer1);
            assertArrayEquals(Arrays.copyOfRange(buffer, 1, 17), tempBuffer2);

            // test with adapter
            mappedFile.put(0, 0x1234567890ABCDEFL, LongStorageAdapter.getInstance());
            mappedFile.put(8, -0x1234567890ABCDEFL, LongStorageAdapter.getInstance());
            assertEquals(0x1234567890ABCDEFL, (long) mappedFile.get(0, LongStorageAdapter.getInstance()));
            assertEquals(-0x1234567890ABCDEFL, (long) mappedFile.get(8, LongStorageAdapter.getInstance()));
        }
    }

    @Test
    public void testCapacity() throws IOException {
        try (TestFolder testFolder = new TestFolder("MemoryMappedFileTest", "testCapacity")) {

            // new file
            try (MemoryMappedFile mappedFile = new MemoryMappedFile(testFolder.getFile("test.dat"))) {

                assertEquals(0, mappedFile.length());
                assertEquals(0, mappedFile.capacity());

                mappedFile.ensureCapacity(4096);

                assertEquals(4096, mappedFile.length());
                assertEquals(4096, mappedFile.capacity());

                mappedFile.ensureCapacity(2048);
                assertEquals(4096, mappedFile.length());
                assertEquals(4096, mappedFile.capacity());

                mappedFile.ensureCapacity(8192);
                assertEquals(8192, mappedFile.length());
                assertEquals(8192, mappedFile.capacity());
            }

            // reopened file
            try (MemoryMappedFile mappedFile = new MemoryMappedFile(testFolder.getFile("test.dat"))) {

                assertEquals(8192, mappedFile.length());
                assertEquals(0, mappedFile.capacity());

                mappedFile.ensureCapacity(4096);

                assertEquals(8192, mappedFile.length());
                assertEquals(4096, mappedFile.capacity());

                mappedFile.ensureCapacity(2048);
                assertEquals(8192, mappedFile.length());
                assertEquals(4096, mappedFile.capacity());

                mappedFile.ensureCapacity(8192);
                assertEquals(8192, mappedFile.length());
                assertEquals(8192, mappedFile.capacity());
            }
        }
    }
}
