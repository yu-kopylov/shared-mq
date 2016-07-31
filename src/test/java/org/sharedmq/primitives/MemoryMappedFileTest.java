package org.sharedmq.primitives;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@Category(CommonTests.class)
public class MemoryMappedFileTest {
    @Test
    public void testSmoke() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("MemoryMappedFileTest", "testSmoke");
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

            assertEquals(4096, mappedFile.capacity());

            mappedFile.ensureCapacity(2048);
            assertEquals(4096, mappedFile.capacity());

            mappedFile.ensureCapacity(8192);
            assertEquals(8192, mappedFile.capacity());
        }
    }
}
