package org.sharedmq.mapped;

import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;
import org.sharedmq.test.TestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.mapped.MappedByteArrayStorage;
import org.sharedmq.mapped.MappedByteArrayStorageKey;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

@Category(CommonTests.class)
public class MappedByteArrayStorageTest {

    @Test
    public void testSmoke() throws IOException {
        try (TestFolder testFolder = new TestFolder("MappedByteArrayStorageTest", "testSmoke")) {
            try (MappedByteArrayStorage storage = new MappedByteArrayStorage(testFolder.getFile("test.dat"))) {

                byte[] originalArray1 = TestUtils.generateArray(20);

                MappedByteArrayStorageKey key1 = storage.add(originalArray1);
                assertEquals(0, key1.getRecordId());

                byte[] originalArray2 = TestUtils.generateArray(21);

                MappedByteArrayStorageKey key2 = storage.add(originalArray2);
                assertEquals(1, key2.getRecordId());

                byte[] restoredArray1 = storage.get(key1);
                byte[] restoredArray2 = storage.get(key2);

                assertTrue(Arrays.equals(originalArray1, restoredArray1));
                assertTrue(Arrays.equals(originalArray2, restoredArray2));

                assertTrue(storage.delete(key1));
                assertTrue(storage.delete(key2));

                assertNull(storage.get(key1));
                assertNull(storage.get(key2));

                assertFalse(storage.delete(key1));
                assertFalse(storage.delete(key2));

                assertNull(storage.get(key1));
                assertNull(storage.get(key2));
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    @Test
    public void testSegmentUsage() throws IOException {
        try (TestFolder testFolder = new TestFolder("MappedByteArrayStorageTest", "testSegmentUsage")) {
            try (MappedByteArrayStorage storage = new MappedByteArrayStorage(testFolder.getFile("test.dat"))) {

                // The array size is set to 2/5 of the segment size.
                // That means that segment can accommodate 2 such arrays, but cannot accommodate the 3rd.
                int arraySize = MappedByteArrayStorage.SegmentSize * 2 / 5;

                MappedByteArrayStorageKey key1 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key2 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key3 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key4 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key5 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key6 = storage.add(TestUtils.generateArray(arraySize));

                assertEquals(0, key1.getSegmentNumber());
                assertEquals(0, key2.getSegmentNumber());
                assertEquals(1, key3.getSegmentNumber());
                assertEquals(1, key4.getSegmentNumber());
                assertEquals(2, key5.getSegmentNumber());
                assertEquals(2, key6.getSegmentNumber());

                // lets delete an array from the middle segment, and then use that space
                assertTrue(storage.delete(key4));
                MappedByteArrayStorageKey key4_2 = storage.add(TestUtils.generateArray(arraySize));
                assertEquals(1, key4_2.getSegmentNumber());
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }

    @Test
    public void testRecordNumbers() throws IOException {
        try (TestFolder testFolder = new TestFolder("MappedByteArrayStorageTest", "testRecordNumbers")) {
            try (MappedByteArrayStorage storage = new MappedByteArrayStorage(testFolder.getFile("test.dat"))) {

                // The array size is set to 2/5 of the segment size.
                // That means that segment can accommodate 2 such arrays, but cannot accommodate the 3rd.
                int arraySize = MappedByteArrayStorage.SegmentSize * 2 / 5;

                MappedByteArrayStorageKey key1 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key2 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key3 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key4 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key5 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key6 = storage.add(TestUtils.generateArray(arraySize));

                assertEquals(0, key1.getRecordNumber());
                assertEquals(1, key2.getRecordNumber());
                assertEquals(0, key3.getRecordNumber());
                assertEquals(1, key4.getRecordNumber());
                assertEquals(0, key5.getRecordNumber());
                assertEquals(1, key6.getRecordNumber());

                assertEquals(0, key1.getRecordId());
                assertEquals(1, key2.getRecordId());
                assertEquals(2, key3.getRecordId());
                assertEquals(3, key4.getRecordId());
                assertEquals(4, key5.getRecordId());
                assertEquals(5, key6.getRecordId());

                // lets delete values in the middle, and then replace them with new ones
                assertTrue(storage.delete(key2));
                assertTrue(storage.delete(key3));
                assertTrue(storage.delete(key4));
                assertTrue(storage.delete(key5));

                MappedByteArrayStorageKey key2_2 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key3_2 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key4_2 = storage.add(TestUtils.generateArray(arraySize));
                MappedByteArrayStorageKey key5_2 = storage.add(TestUtils.generateArray(arraySize));

                assertEquals(1, key2_2.getRecordNumber());
                assertEquals(0, key3_2.getRecordNumber());
                assertEquals(1, key4_2.getRecordNumber());
                assertEquals(0, key5_2.getRecordNumber());

                assertEquals(6, key2_2.getRecordId());
                assertEquals(7, key3_2.getRecordId());
                assertEquals(8, key4_2.getRecordId());
                assertEquals(9, key5_2.getRecordId());

            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }
}
