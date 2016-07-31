package org.sharedmq.primitives;

import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;
import org.sharedmq.test.TestUtils;
import org.sharedmq.util.IOUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

import static org.sharedmq.test.TestUtils.assertThrows;
import static org.junit.Assert.*;

@Category(CommonTests.class)
public class MappedByteArrayStorageSegmentTest {

    @Test
    public void testMaxSizeArray() throws IOException {

        final int segmentSize = 1000;
        final int segmentNumber = 1234;
        final int maxRecordSize = segmentSize
                - MappedByteArrayStorageSegmentHeader.getStorageAdapter().getRecordSize()
                - MappedByteArrayStorageSegment.IndexRecordSize;

        try (
                TestFolder testFolder = new TestFolder("MappedByteArrayStorageSegmentTest", "testMaxSizeArray");
                MemoryMappedFile mappedFile = new MemoryMappedFile(testFolder.getFile("test.dat"))
        ) {
            mappedFile.ensureCapacity(segmentSize + 1);

            // testing the segment with the minimum and maximum possible offsets within the buffer
            for (int segmentOffset = 0; segmentOffset < 2; segmentOffset++) {
                MappedByteArrayStorageSegment segment
                        = MappedByteArrayStorageSegment.create(mappedFile, segmentNumber, segmentOffset, segmentSize);

                // repeat test 2 times to check that segment is reusable
                for (int i = 0; i < 2; i++) {
                    assertThrows(
                            IOException.class,
                            "does not have enough free space",
                            () -> segment.addArray(12345, new byte[maxRecordSize + 1]));

                    byte[] originalArray = TestUtils.generateArray(maxRecordSize);

                    MappedByteArrayStorageKey key = segment.addArray(i, originalArray);
                    assertEquals(segmentNumber, key.getSegmentNumber());
                    assertEquals(0, key.getRecordNumber());
                    assertEquals(i, key.getRecordId());

                    assertThrows(
                            IOException.class,
                            "does not have enough free space",
                            () -> segment.addArray(12345, new byte[0]));

                    byte[] restoredArray = segment.getArray(key);
                    assertTrue(Arrays.equals(originalArray, restoredArray));

                    // check that first delete deletes record
                    assertTrue(segment.deleteArray(key));
                    assertNull(segment.getArray(key));

                    // check that second delete returns false
                    assertFalse(segment.deleteArray(key));
                    assertNull(segment.getArray(key));
                }
            }
        }
    }

    @Test
    public void testGarbageCollection() throws IOException {

        final int segmentSize = 1000;
        final int segmentNumber = 1234;

        try (
                TestFolder testFolder = new TestFolder("MappedByteArrayStorageSegmentTest", "testGarbageCollection");
                MemoryMappedFile mappedFile = new MemoryMappedFile(testFolder.getFile("test.dat"))
        ) {
            mappedFile.ensureCapacity(segmentSize + 1);

            // testing the segment with the minimum and the maximum possible offsets within the buffer
            for (int segmentOffset = 0; segmentOffset <= 1; segmentOffset++) {
                MappedByteArrayStorageSegment segment
                        = MappedByteArrayStorageSegment.create(mappedFile, segmentNumber, segmentOffset, segmentSize);

                byte[] array1 = TestUtils.generateArray(101);
                byte[] array2 = TestUtils.generateArray(102);
                byte[] array3 = TestUtils.generateArray(103);
                byte[] array4 = TestUtils.generateArray(104);
                byte[] array5 = TestUtils.generateArray(105);
                byte[] array6 = TestUtils.generateArray(106);

                MappedByteArrayStorageKey key1 = segment.addArray(10001, array1);
                MappedByteArrayStorageKey key2 = segment.addArray(10002, array2);
                MappedByteArrayStorageKey key3 = segment.addArray(10003, array3);
                MappedByteArrayStorageKey key4 = segment.addArray(10004, array4);
                MappedByteArrayStorageKey key5 = segment.addArray(10005, array5);
                MappedByteArrayStorageKey key6 = segment.addArray(10006, array6);

                segment.deleteArray(key2);
                segment.deleteArray(key4);
                segment.deleteArray(key6);

                byte[] array2_2 = TestUtils.generateArray(202);
                MappedByteArrayStorageKey key2_2 = segment.addArray(20002, array2_2);

                final int releasedSpace = 102 + 104 + 106;
                final int allocatedSpace = 101 + 102 + 103 + 104 + 105 + 106 + 202;
                final int usedSpace = allocatedSpace - releasedSpace;

                MappedByteArrayStorageSegmentHeader headerBefore = segment.getHeader();

                assertEquals(6, headerBefore.getIndexRecordCount());
                assertEquals(3 - 1, headerBefore.getFreeRecordCount());
                assertEquals(4, headerBefore.getLastNonFreeRecord());
                assertEquals(allocatedSpace, headerBefore.getAllocatedSpace());
                assertEquals(releasedSpace, headerBefore.getReleasedSpace());

                segment.collectGarbage();
                MappedByteArrayStorageSegmentHeader headerAfter = segment.getHeader();

                assertEquals(5, headerAfter.getIndexRecordCount());
                assertEquals(1, headerAfter.getFreeRecordCount());
                assertEquals(4, headerAfter.getLastNonFreeRecord());
                assertEquals(headerBefore.getUnallocatedSpace()
                                + releasedSpace
                                + MappedByteArrayStorageIndexRecord.getStorageAdapter().getRecordSize()
                                + 4 /*Free Record Index Heap Entry*/
                        , headerAfter.getUnallocatedSpace());
                assertEquals(usedSpace, headerAfter.getAllocatedSpace());
                assertEquals(0, headerAfter.getReleasedSpace());

                assertTrue(Arrays.equals(array1, segment.getArray(key1)));
                assertNull(segment.getArray(key2));
                assertTrue(Arrays.equals(array3, segment.getArray(key3)));
                assertNull(segment.getArray(key4));
                assertTrue(Arrays.equals(array5, segment.getArray(key5)));
                assertNull(segment.getArray(key6));
                assertTrue(Arrays.equals(array2_2, segment.getArray(key2_2)));
            }
        }
    }

    @Test
    public void testFreeRecordsHeap() throws IOException {

        // lets make segment big enough, to avoid uncalled garbage collection when arrays are added
        final int segmentSize = 10000;
        final int segmentNumber = 1234;

        try (
                TestFolder testFolder = new TestFolder("MappedByteArrayStorageSegmentTest", "testFreeRecordsHeap");
                MemoryMappedFile mappedFile = new MemoryMappedFile(testFolder.getFile("test.dat"))
        ) {
            mappedFile.ensureCapacity(segmentSize + 1);

            // testing the segment with the minimum and the maximum possible offsets within the buffer
            for (int segmentOffset = 0; segmentOffset < 2; segmentOffset++) {
                MappedByteArrayStorageSegment segment
                        = MappedByteArrayStorageSegment.create(mappedFile, segmentNumber, segmentOffset, segmentSize);

                byte[] array = TestUtils.generateArray(20);

                // creating some records for test
                MappedByteArrayStorageKey key0 = segment.addArray(10000, array);
                MappedByteArrayStorageKey key1 = segment.addArray(10001, array);
                MappedByteArrayStorageKey key2 = segment.addArray(10002, array);
                MappedByteArrayStorageKey key3 = segment.addArray(10003, array);
                MappedByteArrayStorageKey key4 = segment.addArray(10004, array);
                MappedByteArrayStorageKey key5 = segment.addArray(10005, array);
                MappedByteArrayStorageKey key6 = segment.addArray(10006, array);

                assertEquals(0, key0.getRecordNumber());
                assertEquals(1, key1.getRecordNumber());
                assertEquals(2, key2.getRecordNumber());
                assertEquals(3, key3.getRecordNumber());
                assertEquals(4, key4.getRecordNumber());
                assertEquals(5, key5.getRecordNumber());
                assertEquals(6, key6.getRecordNumber());

                // removing records in the ascending order except for the first and the last records
                assertTrue(segment.deleteArray(key1));
                assertTrue(segment.deleteArray(key2));
                assertTrue(segment.deleteArray(key3));
                assertTrue(segment.deleteArray(key4));
                assertTrue(segment.deleteArray(key5));

                // reusing existing record numbers and adding a new one
                MappedByteArrayStorageKey key1_2 = segment.addArray(20001, array);
                MappedByteArrayStorageKey key2_2 = segment.addArray(20002, array);
                MappedByteArrayStorageKey key3_2 = segment.addArray(20003, array);
                MappedByteArrayStorageKey key4_2 = segment.addArray(20004, array);
                MappedByteArrayStorageKey key5_2 = segment.addArray(20005, array);
                MappedByteArrayStorageKey key7_2 = segment.addArray(20007, array);

                assertEquals(1, key1_2.getRecordNumber());
                assertEquals(2, key2_2.getRecordNumber());
                assertEquals(3, key3_2.getRecordNumber());
                assertEquals(4, key4_2.getRecordNumber());
                assertEquals(5, key5_2.getRecordNumber());
                assertEquals(7, key7_2.getRecordNumber());

                // removing records in the in the descending order except for the first and the last records
                assertTrue(segment.deleteArray(key6));
                assertTrue(segment.deleteArray(key5_2));
                assertTrue(segment.deleteArray(key4_2));
                assertTrue(segment.deleteArray(key3_2));
                assertTrue(segment.deleteArray(key2_2));
                assertTrue(segment.deleteArray(key1_2));

                // reusing existing record numbers and adding a new one
                MappedByteArrayStorageKey key1_3 = segment.addArray(30001, array);
                MappedByteArrayStorageKey key2_3 = segment.addArray(30002, array);
                MappedByteArrayStorageKey key3_3 = segment.addArray(30003, array);
                MappedByteArrayStorageKey key4_3 = segment.addArray(30004, array);
                MappedByteArrayStorageKey key5_3 = segment.addArray(30005, array);
                MappedByteArrayStorageKey key6_3 = segment.addArray(30006, array);
                MappedByteArrayStorageKey key8_3 = segment.addArray(30008, array);

                assertEquals(1, key1_3.getRecordNumber());
                assertEquals(2, key2_3.getRecordNumber());
                assertEquals(3, key3_3.getRecordNumber());
                assertEquals(4, key4_3.getRecordNumber());
                assertEquals(5, key5_3.getRecordNumber());
                assertEquals(6, key6_3.getRecordNumber());
                assertEquals(8, key8_3.getRecordNumber());

                // removing records in the special order,
                // so that the truncated index record number will be in the beginning
                assertTrue(segment.deleteArray(key1_3));
                assertTrue(segment.deleteArray(key4_3));
                assertTrue(segment.deleteArray(key2_3));
                assertTrue(segment.deleteArray(key5_3));
                assertTrue(segment.deleteArray(key6_3));
                assertTrue(segment.deleteArray(key7_2));
                assertTrue(segment.deleteArray(key8_3));

                // lets do this test part with and without garbage collection
                if (segmentOffset == 0) {
                    segment.collectGarbage();
                }

                // reusing deleted record numbers
                MappedByteArrayStorageKey key1_4 = segment.addArray(40001, array);
                MappedByteArrayStorageKey key2_4 = segment.addArray(40002, array);
                MappedByteArrayStorageKey key4_4 = segment.addArray(40004, array);
                MappedByteArrayStorageKey key5_4 = segment.addArray(40005, array);
                MappedByteArrayStorageKey key6_4 = segment.addArray(40006, array);
                MappedByteArrayStorageKey key7_4 = segment.addArray(40007, array);
                MappedByteArrayStorageKey key8_4 = segment.addArray(40008, array);

                assertEquals(1, key1_4.getRecordNumber());
                assertEquals(2, key2_4.getRecordNumber());
                assertEquals(4, key4_4.getRecordNumber());
                assertEquals(5, key5_4.getRecordNumber());
                assertEquals(6, key6_4.getRecordNumber());
                assertEquals(7, key7_4.getRecordNumber());
                assertEquals(8, key8_4.getRecordNumber());
            }
        }
    }

    @Test
    public void testRecordIdCheck() throws IOException {
        // lets make segment big enough, to avoid uncalled garbage collection when arrays are added
        final int segmentSize = 1000;
        final int segmentNumber = 1234;

        try (
                TestFolder testFolder = new TestFolder("MappedByteArrayStorageSegmentTest", "testRecordIdCheck");
                MemoryMappedFile mappedFile = new MemoryMappedFile(testFolder.getFile("test.dat"))
        ) {
            mappedFile.ensureCapacity(segmentSize + 1);

            // testing the segment with the minimum and the maximum possible offsets within the buffer
            for (int segmentOffset = 0; segmentOffset < 2; segmentOffset++) {
                MappedByteArrayStorageSegment segment
                        = MappedByteArrayStorageSegment.create(mappedFile, segmentNumber, segmentOffset, segmentSize);

                byte[] array = TestUtils.generateArray(20);

                MappedByteArrayStorageKey key = segment.addArray(12345, array);
                assertTrue(Arrays.equals(array, segment.getArray(key)));

                MappedByteArrayStorageKey wrongKey = new MappedByteArrayStorageKey(
                        key.getSegmentNumber(),
                        key.getRecordNumber(),
                        key.getRecordId() + 1000
                );
                assertNull(segment.getArray(wrongKey));
                assertFalse(segment.deleteArray(wrongKey));

                MappedByteArrayStorageKey correctKey = new MappedByteArrayStorageKey(
                        key.getSegmentNumber(),
                        key.getRecordNumber(),
                        key.getRecordId()
                );
                assertTrue(segment.deleteArray(correctKey));
            }
        }
    }

    @Test
    public void testRead() throws IOException {

        // lets make segment big enough, to avoid uncalled garbage collection when arrays are added
        final int segmentSize = 1000;
        final int segmentNumber = 1234;

        try (
                TestFolder testFolder = new TestFolder("MappedByteArrayStorageSegmentTest", "testRead");
                MemoryMappedFile mappedFile = new MemoryMappedFile(testFolder.getFile("test.dat"))
        ) {
            mappedFile.ensureCapacity(segmentSize + 1);

            // testing the segment with the minimum and the maximum possible offsets within the buffer
            for (int segmentOffset = 0; segmentOffset < 2; segmentOffset++) {
                MappedByteArrayStorageSegment segment1
                        = MappedByteArrayStorageSegment.create(mappedFile, segmentNumber, segmentOffset, segmentSize);

                // lets make some operations to make segment non-trivial
                byte[] array1 = TestUtils.generateArray(20);
                byte[] array2 = TestUtils.generateArray(20);
                byte[] array3 = TestUtils.generateArray(20);

                MappedByteArrayStorageKey key1 = segment1.addArray(10001, array1);
                MappedByteArrayStorageKey key2 = segment1.addArray(10002, array2);
                MappedByteArrayStorageKey key3 = segment1.addArray(10003, array3);

                segment1.deleteArray(key2);

                MappedByteArrayStorageSegment segment2
                        = MappedByteArrayStorageSegment.read(mappedFile, segmentNumber, segmentOffset, segmentSize);

                assertTrue(Arrays.equals(array1, segment2.getArray(key1)));
                assertNull(segment2.getArray(key2));
                assertTrue(Arrays.equals(array3, segment2.getArray(key3)));
            }
        }
    }
}
