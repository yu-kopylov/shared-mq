package org.sharedmq.primitives;

import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.primitives.LongStorageAdapter;
import org.sharedmq.primitives.MappedArrayList;

import java.io.IOException;

import static org.sharedmq.test.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

@Category(CommonTests.class)
public class MappedArrayListTest {
    @Test
    public void testSmoke() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("MappedArrayListTest", "testSmoke");
        ) {
            try (
                    MappedArrayList<Long> mappedArrayList1 =
                            new MappedArrayList<>(testFolder.getFile("test.dat"), LongStorageAdapter.getInstance());
                    MappedArrayList<Long> mappedArrayList2 =
                            new MappedArrayList<>(testFolder.getFile("test.dat"), LongStorageAdapter.getInstance())
            ) {
                assertEquals(0, mappedArrayList1.size());
                assertEquals(0, mappedArrayList2.size());

                // add values 0..9999 using first list
                for (int i = 0; i < 10000; i++) {
                    mappedArrayList1.add((long) i);
                }
                assertEquals(10000, mappedArrayList1.size());
                assertEquals(10000, mappedArrayList2.size());

                // add values 10..19 using second list
                for (int i = 10000; i < 20000; i++) {
                    mappedArrayList2.add((long) i);
                }
                assertEquals(20000, mappedArrayList1.size());
                assertEquals(20000, mappedArrayList2.size());

                //check all values using both lists
                for (int i = 0; i < 20000; i++) {
                    assertEquals(i, (long) mappedArrayList1.get(i));
                    assertEquals(i, (long) mappedArrayList2.get(i));
                }
            }

            // close both lists and open them again

            try (
                    MappedArrayList<Long> mappedArrayList1 =
                            new MappedArrayList<>(testFolder.getFile("test.dat"), LongStorageAdapter.getInstance());
                    MappedArrayList<Long> mappedArrayList2 =
                            new MappedArrayList<>(testFolder.getFile("test.dat"), LongStorageAdapter.getInstance())
            ) {
                assertEquals(20000, mappedArrayList1.size());
                assertEquals(20000, mappedArrayList2.size());

                //check all values using both lists
                for (int i = 0; i < 20000; i++) {
                    assertEquals(i, (long) mappedArrayList1.get(i));
                    assertEquals(i, (long) mappedArrayList2.get(i));
                }

                //set all values to negative values using first list
                for (int i = 0; i < 20000; i++) {
                    mappedArrayList1.set(i, (long) -i);
                }

                //check all values using both lists
                for (int i = 0; i < 20000; i++) {
                    assertEquals(-i, (long) mappedArrayList1.get(i));
                    assertEquals(-i, (long) mappedArrayList2.get(i));
                }
            }
        }
    }

    @Test
    public void testRemoveLast() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("MappedArrayListTest", "testRemoveLast");
                MappedArrayList<Long> mappedArrayList1 =
                        new MappedArrayList<>(testFolder.getFile("test.dat"), LongStorageAdapter.getInstance());
                MappedArrayList<Long> mappedArrayList2 =
                        new MappedArrayList<>(testFolder.getFile("test.dat"), LongStorageAdapter.getInstance())
        ) {
            assertEquals(0, mappedArrayList1.size());
            assertEquals(0, mappedArrayList2.size());

            // add values 0..9999 using first list
            for (int i = 0; i < 10000; i++) {
                mappedArrayList1.add((long) i);
            }
            assertEquals(10000, mappedArrayList1.size());
            assertEquals(10000, mappedArrayList2.size());

            // remove all values using second list
            for (int i = 0; i < 10000; i++) {
                assertEquals(9999 - i, (long) mappedArrayList2.removeLast());
            }
            assertEquals(0, mappedArrayList1.size());
            assertEquals(0, mappedArrayList2.size());

            assertThrows(
                    IllegalStateException.class,
                    "is empty",
                    mappedArrayList1::removeLast);
            assertThrows(
                    IllegalStateException.class,
                    "is empty",
                    mappedArrayList2::removeLast);
        }
    }
}
