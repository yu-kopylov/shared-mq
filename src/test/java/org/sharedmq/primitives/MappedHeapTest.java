package org.sharedmq.primitives;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(CommonTests.class)
public class MappedHeapTest {
    @Test
    public void testAddPeekAndPoll() throws IOException {
        try (TestFolder testFolder = new TestFolder("MappedHeapTest", "testAddPeekAndPoll")) {

            // ascending order
            try (
                    MappedHeap<Long> heap = new MappedHeap<>(
                            testFolder.getFile("test-asc.dat"),
                            LongStorageAdapter.getInstance(),
                            Long::compare)
            ) {
                assertNull(heap.peek());
                for (long i = 0; i < 10000; i++) {
                    heap.add(i);
                    assertEquals(0, (long) heap.peek());
                }
                for (long i = 0; i < 10000; i++) {
                    assertEquals(i, (long) heap.poll());
                }
                assertNull(heap.peek());
                assertNull(heap.poll());
            }

            // descending order
            try (
                    MappedHeap<Long> heap = new MappedHeap<>(
                            testFolder.getFile("test-desc.dat"),
                            LongStorageAdapter.getInstance(),
                            Long::compare)
            ) {
                assertNull(heap.peek());
                for (long i = 0; i < 10000; i++) {
                    long value = 9999 - i;
                    heap.add(value);
                    assertEquals(value, (long) heap.peek());
                }
                for (long i = 0; i < 10000; i++) {
                    assertEquals(i, (long) heap.poll());
                }
                assertNull(heap.peek());
                assertNull(heap.poll());
            }

            // random order
            try (
                    MappedHeap<Long> heap = new MappedHeap<>(
                            testFolder.getFile("test-random.dat"),
                            LongStorageAdapter.getInstance(),
                            Long::compare)
            ) {
                assertNull(heap.peek());
                long minValue = Long.MAX_VALUE;
                for (long i = 0; i < 10000; i++) {
                    long value = i * 7039 % 10000;
                    minValue = Math.min(minValue, value);
                    heap.add(value);
                    assertEquals(minValue, (long) heap.peek());
                }
                for (long i = 0; i < 10000; i++) {
                    assertEquals(i, (long) heap.poll());
                }
                assertNull(heap.peek());
                assertNull(heap.poll());
            }
        }
    }

    @Test
    public void testRemoveAt() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("MappedHeapTest", "testRemoveAt");
                MappedHeap<Long> heap = new MappedHeap<>(
                        testFolder.getFile("test-asc.dat"),
                        LongStorageAdapter.getInstance(),
                        Long::compare)
        ) {
            // removing all elements by removing last element
            heap.add(10001L);
            heap.add(10002L);
            heap.add(10003L);
            heap.add(10004L);

            heap.removeAt(3);
            heap.removeAt(2);
            heap.removeAt(1);
            heap.removeAt(0);

            assertNull(heap.peek());

            // removing all elements starting in the center
            heap.add(10001L);
            heap.add(10002L);
            heap.add(10003L);
            heap.add(10004L);

            heap.removeAt(1);
            heap.removeAt(1);
            heap.removeAt(1);
            heap.removeAt(0);

            assertNull(heap.peek());
        }
    }

    @Test
    public void testRecordRelocatedEvent() throws IOException {
        try (
                TestFolder testFolder = new TestFolder("MappedHeapTest", "testRecordRelocatedEvent");
                MappedHeap<Long> heap = new MappedHeap<>(
                        testFolder.getFile("test-asc.dat"),
                        LongStorageAdapter.getInstance(),
                        Long::compare)
        ) {
            List<String> events = new ArrayList<>();
            heap.register((value, index) -> events.add(value + "->" + index));

            // test add

            assertEquals(0, heap.add(10001L));
            assertEquals(1, heap.add(10002L));
            assertEquals(2, heap.add(10003L));

            assertEquals(0, events.size());
            assertEquals(0, heap.add(10000L));
            assertEquals(Arrays.asList("10002->3", "10001->1"), events);

            // test poll

            heap.clear();
            events.clear();

            assertEquals(0, heap.add(10001L));
            assertEquals(1, heap.add(10002L));
            assertEquals(2, heap.add(10003L));
            assertEquals(3, heap.add(10004L));

            assertEquals(0, events.size());

            events.clear();
            assertEquals(10001L, (long) heap.poll());
            assertEquals(Arrays.asList("10002->0", "10004->1"), events);

            events.clear();
            assertEquals(10002L, (long) heap.poll());
            assertEquals(Collections.singletonList("10003->0"), events);

            events.clear();
            assertEquals(10003L, (long) heap.poll());
            assertEquals(Collections.singletonList("10004->0"), events);

            events.clear();
            assertEquals(10004L, (long) heap.poll());
            assertEquals(0, events.size());

            events.clear();
            assertNull(heap.poll());
            assertEquals(0, events.size());

            // test remove at (moving down)

            heap.clear();
            events.clear();

            assertEquals(0, heap.add(10001L));
            assertEquals(1, heap.add(10002L));
            assertEquals(2, heap.add(10003L));
            assertEquals(3, heap.add(10004L));
            assertEquals(4, heap.add(10005L));
            assertEquals(5, heap.add(10006L));
            assertEquals(6, heap.add(10007L));
            assertEquals(7, heap.add(10008L));

            assertEquals(0, events.size());
            heap.removeAt(1);
            assertEquals(Arrays.asList("10004->1", "10008->3"), events);

            // test remove at (moving up)

            heap.clear();
            events.clear();

            assertEquals(0, heap.add(10001L));
            assertEquals(1, heap.add(10004L));
            assertEquals(2, heap.add(10002L));
            assertEquals(3, heap.add(10005L));
            assertEquals(4, heap.add(10006L));
            assertEquals(5, heap.add(10003L));

            assertEquals(0, events.size());
            heap.removeAt(4);
            assertEquals(Arrays.asList("10004->4", "10003->1"), events);
        }
    }
}
