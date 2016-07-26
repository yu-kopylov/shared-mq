package org.sharedmq.primitives;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A heap structure mapped to a file.<br/>
 * Null is not an allowed value, to avoid confusion with empty {@link #poll()} result.<br/>
 * <br/>
 * This class is not thread-safe.
 */
public class MappedHeap<TRecord> implements Closeable {

    private final List<MappedHeapEventListener<TRecord>> eventListeners = new ArrayList<>();

    private MappedArrayList<TRecord> list;
    private final Comparator<TRecord> comparator;


    public MappedHeap(File file, StorageAdapter<TRecord> adapter, Comparator<TRecord> comparator) throws IOException {
        this.list = new MappedArrayList<>(file, adapter);
        this.comparator = comparator;
    }

    @Override
    public void close() throws IOException {
        list.close();
    }

    public void register(MappedHeapEventListener<TRecord> listener) {
        eventListeners.add(listener);
    }

    public void clear() {
        list.clear();
    }

    public int size() {
        return list.size();
    }

    public int add(TRecord record) throws IOException {

        if (record == null) {
            throw new IllegalArgumentException("The record cannot be null.");
        }

        int recordIndex = list.size();
        list.add(record);
        return moveUp(recordIndex);
    }

    public TRecord peek() throws IOException {
        if (list.size() == 0) {
            return null;
        }
        return list.get(0);
    }

    public TRecord poll() throws IOException {
        int size = list.size();

        if (size == 0) {
            return null;
        }
        if (size == 1) {
            return list.removeLast();
        }

        TRecord firstRecord = list.get(0);
        TRecord lastRecord = list.removeLast();

        list.set(0, lastRecord);

        int newLastRecordIndex = moveDown(0);
        recordRelocated(lastRecord, newLastRecordIndex);

        return firstRecord;
    }

    public void removeAt(int recordIndex) throws IOException {

        int originalHeapSize = list.size();
        TRecord lastRecord = list.removeLast();

        if (recordIndex == originalHeapSize - 1) {
            // removed element from the end of the heap
            return;
        }

        list.set(recordIndex, lastRecord);

        int newLastRecordIndex = moveDown(recordIndex);
        newLastRecordIndex = moveUp(newLastRecordIndex);

        recordRelocated(lastRecord, newLastRecordIndex);
    }


    private int moveUp(int recordIndex) throws IOException {
        TRecord record = list.get(recordIndex);
        while (recordIndex > 0) {
            int parentIndex = (recordIndex - 1) / 2;
            TRecord parentRecord = list.get(parentIndex);

            if (comparator.compare(record, parentRecord) >= 0) {
                break;
            }

            list.set(parentIndex, record);
            list.set(recordIndex, parentRecord);

            recordRelocated(parentRecord, recordIndex);

            recordIndex = parentIndex;
        }
        return recordIndex;
    }

    private int moveDown(int recordIndex) throws IOException {
        TRecord record = list.get(recordIndex);
        int size = list.size();
        while (recordIndex * 2 + 1 < size) {

            int childIndex = recordIndex * 2 + 1;
            TRecord child = list.get(childIndex);

            {
                int childIndex2 = recordIndex * 2 + 2;
                if (childIndex2 < size) {
                    TRecord child2 = list.get(childIndex2);
                    if (comparator.compare(child2, child) < 0) {
                        child = child2;
                        childIndex = childIndex2;
                    }
                }
            }

            if (comparator.compare(record, child) <= 0) {
                break;
            }

            list.set(childIndex, record);
            list.set(recordIndex, child);

            recordRelocated(child, recordIndex);

            recordIndex = childIndex;
        }

        return recordIndex;
    }

    private void recordRelocated(TRecord record, int recordIndex) throws IOException {
        for (MappedHeapEventListener<TRecord> eventListener : eventListeners) {
            eventListener.recordRelocated(record, recordIndex);
        }
    }
}
