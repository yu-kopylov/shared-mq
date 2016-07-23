package org.sharedmq.primitives;

import java.io.IOException;

/**
 * A listener that receives events about new record positions from the {@link MappedHeap}.
 */
public interface MappedHeapEventListener<TRecord> {
    void recordRelocated(TRecord record, int recordIndex) throws IOException;
}
