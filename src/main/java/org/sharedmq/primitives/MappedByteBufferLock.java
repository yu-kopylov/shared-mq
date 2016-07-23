package org.sharedmq.primitives;

import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;

/**
 * A class that represent a lock within a {@link java.nio.MappedByteBuffer}, that
 * synchronize access to all open memory-mapped files between threads and processes.<br/>
 * <br/>
 * Since java does not have an API to synchronize access to memory-mapped files across different processes,
 * we have to rely on the {@link Unsafe} class, which is not very standard.<br/>
 * <br/>
 * It should not be a problem, if the file contains some garbage data before the first lock.<br/>
 * The stale lock detection should deal with that.
 */
public class MappedByteBufferLock implements AutoCloseable {

    public static final int LockSize = 8;

    public static final long UnlockedTimestamp = 0;

    // MappedQueueService operations are fast, we expect each operation to be completed in less than 1ms.
    // So, we are using the minimal recheck interval.
    // A more advanced strategy with a variable interval can be used to reduce contention.
    // For now, this simple solution is sufficient.
    private static final int RecheckInterval = 1;

    // We assume that all operations can be finished within 5 minutes.
    // At the same time, 5 minutes is a short enough time for a resolution of a stale-lock problem.
    private static final int MaxLockDuration = 5 * 60 * 1000;

    private static final Unsafe unsafe;

    private final MappedByteBuffer buffer;
    private final int offset;
    private final long timestamp;

    static {
        try {
            Field theUnsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafeField.setAccessible(true);
            unsafe = (Unsafe) theUnsafeField.get(null);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException("Failed to get instance of Unsafe class.", e);
        }
    }

    public MappedByteBufferLock(MappedByteBuffer buffer, int offset) throws InterruptedException {
        this.buffer = buffer;
        this.offset = offset;

        DirectBuffer directBuffer = (DirectBuffer) buffer;
        long address = directBuffer.address();

        long timestamp = System.currentTimeMillis();

        while (!unsafe.compareAndSwapLong(null, address + offset, UnlockedTimestamp, timestamp)) {

            long oldTimestamp = unsafe.getLongVolatile(null, address + offset);

            if (oldTimestamp == UnlockedTimestamp) {
                // Someone just released the lock.
                // Lets try to lock again.
            } else if (oldTimestamp > timestamp) {
                // If lock timestamp is from the future, then set it to the current time.
                // The compareAndSwapInt method will make sure,
                // that we will not overwrite concurrent changes.
                unsafe.compareAndSwapLong(null, address + offset, oldTimestamp, timestamp);
            } else if (oldTimestamp + MaxLockDuration < timestamp) {
                // If lock is obsolete, than we forcefully release it.
                // The compareAndSwapInt method will make sure,
                // that we will not overwrite concurrent changes.
                unsafe.compareAndSwapLong(null, address + offset, oldTimestamp, UnlockedTimestamp);
            } else {
                Thread.sleep(RecheckInterval);
                timestamp = System.currentTimeMillis();
            }
        }

        //Lets remember the timestamp that was used for lock.
        this.timestamp = timestamp;

        // Now we obtained the lock.
        // Lets make sure that we have fresh data.
        // The full fence should not be necessary here, but we want to be extra sure.
        unsafe.fullFence();
    }

    @Override
    public void close() {

        DirectBuffer directBuffer = (DirectBuffer) buffer;
        long address = directBuffer.address();

        // Before releasing the lock, lets make sure that all our data available to other processes.
        // The full fence should not be necessary here, but we want to be extra sure.
        unsafe.fullFence();


        // If we were slow, then another thread could change or forcefully release our lock.
        // So we are checking that we are releasing our own lock.
        if (!unsafe.compareAndSwapLong(null, address + offset, timestamp, UnlockedTimestamp)) {

            long currentTimestamp = unsafe.getLongVolatile(null, address + offset);

            if (currentTimestamp == UnlockedTimestamp) {
                // Some one just released the lock for us.
            } else if (currentTimestamp <= timestamp) {
                // Time changed backwards.
                // It is unknown if some other thread has a lock.
                // We hope that we were fast this time, and lock still belongs to us.
                unsafe.putLongVolatile(null, address + offset, UnlockedTimestamp);
            } else {
                // Someone taken the lock already.
                // We cannot release it. It is the responsibility of the thread that taken it.
            }
        }
    }
}
