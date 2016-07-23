package org.sharedmq.mapped;

import org.sharedmq.mapped.MappedByteBufferLock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * This is a small utility to check that locks are applied correctly.<br/>
 * It writes and reads variable-length messages to a the given file.<br/>
 * Each message has equal the first and the last halves.<br/>
 * It locks does not work correctly then messages that does not match this rule will be detected.
 */
public class IpcChecker {

    private static final int LockOffset = 0;
    private static final int MessageLengthOffset = LockOffset + MappedByteBufferLock.LockSize;
    private static final int SourceOffset = MessageLengthOffset + 4;
    private static final int SeqNumberOffset = SourceOffset + 8;
    private static final int MessageOffset = SeqNumberOffset + 8;

    private static final Random random = new Random();

    private static final long SourceId = System.currentTimeMillis();
    private static long seqNum = 0;

    private static final Map<Long, Long> lastSeqNumFromSource = new HashMap<>();

    public static void main(String... args) throws IOException, InterruptedException {
        if (args.length != 1) {
            System.out.println("Usage: IpcChecker <filename>");
            return;
        }

        boolean newFile = !new File(args[0]).exists();

        MappedByteBuffer buffer;
        try (
                RandomAccessFile randomAccessFile = new RandomAccessFile(args[0], "rw");
                FileChannel channel = randomAccessFile.getChannel();
        ) {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, 256 * 1024);
        }

        if (newFile) {
            buffer.putLong(LockOffset, MappedByteBufferLock.UnlockedTimestamp);
            buffer.putInt(MessageLengthOffset, 0);
            buffer.putLong(SourceOffset, 0);
            buffer.putLong(SeqNumberOffset, ++seqNum);
        }

        while (true) {
            if (readMessage(buffer)) {
                writeMessage(buffer);
            }
        }
    }

    private static void writeMessage(MappedByteBuffer buffer) throws InterruptedException {
        byte[] message = generateMessage();
        try (MappedByteBufferLock lock = new MappedByteBufferLock(buffer, LockOffset)) {
            buffer.putInt(MessageLengthOffset, message.length);
            buffer.putLong(SourceOffset, SourceId);
            buffer.putLong(SeqNumberOffset, ++seqNum);
            buffer.position(MessageOffset);
            buffer.put(message);
        }
    }

    private static byte[] generateMessage() {
        int halfLen = random.nextInt(125000);
        byte[] halfMessage = new byte[halfLen];
        random.nextBytes(halfMessage);

        byte[] res = new byte[halfLen * 2];
        for (int i = 0; i < halfLen; i++) {
            res[i] = halfMessage[i];
            res[i + halfLen] = halfMessage[i];
        }
        return res;
    }

    private static boolean readMessage(MappedByteBuffer buffer) throws InterruptedException {

        try (MappedByteBufferLock lock = new MappedByteBufferLock(buffer, LockOffset)) {
            long sourceId = buffer.getLong(SourceOffset);
            if (sourceId == SourceId) {
                return false;
            }
            long seqNum = buffer.getLong(SeqNumberOffset);
            Long prevSeqNum = lastSeqNumFromSource.get(sourceId);
            if (prevSeqNum != null && prevSeqNum == seqNum) {
                return false;
            }
            if (seqNum % 25000 == 0) {
                System.out.println("Received message #" + seqNum + " from source " + sourceId + ".");
            }
            lastSeqNumFromSource.put(sourceId, seqNum);
            int messageLength = buffer.getInt(MessageLengthOffset);
            byte[] message = new byte[messageLength];
            buffer.position(MessageOffset);
            buffer.get(message);
            if (!checkMessage(message)) {
                System.out.println(new Date() + ": Corrupted message.");
            }
            return true;
        }
    }

    private static boolean checkMessage(byte[] message) {
        int halfLength = message.length / 2;
        for (int i = 0; i < halfLength; i++) {
            if (message[i] != message[i + halfLength]) {
                return false;
            }
        }
        return true;
    }
}
