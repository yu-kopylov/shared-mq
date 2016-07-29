package org.sharedmq;

import com.google.common.base.Stopwatch;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * The utility that allow to test performance and inter-process safety of the message queue.<br/>
 * <br/>
 * Messages that are sent to the queue has variable length an matching left and right part.<br/>
 * If locks does not work correctly, then corrupted messages would be detected.
 */
public class QueueTester {

    private static final String SendMode = "-send";
    private static final String ReceiveMode = "-receive";
    private static final long visibilityTimeout = 15000;
    private static final long retentionPeriod = 60000;
    private static final Random random = new Random();
    private static final int MaxDelay = 10;
    private static final int MaxMessageLength = 128;
    // MaxMessagesCount depends on MaxMessageLength and the maximum content storage size which is 2GB.
    private static final int MaxMessagesCount = 10000000;
    private static final String Alphabet = "01234567890!@#$%^&*()_+ abcdefghijklmnopqrstuvwxyz|ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final long BatchSize = 1000000;

    private static volatile boolean stopped;

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log("Stopping...");
            stopped = true;
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        System.out.println("**************************************");
        System.out.println("*** Press Ctrl-C to stop the test. ***");
        System.out.println("**************************************");

        String mode = args[0];
        switch (mode) {
            case SendMode:
                send(args[1]);
                break;
            case ReceiveMode:
                receive(args[1]);
                break;
            default:
                printUsage();
                System.exit(1);
        }
    }

    private static void send(String folder) throws IOException, InterruptedException {

        log("Sender started.");

        SharedMessageQueue queue = SharedMessageQueue.createQueue(new File(folder), visibilityTimeout, retentionPeriod);

        log("Queue opened.");

        long sentMessages = 0;
        Stopwatch totalTime = Stopwatch.createStarted();
        Stopwatch batchTime = Stopwatch.createStarted();


        while (!stopped) {

            while (!stopped && queue.size() >= MaxMessagesCount) {
                log("The queue already has " + MaxMessagesCount + " messages. Waiting for some space to be freed.");
                Thread.sleep(5000);
            }

            //todo: check how delay affects performance
            queue.push(random.nextInt(MaxDelay + 1), generateMessage());
            sentMessages++;
            if (sentMessages % BatchSize == 0) {

                long totalTimeMilliseconds = Math.max(1, totalTime.elapsed(TimeUnit.MILLISECONDS));
                long batchTimeMilliseconds = Math.max(1, batchTime.elapsed(TimeUnit.MILLISECONDS));

                long messagesPerSecondTotal = sentMessages * 1000 / totalTimeMilliseconds;
                long messagesPerSecondBatch = BatchSize * 1000 / batchTimeMilliseconds;

                log("Sent " + sentMessages + " messages within " + (totalTimeMilliseconds / 1000) + " seconds" +
                        " (" + messagesPerSecondTotal + " messages/second)." +
                        " In this batch " + BatchSize + " messages within " + batchTimeMilliseconds + "ms" +
                        " (" + messagesPerSecondBatch + " messages/second).");
                batchTime = Stopwatch.createStarted();
            }
        }

        log("Sender stopped after sending " + sentMessages + " messages.");
    }

    private static String generateMessage() {
        int halfLen = random.nextInt(MaxMessageLength / 2 + 1);
        String halfMessage = generateString(halfLen);
        return halfMessage + halfMessage;
    }

    private static String generateString(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(Alphabet.charAt(random.nextInt(Alphabet.length())));
        }
        return sb.toString();
    }

    private static void receive(String folder) throws IOException, InterruptedException {

        log("Receiver started.");

        SharedMessageQueue queue = SharedMessageQueue.createQueue(new File(folder), visibilityTimeout, retentionPeriod);

        log("Queue opened.");

        long receivedMessages = 0;
        long corruptedMessages = 0;
        Stopwatch totalTime = Stopwatch.createStarted();
        Stopwatch batchTime = Stopwatch.createStarted();

        while (!stopped) {
            Message message = queue.pull(1000);
            if (message != null) {
                queue.delete(message);

                receivedMessages++;
                if (!validateMessage(message)) {
                    corruptedMessages++;
                }

                if (receivedMessages % BatchSize == 0) {

                    long totalTimeMilliseconds = Math.max(1, totalTime.elapsed(TimeUnit.MILLISECONDS));
                    long batchTimeMilliseconds = Math.max(1, batchTime.elapsed(TimeUnit.MILLISECONDS));

                    long messagesPerSecondTotal = receivedMessages * 1000 / totalTimeMilliseconds;
                    long messagesPerSecondBatch = BatchSize * 1000 / batchTimeMilliseconds;

                    log("Received " + receivedMessages + " messages within " + (totalTimeMilliseconds / 1000) + " seconds" +
                            " (" + messagesPerSecondTotal + " messages/second, " + corruptedMessages + " corrupted)." +
                            " In this batch " + BatchSize + " messages within " + batchTimeMilliseconds + "ms" +
                            " (" + messagesPerSecondBatch + " messages/second).");
                    batchTime = Stopwatch.createStarted();
                }
            }
        }

        log("Receiver stopped after receiving " + receivedMessages + " messages.");
    }

    private static boolean validateMessage(Message message) {
        String text = message.asString();
        int len = text.length();
        if (len % 2 != 0) {
            log("Error: Message has odd length.");
            return false;
        }
        String leftPart = text.substring(0, len / 2);
        String rightPart = text.substring(len / 2, len);
        if (!leftPart.equals(rightPart)) {
            log("Error: Message has different left and right parts.");
            return false;
        }
        return true;
    }

    private static void log(String message) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(new Date()) + ": " + message);
    }

    private static void printUsage() {
        System.out.println("Usage: queue-tester (-send|-receive) <queue-folder>");
    }
}
