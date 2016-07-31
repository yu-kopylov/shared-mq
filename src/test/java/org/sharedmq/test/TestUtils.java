package org.sharedmq.test;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Utilities for testing.
 */
public class TestUtils {

    // random is only used for quick generation of non-empty arrays
    private static final Random random = new Random();

    public static <T extends Exception> void assertThrows(
            Class<T> errorClass,
            String expectedMessagePart,
            TestedAction action
    ) {
        try {
            action.invoke();
            fail("an exception was expected");
        } catch (AssertionError e) {
            throw e;
        } catch (Throwable e) {
            if (!errorClass.isInstance(e)) {
                throw new AssertionError("Expected '" + errorClass.getName() + "'" +
                        ", but encountered '" + e.getClass().getName() + "'."
                        , e);
            }
            if (expectedMessagePart != null) {
                String message = e.getMessage();
                if (message == null || !message.contains(expectedMessagePart)) {
                    throw new AssertionError("Expected an exception" +
                            " with the message '..." + expectedMessagePart + "...'" +
                            ", but encountered '" + e.getMessage() + "'."
                            , e);
                }
            }
        }
    }

    public interface TestedAction {
        void invoke() throws Exception;
    }

    public static byte[] generateArray(int length) {
        byte[] array = new byte[length];
        random.nextBytes(array);
        return array;
    }

    public static void printResult(String prefix, Stopwatch timer, int operationCount) {
        printResult(prefix, timer.elapsed(TimeUnit.MILLISECONDS), operationCount);
    }

    public static void printResult(String prefix, long timeSpent, int operationCount) {
        String messagesPerSecond = timeSpent == 0 ? "unknown" : String.valueOf(operationCount * 1000L / timeSpent);

        long operationTime = timeSpent * 1000L / operationCount;

        System.out.println(Strings.padEnd(prefix, 39, ' ') +
                ": " + operationCount + " operations" +
                ", " + Strings.padStart(String.valueOf(timeSpent), 5, ' ') + "ms" +
                " (" + Strings.padStart(String.valueOf(operationTime), 5, ' ') + "\u00B5s per op." +
                ", " + Strings.padStart(messagesPerSecond, 6, ' ') + " op/second).");
    }
}
