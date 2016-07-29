package org.sharedmq.test;

import java.util.Random;

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
}
