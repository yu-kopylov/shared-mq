package org.sharedmq.internals;

import com.google.common.base.Strings;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.sharedmq.Message;
import org.sharedmq.test.CommonTests;

import java.io.File;

import static org.sharedmq.test.TestUtils.assertThrows;

@Category(CommonTests.class)
public class QueueParametersValidatorTest {

    @Test
    public void testValidateCreateQueue() {

        File rootFolder = new File("some.folder");

        QueueParametersValidator.validateCreateQueue(rootFolder, 0, 15 * 1000L);
        QueueParametersValidator.validateCreateQueue(rootFolder, 12 * 60 * 60 * 1000L, 14 * 24 * 60 * 60 * 1000L);

        // check rootFolder validation
        assertThrows(
                IllegalArgumentException.class,
                "The rootFolder parameter cannot be null.",
                () -> QueueParametersValidator.validateCreateQueue(null, 0, 120000));

        // check visibilityTimeout validation
        assertThrows(
                IllegalArgumentException.class,
                "visibilityTimeout in milliseconds must be between 0 and 43200000",
                () -> QueueParametersValidator.validateCreateQueue(rootFolder, -1, 120000));
        assertThrows(
                IllegalArgumentException.class,
                "visibilityTimeout in milliseconds must be between 0 and 43200000",
                () -> QueueParametersValidator.validateCreateQueue(rootFolder, 12 * 60 * 60 * 1000L + 1, 120000));

        // check visibilityTimeout validation
        assertThrows(
                IllegalArgumentException.class,
                "retentionPeriod in milliseconds must be between 15000 and 1209600000",
                () -> QueueParametersValidator.validateCreateQueue(rootFolder, 0, 15 * 1000L - 1));
        assertThrows(
                IllegalArgumentException.class,
                "retentionPeriod in milliseconds must be between 15000 and 1209600000",
                () -> QueueParametersValidator.validateCreateQueue(rootFolder, 0, 14 * 24 * 60 * 60 * 1000L + 1));
    }

    @Test
    public void testValidateOpenQueue() {

        File rootFolder = new File("some.folder");

        QueueParametersValidator.validateOpenQueue(rootFolder);

        // check rootFolder validation
        assertThrows(
                IllegalArgumentException.class,
                "The rootFolder parameter cannot be null.",
                () -> QueueParametersValidator.validateOpenQueue(null));
    }

    @Test
    public void testValidatePush() {

        QueueParametersValidator.validatePush(1, "Test Message");
        QueueParametersValidator.validatePush(1, Strings.repeat("x", 256 * 1024));

        // delay validation
        assertThrows(IllegalArgumentException.class,
                "delay in milliseconds must be between 0 and 900000",
                () -> QueueParametersValidator.validatePush(-1, "Test Message"));

        assertThrows(IllegalArgumentException.class,
                "delay in milliseconds must be between 0 and 900000",
                () -> QueueParametersValidator.validatePush(900001, "Test Message"));


        // message validation
        assertThrows(IllegalArgumentException.class,
                "message parameter cannot be null",
                () -> QueueParametersValidator.validatePush(1, null));

        assertThrows(IllegalArgumentException.class,
                "message cannot be longer than 256KB in the UTF-8 encoding",
                () -> QueueParametersValidator.validatePush(1, Strings.repeat("x", 256 * 1024 + 1)));

        assertThrows(IllegalArgumentException.class,
                "message cannot be longer than 256KB in the UTF-8 encoding",
                () -> QueueParametersValidator.validatePush(1, Strings.repeat("\u044F", 256 * 1024)));
    }

    @Test
    public void testValidatePull() {

        QueueParametersValidator.validatePull(0);
        QueueParametersValidator.validatePull(20000);

        // timeout validation
        assertThrows(IllegalArgumentException.class,
                "timeout in milliseconds must be between 0 and 20000",
                () -> QueueParametersValidator.validatePull(-1));
        assertThrows(IllegalArgumentException.class,
                "timeout in milliseconds must be between 0 and 20000",
                () -> QueueParametersValidator.validatePull(20001));
    }

    @Test
    public void testValidateDelete() {

        QueueParametersValidator.validateDelete(Mockito.mock(MappedQueueMessage.class));

        // message validation
        assertThrows(IllegalArgumentException.class,
                "message parameter cannot be null",
                () -> QueueParametersValidator.validateDelete(null));

        assertThrows(IllegalArgumentException.class,
                "message type does not belong to this message queue",
                () -> QueueParametersValidator.validateDelete(Mockito.mock(Message.class)));
    }
}
