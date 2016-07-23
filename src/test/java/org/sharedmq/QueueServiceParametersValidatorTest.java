package org.sharedmq;

import org.sharedmq.test.CommonTests;
import com.google.common.base.Strings;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.sharedmq.test.TestUtils.assertThrows;

@Category(CommonTests.class)
public class QueueServiceParametersValidatorTest {

    @Test
    public void testValidateCreateQueue() {

        QueueServiceParametersValidator.validateCreateQueue(Strings.padEnd("Valid_Queue_Name-1", 80, '_'), 15, 120);

        // check queueName validation
        assertThrows(
                IllegalArgumentException.class,
                "queue name cannot be null",
                () -> QueueServiceParametersValidator.validateCreateQueue(null, 15, 120));
        assertThrows(
                IllegalArgumentException.class,
                "queue name cannot be empty",
                () -> QueueServiceParametersValidator.validateCreateQueue("", 15, 120));
        assertThrows(
                IllegalArgumentException.class,
                "queue name cannot be longer than 80 characters",
                () -> QueueServiceParametersValidator.validateCreateQueue(Strings.repeat("x", 81), 15, 120));
        assertThrows(
                IllegalArgumentException.class,
                "queue name can contain only alphanumeric characters, hyphens and underscores",
                () -> QueueServiceParametersValidator.validateCreateQueue("WrongName!", 15, 120));

        // check visibilityTimeout validation
        assertThrows(
                IllegalArgumentException.class,
                "visibilityTimeout in seconds must be between 0 and 43200",
                () -> QueueServiceParametersValidator.validateCreateQueue("test", -1, 120));
        assertThrows(
                IllegalArgumentException.class,
                "visibilityTimeout in seconds must be between 0 and 43200",
                () -> QueueServiceParametersValidator.validateCreateQueue("test", 12 * 60 * 60 + 1, 120));

        // check visibilityTimeout validation
        assertThrows(
                IllegalArgumentException.class,
                "retentionPeriod in seconds must be between 0 and 1209600",
                () -> QueueServiceParametersValidator.validateCreateQueue("test", 15, -1));
        assertThrows(
                IllegalArgumentException.class,
                "retentionPeriod in seconds must be between 0 and 1209600",
                () -> QueueServiceParametersValidator.validateCreateQueue("test", 15, 1209601));
    }

    @Test
    public void testValidatePush() {

        QueueServiceParametersValidator.validatePush("Valid_Queue_Url-1", 1, "Test Message");
        QueueServiceParametersValidator.validatePush("Valid_Queue_Url-1", 1, Strings.repeat("x", 256 * 1024));

        // queueUrl validation
        assertThrows(IllegalArgumentException.class,
                "queueUrl parameter cannot be null",
                () -> QueueServiceParametersValidator.validatePush(null, 1, "Test Message"));

        // delay validation
        assertThrows(IllegalArgumentException.class,
                "delay in seconds must be between 0 and 900",
                () -> QueueServiceParametersValidator.validatePush("test", -1, "Test Message"));
        assertThrows(IllegalArgumentException.class,
                "delay in seconds must be between 0 and 900",
                () -> QueueServiceParametersValidator.validatePush("test", 901, "Test Message"));


        // message validation
        assertThrows(IllegalArgumentException.class,
                "message parameter cannot be null",
                () -> QueueServiceParametersValidator.validatePush("test", 1, null));

        assertThrows(IllegalArgumentException.class,
                "message cannot be longer than 256KB in the UTF-8 encoding",
                () -> QueueServiceParametersValidator.validatePush("test", 1, Strings.repeat("x", 256 * 1024 + 1)));

        assertThrows(IllegalArgumentException.class,
                "message cannot be longer than 256KB in the UTF-8 encoding",
                () -> QueueServiceParametersValidator.validatePush("test", 1, Strings.repeat("\u044F", 256 * 1024)));
    }

    @Test
    public void testValidatePull() {

        QueueServiceParametersValidator.validatePull("Valid_Queue_Url-1", 0);
        QueueServiceParametersValidator.validatePull("Valid_Queue_Url-1", 20);

        // queueUrl validation
        assertThrows(IllegalArgumentException.class,
                "queueUrl parameter cannot be null",
                () -> QueueServiceParametersValidator.validatePull(null, 1));

        // timeout validation
        assertThrows(IllegalArgumentException.class,
                "timeout in seconds must be between 0 and 20",
                () -> QueueServiceParametersValidator.validatePull("test", -1));
        assertThrows(IllegalArgumentException.class,
                "timeout in seconds must be between 0 and 20",
                () -> QueueServiceParametersValidator.validatePull("test", 21));
    }

    @Test
    public void testValidateDelete() {

        class FakeMessage1 implements Message {
            @Override
            public String asString() {
                return "fake";
            }
        }

        class FakeMessage2 extends FakeMessage1 {
        }

        QueueServiceParametersValidator.validateDelete("Valid_Queue_Url-1", new FakeMessage1(), FakeMessage1.class);

        // queueUrl validation
        assertThrows(IllegalArgumentException.class,
                "queueUrl parameter cannot be null",
                () -> QueueServiceParametersValidator.validateDelete(null, new FakeMessage1(), FakeMessage1.class));

        // message validation
        assertThrows(IllegalArgumentException.class,
                "message parameter cannot be null",
                () -> QueueServiceParametersValidator.validateDelete("test", null, FakeMessage1.class));
        // it is ok, FakeMessage2 inherits FakeMessage1
        QueueServiceParametersValidator.validateDelete("test", new FakeMessage2(), FakeMessage1.class);
        assertThrows(IllegalArgumentException.class,
                "message does not belong to this type of a message queue",
                () -> QueueServiceParametersValidator.validateDelete("test", new FakeMessage1(), FakeMessage2.class));

        // expectedMessageClass validation
        assertThrows(IllegalArgumentException.class,
                "expectedMessageClass parameter cannot be null",
                () -> QueueServiceParametersValidator.validateDelete("test", new FakeMessage1(), null));
    }
}
