package org.sharedmq.internals;

import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.internals.MappedQueueConfigFile;

import java.io.File;
import java.io.IOException;

import static org.sharedmq.test.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

@Category(CommonTests.class)
public class SharedMessageQueueConfigFileTest {
    @Test
    public void testSmoke() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("SharedMessageQueueConfigFileTest", "testSmoke")) {
            try {
                File file = testFolder.getFile("config.dat");

                assertThrows(
                        IOException.class,
                        "is not a SharedMessageQueue configuration file",
                        () -> new MappedQueueConfigFile(file));

                try (MappedQueueConfigFile config = new MappedQueueConfigFile(file, 123, 456)) {
                    assertEquals(config.getVisibilityTimeout(), 123);
                    assertEquals(config.getRetentionPeriod(), 456);
                }

                // creating a config file with same parameters
                try (MappedQueueConfigFile config = new MappedQueueConfigFile(file, 123, 456)) {
                    assertEquals(config.getVisibilityTimeout(), 123);
                    assertEquals(config.getRetentionPeriod(), 456);
                }

                // creating a config file with different parameter (visibilityTimeout)
                assertThrows(
                        IOException.class,
                        "exists and have different parameters",
                        () -> new MappedQueueConfigFile(file, 111, 456));

                // creating a config file with different parameter (retentionPeriod)
                assertThrows(
                        IOException.class,
                        "exists and have different parameters",
                        () -> new MappedQueueConfigFile(file, 123, 444));

                // obtaining parameters from the configuration file
                try (MappedQueueConfigFile config = new MappedQueueConfigFile(file)) {
                    assertEquals(config.getVisibilityTimeout(), 123);
                    assertEquals(config.getRetentionPeriod(), 456);
                }
            } finally {
                // A mapped byte buffer and the file mapping that it represents
                // remain valid until the buffer itself is garbage-collected.
                System.gc();
            }
        }
    }
}
