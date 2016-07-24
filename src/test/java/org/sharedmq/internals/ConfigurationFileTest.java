package org.sharedmq.internals;

import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;

import static org.sharedmq.test.TestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

@Category(CommonTests.class)
public class ConfigurationFileTest {
    @Test
    public void testSmoke() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("ConfigurationFileTest", "testSmoke")) {
            try {
                File file = testFolder.getFile("config.dat");

                assertThrows(
                        IOException.class,
                        "is not a SharedMessageQueue configuration file",
                        () -> new ConfigurationFile(file));

                try (ConfigurationFile config = new ConfigurationFile(file, 123, 456)) {
                    assertEquals(config.getVisibilityTimeout(), 123);
                    assertEquals(config.getRetentionPeriod(), 456);
                }

                // creating a config file with same parameters
                try (ConfigurationFile config = new ConfigurationFile(file, 123, 456)) {
                    assertEquals(config.getVisibilityTimeout(), 123);
                    assertEquals(config.getRetentionPeriod(), 456);
                }

                // creating a config file with different parameter (visibilityTimeout)
                assertThrows(
                        IOException.class,
                        "exists and have different parameters",
                        () -> new ConfigurationFile(file, 111, 456));

                // creating a config file with different parameter (retentionPeriod)
                assertThrows(
                        IOException.class,
                        "exists and have different parameters",
                        () -> new ConfigurationFile(file, 123, 444));

                // obtaining parameters from the configuration file
                try (ConfigurationFile config = new ConfigurationFile(file)) {
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
