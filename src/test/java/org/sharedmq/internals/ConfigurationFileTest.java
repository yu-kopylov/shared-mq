package org.sharedmq.internals;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.sharedmq.test.TestUtils.assertThrows;

@Category(CommonTests.class)
public class ConfigurationFileTest {
    @Test
    public void testCreateAndOpen() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("ConfigurationFileTest", "testCreateAndOpen")) {

            File file = testFolder.getFile("config.dat");

            assertThrows(
                    IOException.class,
                    "does not exist",
                    () -> ConfigurationFile.open(file));

            assertTrue(file.createNewFile());

            assertThrows(
                    IOException.class,
                    "is not a configuration file",
                    () -> ConfigurationFile.open(file));

            assertTrue(file.delete());

            try (ConfigurationFile configFile = ConfigurationFile.create(file, new Configuration(123, 456))) {
                Configuration config = configFile.getConfiguration();
                assertEquals(config.getVisibilityTimeout(), 123);
                assertEquals(config.getRetentionPeriod(), 456);
            }

            // creating a config file with same parameters
            try (ConfigurationFile configFile = ConfigurationFile.create(file, new Configuration(123, 456))) {
                Configuration config = configFile.getConfiguration();
                assertEquals(config.getVisibilityTimeout(), 123);
                assertEquals(config.getRetentionPeriod(), 456);
            }

            // creating a config file with different parameter (visibilityTimeout)
            assertThrows(
                    IOException.class,
                    "exists and have different parameters",
                    () -> ConfigurationFile.create(file, new Configuration(111, 456)));

            // creating a config file with different parameter (retentionPeriod)
            assertThrows(
                    IOException.class,
                    "exists and have different parameters",
                    () -> ConfigurationFile.create(file, new Configuration(123, 444)));

            // obtaining parameters from the configuration file
            try (ConfigurationFile configFile = ConfigurationFile.open(file)) {
                Configuration config = configFile.getConfiguration();
                assertEquals(config.getVisibilityTimeout(), 123);
                assertEquals(config.getRetentionPeriod(), 456);
            }
        }
    }

    @Test
    public void testNextMessageId() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("ConfigurationFileTest", "testNextMessageId")) {

            File file = testFolder.getFile("config.dat");

            try (ConfigurationFile configFile = ConfigurationFile.create(file, new Configuration(100, 200))) {
                assertEquals(0, configFile.getNextMessageId());
                assertEquals(1, configFile.getNextMessageId());
                assertEquals(2, configFile.getNextMessageId());
            }

            // lets reopen file
            try (ConfigurationFile configFile = ConfigurationFile.create(file, new Configuration(100, 200))) {
                assertEquals(3, configFile.getNextMessageId());
                assertEquals(4, configFile.getNextMessageId());
                assertEquals(5, configFile.getNextMessageId());
            }

            // lets reopen file with different method
            try (ConfigurationFile configFile = ConfigurationFile.open(file)) {
                assertEquals(6, configFile.getNextMessageId());
                assertEquals(7, configFile.getNextMessageId());
                assertEquals(8, configFile.getNextMessageId());
            }
        }
    }
}
