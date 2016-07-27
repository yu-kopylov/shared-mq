package org.sharedmq.util;

import org.sharedmq.test.CommonTests;
import org.sharedmq.test.TestFolder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

import static org.sharedmq.test.TestUtils.assertThrows;
import static org.junit.Assert.*;

@Category(CommonTests.class)
public class IOUtilsTest {
    @Test
    public void testDelete() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("IOUtilsTest", "testDelete")) {
            File file = testFolder.getFile("test.dat");

            // nothing should happen if the file does not exist
            IOUtils.delete(file);

            assertTrue(file.createNewFile());
            IOUtils.delete(file);

            assertFalse(file.exists());

            assertTrue(file.createNewFile());

            CountDownLatch fileLocked = new CountDownLatch(1);

            Thread thread = new Thread(() -> {
                // locking file, to prevent its deletion
                try {
                    try (FileInputStream stream = new FileInputStream(file)) {
                        fileLocked.countDown();
                        Thread.sleep(200);
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            });

            thread.start();

            fileLocked.await();

            long deleteStarted = System.currentTimeMillis();
            IOUtils.delete(file);
            long deleteTime = System.currentTimeMillis() - deleteStarted;

            assertFalse(file.exists());

            System.out.println("Locked file was deleted within " + deleteTime + "ms.");
        }
    }

    @Test
    public void testCreateFolder() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("IOUtilsTest", "testCreateFolder")) {

            File folder = testFolder.getFile("folder");

            IOUtils.createFolder(folder);
            assertTrue(folder.exists());
            assertTrue(folder.isDirectory());

            File fileInFolder = new File(folder, "test.dat");
            assertTrue(fileInFolder.createNewFile());

            IOUtils.createFolder(folder);
            assertTrue(folder.exists());
            assertTrue(folder.isDirectory());
            assertTrue(fileInFolder.exists());

            assertThrows(
                    IOException.class,
                    "is not a directory",
                    () -> IOUtils.createFolder(fileInFolder));

            assertTrue(fileInFolder.exists());
            assertFalse(fileInFolder.isDirectory());
        }
    }

    @Test
    public void testDeleteTree() throws IOException, InterruptedException {
        try (TestFolder testFolder = new TestFolder("IOUtilsTest", "testDeleteTree")) {

            File folder = testFolder.getFile("folder");

            assertThrows(
                    IOException.class,
                    "folder",
                    () -> IOUtils.deleteTree(folder)
            );

            File subfolder = new File(folder, "subfolder");
            File fileInSubfolder = new File(subfolder, "test.dat");

            assertTrue(folder.mkdir());
            assertTrue(subfolder.mkdir());
            assertTrue(fileInSubfolder.createNewFile());

            IOUtils.deleteTree(folder);

            assertFalse(folder.exists());
            assertFalse(subfolder.exists());
            assertFalse(fileInSubfolder.exists());
        }
    }

    @Test
    public void testClose() throws IOException, InterruptedException {

        IOUtils.close();
        IOUtils.close((Closeable) null);
        IOUtils.close((Closeable) null, (Closeable) null);

        IOUtils.close(Arrays.asList());
        IOUtils.close(Arrays.asList((Closeable) null));
        IOUtils.close(Arrays.asList((Closeable) null, (Closeable) null));

        try {
            IOUtils.close(
                    () -> {
                        throw new IOException("Exception #1");
                    },
                    () -> {
                        throw new IOException("Exception #2");
                    }
            );
            fail("an exception was expected");
        } catch (IOException e) {
            assertEquals("Failed to close resources.", e.getMessage());
            assertEquals(2, e.getSuppressed().length);
            assertEquals("Exception #1", e.getSuppressed()[0].getMessage());
            assertEquals("Exception #2", e.getSuppressed()[1].getMessage());
        }

        IllegalArgumentException mainException = new IllegalArgumentException("Main Exception");
        IOUtils.closeOnError(
                mainException,
                () -> {
                    throw new IOException("Exception #1");
                },
                () -> {
                    throw new IOException("Exception #2");
                }
        );
        assertEquals(mainException.getMessage(), "Main Exception");
        assertEquals(2, mainException.getSuppressed().length);
        assertEquals("Exception #1", mainException.getSuppressed()[0].getMessage());
        assertEquals("Exception #2", mainException.getSuppressed()[1].getMessage());
    }
}
