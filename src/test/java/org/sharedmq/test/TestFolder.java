package org.sharedmq.test;

import org.sharedmq.util.IOUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

/**
 * Represents a temporary test folder.<br/>
 * The folder and all its content are deleted on close.
 */
public class TestFolder implements Closeable {

    private final File root;

    /**
     * Creates an new folder in the default temporary-file directory.
     *
     * @param prefix The prefix for the new folder name.
     * @param suffix The suffix for the new folder name.
     * @throws IOException If folder creation fails.
     */
    public TestFolder(String prefix, String suffix) throws IOException {
        root = File.createTempFile(prefix, suffix);
        if (!root.delete() || !root.mkdir()) {
            throw new IOException("Failed to prepare test folder.");
        }
    }

    /**
     * Returns a {@link File} object matching this folder location.
     */
    public File getRoot() {
        return root;
    }

    /**
     * Returns an {@link File} instance for the file with the given name in this test folder.
     *
     * @param filename The name of the faile.
     */
    public File getFile(String filename) {
        return new File(root, filename);
    }

    /**
     * Deletes this test folder and all its content.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

        // A mapped byte buffer and the file mapping that it represents
        // remain valid until the buffer itself is garbage-collected.
        System.gc();

        IOUtils.deleteTree(root);
    }
}
