package org.sharedmq.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileUtils {

    private static final Logger logger = LoggerFactory.getLogger(FileUtils.class);

    /**
     * Deletes the given file or directory.<br/>
     * Method makes several attempts to delete the file.<br/>
     * It is useful when an antivirus software can temporarily prevent deletion of the file.<br/>
     * This method does nothing is the file does not exist.
     *
     * @param file the file to delete
     * @throws IOException          If the file exists and was not deleted.
     * @throws InterruptedException If this thread is interrupted.
     */
    public static void delete(File file) throws IOException, InterruptedException {

        boolean deleted = file.delete();

        if (deleted) {
            return;
        }

        int delay = 50;
        int attempt = 2;

        while (!deleted && file.exists() && attempt <= 7) {

            logger.debug(
                    "An attempt to delete file '" + file.getAbsolutePath() + "' failed." +
                            " Next attempt #" + attempt + ".");

            Thread.sleep(delay);

            deleted = file.delete();
            delay *= 2;
            attempt++;
        }

        if (!deleted && file.exists()) {
            throw new IOException("Cannot delete file or folder '" + file.getAbsolutePath() + "'.");
        }
    }

    /**
     * Deletes a file or directory including all its content.
     *
     * @param directory The directory to delete.
     * @throws IOException If deletion fails or this thread is interrupted.
     */
    public static void deleteTree(File directory) throws IOException {
        Path root = Paths.get(directory.getAbsolutePath());
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                try {
                    delete(file.toFile());
                } catch (InterruptedException e) {
                    logger.error("File tree deletion was interrupted.", e);
                    throw new IOException("File tree deletion was interrupted.", e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                try {
                    delete(dir.toFile());
                } catch (InterruptedException e) {
                    logger.error("File tree deletion was interrupted.", e);
                    throw new IOException("File tree deletion was interrupted.", e);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /**
     * Creates a folder specified by the parameter.<br/>
     * Does nothing if folder already exists.
     *
     * @param folder The folder to create.
     * @throws IOException If folder cannot be created.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void createFolder(File folder) throws IOException {
        // we do not check result here, it is possible that folder already exists
        folder.mkdir();

        if (!folder.exists()) {
            throw new IOException("Cannot create directory '" + folder.getAbsolutePath() + "'.");
        }

        if (!folder.isDirectory()) {
            throw new IOException("'" + folder.getAbsolutePath() + "' is not a directory.");
        }
    }

    /**
     * Creates a MappedByteBuffer for the given file.
     *
     * @param file The file to create buffer for.
     * @param size The size of the buffer.
     * @return A new MappedByteBuffer for the given file.
     * @throws IOException If there is a file-related problem.
     */
    public static MappedByteBuffer createMappedByteBuffer(File file, long size) throws IOException {

        try (
                RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
                FileChannel channel = randomAccessFile.getChannel()
        ) {
            return channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        }
    }

    /**
     * Attempts to close all given resources.<br/>
     * Method closes resources in the same order they appear in the parameters.<br/>
     * Resources that are null are ignored.
     *
     * @param resources The array of closeable resources.
     * @throws IOException If close failed for one or more resources.
     */
    public static void close(AutoCloseable... resources) throws IOException {
        close(Arrays.asList(resources));
    }

    /**
     * Attempts to close all given resources.<br/>
     * Method closes resources in the same order they appear in the parameters.<br/>
     * Resources that are null are ignored.
     *
     * @param resources The array of closeable resources.
     * @throws IOException If close failed for one or more resources.
     */
    public static void close(Iterable<AutoCloseable> resources) throws IOException {

        List<Throwable> exceptions = closeResources(resources);

        if (exceptions.size() > 0) {
            IOException aggregateException = new IOException("Failed to close resources.");
            for (Throwable exception : exceptions) {
                aggregateException.addSuppressed(exception);
            }
            throw aggregateException;
        }
    }

    /**
     * Attempts to close all given resources.<br/>
     * Method closes resources in the same order they appear in the parameters.<br/>
     * Resources that are null are ignored.<br/>
     * Adds all exceptions that occur during close to the list of suppressed exceptions of the given error.
     *
     * @param error     The exception to which errors will be added.
     * @param resources The array of closeable resources.
     */
    public static void closeOnError(Throwable error, AutoCloseable... resources) {

        List<Throwable> exceptions = closeResources(Arrays.asList(resources));
        for (Throwable exception : exceptions) {
            error.addSuppressed(exception);
        }
    }

    private static List<Throwable> closeResources(Iterable<AutoCloseable> resources) {
        List<Throwable> exceptions = new ArrayList<>();
        for (AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Throwable e) {
                    exceptions.add(e);
                }
            }
        }
        return exceptions;
    }
}
