package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

abstract class FileReaderTestBase {

    private static final List<FileSystemConfig> TEST_FILE_SYSTEMS = Arrays.asList(
            new LocalFsConfig(),
            new HdfsFsConfig()
    );
    protected static final int NUM_RECORDS = 100;

    @BeforeAll
    public static void initFs() throws IOException {
        for (FileSystemConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.initFs();
        }
    }

    @AfterAll
    public static void finishFs() throws IOException {
        for (FileSystemConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.close();
        }
    }

    @BeforeEach
    public void openReader() throws Throwable {
        for (FileSystemConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.setDataFile(createDataFile(fsConfig));
            FileReader reader = ReflectionUtils.makeReader(getReaderClass(), fsConfig.getFs(),
                    fsConfig.getDataFile(), getReaderConfig());
            assertEquals(reader.getFilePath(), fsConfig.getDataFile());
            fsConfig.setReader(reader);
        }
    }

    @AfterEach
    public void closeReader() {
        for (FileSystemConfig fsConfig : TEST_FILE_SYSTEMS) {
            try {
                fsConfig.getReader().close();
            } catch (Exception e) {
                //ignoring
            }
        }
    }

    private static Stream<Arguments> fileSystemConfigProvider() {
        return TEST_FILE_SYSTEMS.stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidArgs(FileSystemConfig fsConfig) {
        try {
            fsConfig.getReader().getClass().getConstructor(FileSystem.class, Path.class, Map.class)
                    .newInstance(null, null, null);
        } catch (Exception e) {
            assertThrows(IllegalArgumentException.class, () -> {
                throw e.getCause();
            });
        }
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void fileDoesNotExist(FileSystemConfig fsConfig) {
        Path path = new Path(new Path(fsConfig.getFsUri()), UUID.randomUUID().toString());
        assertThrows(FileNotFoundException.class, () -> getReader(fsConfig.getFs(), path, getReaderConfig()));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFile(FileSystemConfig fsConfig) throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        assertThrows(IOException.class, () -> getReader(fsConfig.getFs(), path, getReaderConfig()));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidFileFormat(FileSystemConfig fsConfig) throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
            writer.write("test");
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        assertThrows(IOException.class, () -> getReader(fsConfig.getFs(), path, getReaderConfig()));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllData(FileSystemConfig fsConfig) {
        FileReader reader = fsConfig.getReader();
        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(record, recordCount);
            recordCount++;
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void seekFile(FileSystemConfig fsConfig) {
        FileReader reader = fsConfig.getReader();
        int recordIndex = NUM_RECORDS / 2;
        reader.seek(getOffset(fsConfig.getOffsetsByIndex().get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.getOffsetsByIndex().get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = 0;
        reader.seek(getOffset(fsConfig.getOffsetsByIndex().get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.getOffsetsByIndex().get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = NUM_RECORDS - 3;
        reader.seek(getOffset(fsConfig.getOffsetsByIndex().get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.getOffsetsByIndex().get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        reader.seek(getOffset(fsConfig.getOffsetsByIndex().get(NUM_RECORDS - 1) + 1));
        assertFalse(reader.hasNext());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void negativeSeek(FileSystemConfig fsConfig) {
        FileReader reader = fsConfig.getReader();
        assertThrows(RuntimeException.class, () -> reader.seek(getOffset(-1)));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void exceededSeek(FileSystemConfig fsConfig) {
        FileReader reader = fsConfig.getReader();
        reader.seek(getOffset(fsConfig.getOffsetsByIndex().get(NUM_RECORDS - 1) + 1));
        assertFalse(reader.hasNext());
        assertThrows(NoSuchElementException.class, reader::next);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readFileAlreadyClosed(FileSystemConfig fsConfig) throws IOException {
        FileReader reader = fsConfig.getReader();
        reader.close();
        assertThrows(IllegalStateException.class, reader::hasNext);
        assertThrows(IllegalStateException.class, reader::next);
    }

    protected Offset getOffset(long offset) {
        return () -> offset;
    }

    protected final FileReader getReader(FileSystem fs, Path path, Map<String, Object> config) throws Throwable {
        return ReflectionUtils.makeReader(getReaderClass(), fs, path, config);
    }

    protected OutputStream getOutputStream(File file, CompressionType compression) throws IOException {
        final OutputStream os;
        switch (compression) {
            case BZIP2:
                os = new BZip2CompressorOutputStream(new FileOutputStream(file));
                break;
            case GZIP:
                os = new GzipCompressorOutputStream(new FileOutputStream(file));
                break;
            default:
                os = new FileOutputStream(file);
                break;
        }
        return os;
    }

    protected abstract Class<? extends FileReader> getReaderClass();

    protected abstract Path createDataFile(FileSystemConfig fsConfig, Object... args) throws IOException;

    protected abstract Map<String, Object> getReaderConfig();

    protected abstract String getFileExtension();

    protected abstract void checkData(Struct record, long index);

}
