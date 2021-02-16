package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class BinaryFileReaderTest extends FileReaderTestBase {

    private static final String FILE_EXTENSION = "bin";

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        File binaryFile = File.createTempFile("test-", "." + getFileExtension());
        byte[] content = "test".getBytes();
        Path path = new Path(new Path(fsConfig.getFsUri()), binaryFile.getName());
        Files.write(binaryFile.toPath(), content);
        fsConfig.getFs().moveFromLocalFile(new Path(binaryFile.getAbsolutePath()), path);
        IntStream.range(0, NUM_RECORDS).forEach(index -> fsConfig.offsetsByIndex().put(index, (long) 0));
        return path;
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFile(ReaderFsTestConfig fsConfig) throws IOException {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        FileReader reader = getReader(fsConfig.getFs(), path, getReaderConfig());
        assertFalse(reader.hasNext());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Override
    public void readAllData(ReaderFsTestConfig fsConfig) {
        FileReader reader = fsConfig.getReader();
        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(record, recordCount);
            recordCount++;
        }
        assertEquals(1, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataInBatches(ReaderFsTestConfig fsConfig) {
        Map<String, Object> config = getReaderConfig();
        int batchSize = 5;
        config.put(FsSourceTaskConfig.FILE_READER_BATCH_SIZE, batchSize);
        AbstractFileReader<?> reader = (AbstractFileReader<?>) getReader(fsConfig.getFs(), fsConfig.getDataFile(), config);
        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNextBatch()) {
            reader.nextBatch();
            while (reader.hasNext()) {
                Struct record = reader.next();
                checkData(record, recordCount);
                recordCount++;
            }
            assertEquals(1, recordCount % batchSize);
        }
        assertThrows(NoSuchElementException.class, reader::nextBatch);
        assertEquals(1, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Disabled
    public void invalidFileFormat(ReaderFsTestConfig fsConfig) throws IOException {
    }

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return BinaryFileReader.class;
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<>();
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertAll(
                () -> assertFalse(record.get("path").toString().isEmpty()),
                () -> assertFalse(record.get("owner").toString().isEmpty()),
                () -> assertFalse(record.get("group").toString().isEmpty()),
                () -> assertEquals(record.getInt64("length"), 4L),
                () -> assertNotNull(record.get("access_time")),
                () -> assertNotNull(record.get("modification_time")),
                () -> assertEquals(new String(record.getBytes("content")), "test")
        );
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
