package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

abstract class UnivocityFileReaderTest<T extends UnivocityFileReader> extends FileReaderTestBase {

    protected static final String FIELD_COLUMN1 = "column_1";
    protected static final String FIELD_COLUMN2 = "column_2";
    protected static final String FIELD_COLUMN3 = "column_3";
    protected static final String FIELD_COLUMN4 = "column_4";
    protected static final String FILE_EXTENSION = "tcsv";
    protected static final CompressionType COMPRESSION_TYPE_DEFAULT = CompressionType.NONE;

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFile(ReaderFsTestConfig fsConfig) throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fsConfig.getFs(), path, getReaderConfig());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidFileFormat(ReaderFsTestConfig fsConfig) throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
            writer.write("test");
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fsConfig.getFs(), path, getReaderConfig());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invaliConfigArgs(ReaderFsTestConfig fsConfig) {
        try {
            getReaderClass().getConstructor(FileSystem.class, Path.class, Map.class)
                    .newInstance(fsConfig.getFs(), fsConfig.getDataFile(), new HashMap<String, Object>());
        } catch (Exception e) {
            assertThrows(IllegalArgumentException.class, () -> {
                throw e.getCause();
            });
        }
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithoutHeader(ReaderFsTestConfig fsConfig) throws Throwable {
        Path file = createDataFile(fsConfig, false);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_HEADER, "false");
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

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
    public void readDifferentCompressionTypes(ReaderFsTestConfig fsConfig) {
        Arrays.stream(CompressionType.values()).forEach(compressionType -> {
            try {
                Path file = createDataFile(fsConfig, true, compressionType);
                Map<String, Object> readerConfig = getReaderConfig();
                readerConfig.put(T.FILE_READER_DELIMITED_COMPRESSION_TYPE, compressionType.toString());
                readerConfig.put(T.FILE_READER_DELIMITED_COMPRESSION_CONCATENATED, "true");
                readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
                FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

                assertTrue(reader.hasNext());

                int recordCount = 0;
                while (reader.hasNext()) {
                    Struct record = reader.next();
                    checkData(record, recordCount);
                    recordCount++;
                }
                reader.close();
                assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void seekFileWithoutHeader(ReaderFsTestConfig fsConfig) throws Throwable {
        Path file = createDataFile(fsConfig, false);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_HEADER, "false");
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordIndex = NUM_RECORDS / 2;
        reader.seek(getOffset(fsConfig.offsetsByIndex().get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.offsetsByIndex().get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = 0;
        reader.seek(getOffset(fsConfig.offsetsByIndex().get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.offsetsByIndex().get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = NUM_RECORDS - 3;
        reader.seek(getOffset(fsConfig.offsetsByIndex().get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.offsetsByIndex().get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        reader.seek(getOffset(fsConfig.offsetsByIndex().get(NUM_RECORDS - 1) + 1));
        assertFalse(reader.hasNext());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void validFileEncoding(ReaderFsTestConfig fsConfig) throws Throwable {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
        readerConfig.put(T.FILE_READER_DELIMITED_ENCODING, "Cp1252");
        getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidFileEncoding(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
        readerConfig.put(T.FILE_READER_DELIMITED_ENCODING, "invalid_charset");
        assertThrows(UnsupportedCharsetException.class, () -> getReader(fsConfig.getFs(),
                fsConfig.getDataFile(), readerConfig));
    }

    @Override
    protected Offset getOffset(long offset) {
        return new T.UnivocityOffset(offset);
    }

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass())
                .getActualTypeArguments()[0];
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertAll(
                () -> assertTrue(record.get(FIELD_COLUMN1).toString().startsWith(index + "_")),
                () -> assertTrue(record.get(FIELD_COLUMN2).toString().startsWith(index + "_")),
                () -> assertTrue(record.get(FIELD_COLUMN3).toString().startsWith(index + "_")),
                () -> assertTrue(record.get(FIELD_COLUMN4).toString().startsWith(index + "_"))
        );
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
