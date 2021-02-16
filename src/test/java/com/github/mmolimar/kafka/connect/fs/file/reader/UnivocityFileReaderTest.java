package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.univocity.parsers.common.DataProcessingException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

abstract class UnivocityFileReaderTest<T extends UnivocityFileReader<?>> extends FileReaderTestBase {

    protected static final String FIELD_COLUMN1 = "column_1";
    protected static final String FIELD_COLUMN2 = "column_2";
    protected static final String FIELD_COLUMN3 = "column_3";
    protected static final String FIELD_COLUMN4 = "column_4";
    protected static final String FIELD_COLUMN5 = "column_5";
    protected static final String FIELD_COLUMN6 = "column_6";
    protected static final String FIELD_COLUMN7 = "column_7";
    protected static final String FIELD_COLUMN8 = "column_8";
    protected static final String FIELD_COLUMN9 = "column_9";
    protected static final String FILE_EXTENSION = "tcsv";
    protected static final CompressionType COMPRESSION_TYPE_DEFAULT = CompressionType.NONE;

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFile(ReaderFsTestConfig fsConfig) throws IOException {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fsConfig.getFs(), path, getReaderConfig());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidFileFormat(ReaderFsTestConfig fsConfig) throws IOException {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
            writer.write("test");
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        assertThrows(ConnectException.class, () -> getReader(fsConfig.getFs(), path, getReaderConfig()));
        assertThrows(IllegalArgumentException.class, () -> {
            try {
                getReader(fsConfig.getFs(), path, getReaderConfig());
            } catch (Exception ce) {
                throw ce.getCause();
            }
        });
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
    public void readAllDataWithoutHeader(ReaderFsTestConfig fsConfig) throws IOException {
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
    public void readAllDataWithCustomHeaders(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFile(fsConfig, false);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_HEADER, "false");
        // NOTE: 9 custom header names to match the quantity of static fields
        // FIELD_COLUMN1, ... , FIELD_COLUMN9
        String[] headers = new String[] { "a", "b", "c", "d", "e", "f", "g", "h", "i" };
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_HEADER_NAMES, String.join(",", headers));
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkDataWithHeaders(record, headers);
            recordCount++;
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithoutSchema(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFile(fsConfig, true);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.remove(T.FILE_READER_DELIMITED_SETTINGS_SCHEMA);
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkDataString(record);
            recordCount++;
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithMappingErrors(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFile(fsConfig, true);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_SCHEMA, "boolean,boolean,boolean,boolean,boolean,boolean,int,long,double");
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            try {
                reader.next();
            } catch (Exception e) {
                assertEquals(ConnectException.class, e.getClass());
                assertEquals(DataProcessingException.class, e.getCause().getClass());
            }
            recordCount++;
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataToleratingMappingErrors(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFile(fsConfig, true);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_SCHEMA, "boolean,boolean,boolean,boolean,boolean,boolean,int,long,double");
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_DATA_TYPE_MAPPING_ERROR, "false");
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkDataNull(record);
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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void seekFileWithoutHeader(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFile(fsConfig, false);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(T.FILE_READER_DELIMITED_SETTINGS_HEADER, "false");
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordIndex = NUM_RECORDS / 2;
        reader.seek(fsConfig.offsetsByIndex().get(recordIndex));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.offsetsByIndex().get(recordIndex), reader.currentOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = 0;
        reader.seek(fsConfig.offsetsByIndex().get(recordIndex));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.offsetsByIndex().get(recordIndex), reader.currentOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = NUM_RECORDS - 3;
        reader.seek(fsConfig.offsetsByIndex().get(recordIndex));
        assertTrue(reader.hasNext());
        assertEquals(fsConfig.offsetsByIndex().get(recordIndex), reader.currentOffset());
        checkData(reader.next(), recordIndex);

        reader.seek(fsConfig.offsetsByIndex().get(NUM_RECORDS - 1) + 1);
        assertFalse(reader.hasNext());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void validFileEncoding(ReaderFsTestConfig fsConfig) {
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
        assertThrows(ConnectException.class, () -> getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig));
        assertThrows(UnsupportedCharsetException.class, () -> {
            try {
                getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Class<? extends FileReader> getReaderClass() {
        return (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertAll(
                () -> assertEquals((byte) 2, record.get(FIELD_COLUMN1)),
                () -> assertEquals((short) 4, record.get(FIELD_COLUMN2)),
                () -> assertEquals(8, record.get(FIELD_COLUMN3)),
                () -> assertEquals(16L, record.get(FIELD_COLUMN4)),
                () -> assertEquals(32.32f, record.get(FIELD_COLUMN5)),
                () -> assertEquals(64.64d, record.get(FIELD_COLUMN6)),
                () -> assertEquals(true, record.get(FIELD_COLUMN7)),
                () -> assertEquals("test bytes", new String((byte[]) record.get(FIELD_COLUMN8))),
                () -> assertEquals("test string", record.get(FIELD_COLUMN9))
        );
    }

    protected void checkDataWithHeaders(Struct record, String[] headers) {
        assertAll(() -> assertEquals((byte) 2, record.get(headers[0])),
                () -> assertEquals((short) 4, record.get(headers[1])),
                () -> assertEquals(8, record.get(headers[2])),
                () -> assertEquals(16L, record.get(headers[3])),
                () -> assertEquals(32.32f, record.get(headers[4])),
                () -> assertEquals(64.64d, record.get(headers[5])),
                () -> assertEquals(true, record.get(headers[6])),
                () -> assertEquals("test bytes", new String((byte[]) record.get(headers[7]))),
                () -> assertEquals("test string", record.get(headers[8]))
        );
    }

    protected void checkDataString(Struct record) {
        assertAll(
                () -> assertEquals("2", record.get(FIELD_COLUMN1)),
                () -> assertEquals("4", record.get(FIELD_COLUMN2)),
                () -> assertEquals("8", record.get(FIELD_COLUMN3)),
                () -> assertEquals("16", record.get(FIELD_COLUMN4)),
                () -> assertEquals("32.320000", record.get(FIELD_COLUMN5)),
                () -> assertEquals("64.640000", record.get(FIELD_COLUMN6)),
                () -> assertEquals("true", record.get(FIELD_COLUMN7)),
                () -> assertEquals("test bytes", record.get(FIELD_COLUMN8)),
                () -> assertEquals("test string", record.get(FIELD_COLUMN9))
        );
    }

    protected void checkDataNull(Struct record) {
        assertAll(
                () -> assertNull(record.get(FIELD_COLUMN1)),
                () -> assertNull(record.get(FIELD_COLUMN2)),
                () -> assertNull(record.get(FIELD_COLUMN3)),
                () -> assertNull(record.get(FIELD_COLUMN4)),
                () -> assertNull(record.get(FIELD_COLUMN5)),
                () -> assertNull(record.get(FIELD_COLUMN6)),
                () -> assertNull(record.get(FIELD_COLUMN7)),
                () -> assertNull(record.get(FIELD_COLUMN8)),
                () -> assertNull(record.get(FIELD_COLUMN9))
        );
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
