package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class CsvFileReaderTest extends UnivocityFileReaderTest<CsvFileReader> {

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        boolean header = args.length < 1 || (boolean) args[0];
        CompressionType compression = args.length < 2 ? COMPRESSION_TYPE_DEFAULT : (CompressionType) args[1];
        File txtFile = File.createTempFile("test-", "." + getFileExtension());
        try (PrintWriter writer = new PrintWriter(getOutputStream(txtFile, compression))) {
            if (header) {
                String headerValue = String.join("#", FIELD_COLUMN1, FIELD_COLUMN2, FIELD_COLUMN3, FIELD_COLUMN4,
                        FIELD_COLUMN5, FIELD_COLUMN6, FIELD_COLUMN7, FIELD_COLUMN8, FIELD_COLUMN9);
                writer.append(headerValue + "\n");
            }
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d#%d#%d#%d#%f#%f#%s#%s#%s\n",
                        (byte) 2, (short) 4, 8, 16L, 32.32f, 64.64d,
                        true, "test bytes", "test string");
                writer.append(value);
                fsConfig.offsetsByIndex().put(index, (long) index);
            });
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), txtFile.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithMalformedRows(ReaderFsTestConfig fsConfig) throws IOException {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (FileWriter writer = new FileWriter(tmp)) {
            String headerValue = String.join(",", FIELD_COLUMN1, FIELD_COLUMN2, FIELD_COLUMN3, FIELD_COLUMN4,
                    FIELD_COLUMN5, FIELD_COLUMN6, FIELD_COLUMN7, FIELD_COLUMN8, FIELD_COLUMN9);
            writer.append(headerValue + "\n");
            writer.append(",\"\",,,,,true,test bytes,test string\n");
            writer.append("#comment\n");
            writer.append(",\"\",,,,,true,test bytes,test string\n");
        }
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_FORMAT_DELIMITER, ",");
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_EMPTY_VALUE, "10");
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_NULL_VALUE, "100");

        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        FileReader reader = getReader(fsConfig.getFs(), path, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            assertAll(
                    () -> assertEquals(record.get(FIELD_COLUMN1), (byte) 100),
                    () -> assertEquals(record.get(FIELD_COLUMN2), (short) 10),
                    () -> assertEquals(record.get(FIELD_COLUMN3), 100),
                    () -> assertEquals(record.get(FIELD_COLUMN4), 100L),
                    () -> assertEquals(record.get(FIELD_COLUMN5), 100.00f),
                    () -> assertEquals(record.get(FIELD_COLUMN6), 100.00d),
                    () -> assertEquals(record.get(FIELD_COLUMN7), true),
                    () -> assertEquals(new String((byte[]) record.get(FIELD_COLUMN8)), "test bytes"),
                    () -> assertEquals(record.get(FIELD_COLUMN9), "test string")
            );
            recordCount++;
        }
        assertEquals(2, recordCount, () -> "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithEmptyAndNullValueWithAllowNullsAndWithoutSchemaProvided(ReaderFsTestConfig fsConfig) throws IOException {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (FileWriter writer = new FileWriter(tmp)) {
            String headerValue = String.join(",", FIELD_COLUMN1, FIELD_COLUMN2, FIELD_COLUMN3);
            writer.append(headerValue + "\n");
            writer.append("yes,\"\",\n");
            writer.append("yes,cool,test");
        }

        Map<String, Object> defaultReadConfig = getReaderConfig();
        defaultReadConfig.remove(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_SCHEMA);
        Map<String, Object> readerConfig = defaultReadConfig;
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_FORMAT_DELIMITER, ",");
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_ALLOW_NULLS, "true");

        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        FileReader reader = getReader(fsConfig.getFs(), path, readerConfig);

        assertTrue(reader.hasNext());

        Struct record = reader.next();
        assertAll(
                () -> assertEquals(record.get(FIELD_COLUMN1), "yes"),
                () -> assertNull(record.get(FIELD_COLUMN2)),
                () -> assertNull(record.get(FIELD_COLUMN3))
        );

        assertTrue(reader.hasNext());
        Struct record2 = reader.next();
        assertAll(
                () -> assertEquals(record2.get(FIELD_COLUMN1), "yes"),
                () -> assertEquals(record2.get(FIELD_COLUMN2), "cool"),
                () -> assertEquals(record2.get(FIELD_COLUMN3), "test")
        );

        assertFalse(reader.hasNext());
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<String, Object>() {{
            put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_FORMAT_DELIMITER, "#");
            put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
            put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_SCHEMA, "byte,short,int,long,float,double,boolean,bytes,string");
        }};
    }
}
