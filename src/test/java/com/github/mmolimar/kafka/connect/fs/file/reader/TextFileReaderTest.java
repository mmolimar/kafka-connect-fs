package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class TextFileReaderTest extends FileReaderTestBase {

    private static final String FIELD_NAME_VALUE = "custom_field_name";
    private static final String FILE_EXTENSION = "txt";
    private static final CompressionType COMPRESSION_TYPE_DEFAULT = CompressionType.GZIP;

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        CompressionType compression = args.length < 1 ? COMPRESSION_TYPE_DEFAULT : (CompressionType) args[0];
        File txtFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (PrintWriter writer = new PrintWriter(getOutputStream(txtFile, compression))) {
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                writer.append(value + "\n");
                fsConfig.offsetsByIndex().put(index, (long) index);
            });
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), txtFile.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void validFileEncoding(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
        readerConfig.put(TextFileReader.FILE_READER_TEXT_ENCODING, "Cp1252");
        readerConfig.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, COMPRESSION_TYPE_DEFAULT);
        FileReader reader = getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig);
        fsConfig.setReader(reader);
        readAllData(fsConfig);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidFileEncoding(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
        readerConfig.put(TextFileReader.FILE_READER_TEXT_ENCODING, "invalid_charset");
        readerConfig.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, COMPRESSION_TYPE_DEFAULT);
        assertThrows(ConnectException.class, () -> getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig));
        assertThrows(UnsupportedCharsetException.class, () -> {
            try {
                getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readDataWithRecordPerLineDisabled(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFile(fsConfig, COMPRESSION_TYPE_DEFAULT);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
        readerConfig.put(TextFileReader.FILE_READER_TEXT_RECORD_PER_LINE, "false");
        readerConfig.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, COMPRESSION_TYPE_DEFAULT);
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(record, recordCount);
            recordCount++;
        }
        reader.close();
        assertEquals(1, recordCount, () -> "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readDifferentCompressionTypes(ReaderFsTestConfig fsConfig) {
        Arrays.stream(CompressionType.values()).forEach(compressionType -> {
            try {
                Path file = createDataFile(fsConfig, compressionType);
                Map<String, Object> readerConfig = getReaderConfig();
                readerConfig.put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
                readerConfig.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, compressionType);
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

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return TextFileReader.class;
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, COMPRESSION_TYPE_DEFAULT);
            put(TextFileReader.FILE_READER_TEXT_COMPRESSION_CONCATENATED, "true");
        }};
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertTrue(record.get(FIELD_NAME_VALUE).toString().startsWith(index + "_"));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
