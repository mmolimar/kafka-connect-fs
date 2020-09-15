package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

abstract class JacksonFileReaderTest extends FileReaderTestBase {

    static final String FIELD_INTEGER = "integerField";
    static final String FIELD_BIG_INTEGER = "bigIntegerField";
    static final String FIELD_LONG = "longField";
    static final String FIELD_BOOLEAN = "booleanField";
    static final String FIELD_STRING = "stringField";
    static final String FIELD_DECIMAL = "decimalField";
    static final String FIELD_BINARY = "binaryField";
    static final String FIELD_ARRAY_SIMPLE = "arraySimpleField";
    static final String FIELD_ARRAY_COMPLEX = "arrayComplexField";
    static final String FIELD_STRUCT = "structField";
    static final String FIELD_NULL = "nullField";
    static final CompressionType COMPRESSION_TYPE_DEFAULT = CompressionType.NONE;

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        int numRecords = args.length < 1 ? NUM_RECORDS : (int) args[0];
        boolean recordPerLine = args.length < 2 || (boolean) args[1];
        CompressionType compression = args.length < 3 ? COMPRESSION_TYPE_DEFAULT : (CompressionType) args[2];
        File txtFile = File.createTempFile("test-", "." + getFileExtension());
        try (PrintWriter writer = new PrintWriter(getOutputStream(txtFile, compression))) {
            ObjectWriter objectWriter = getObjectMapper().writerWithDefaultPrettyPrinter();
            IntStream.range(0, numRecords).forEach(index -> {
                ObjectNode node = JsonNodeFactory.instance.objectNode()
                        .put(FIELD_INTEGER, index)
                        .put(FIELD_BIG_INTEGER, new BigInteger("9999999999999999999"))
                        .put(FIELD_LONG, Long.MAX_VALUE)
                        .put(FIELD_STRING, String.format("%d_%s", index, UUID.randomUUID()))
                        .put(FIELD_BOOLEAN, true)
                        .put(FIELD_DECIMAL, Double.parseDouble(index + "." + index))
                        .put(FIELD_BINARY, "test".getBytes())
                        .put(FIELD_NULL, (String) null);
                node.putArray(FIELD_ARRAY_SIMPLE)
                        .add("elm[" + index + "]")
                        .add("elm[" + (index + 1) + "]");
                ArrayNode array = node.putArray(FIELD_ARRAY_COMPLEX);
                array.addObject()
                        .put(FIELD_INTEGER, index)
                        .put(FIELD_LONG, Long.MAX_VALUE)
                        .put(FIELD_STRING, String.format("%d_%s", index, UUID.randomUUID()))
                        .put(FIELD_BOOLEAN, true)
                        .put(FIELD_DECIMAL, Double.parseDouble(index + "." + index))
                        .put(FIELD_NULL, (String) null);
                array.addObject()
                        .put(FIELD_INTEGER, index + 1)
                        .put(FIELD_LONG, Long.MAX_VALUE)
                        .put(FIELD_STRING, String.format("%d_%s", index, UUID.randomUUID()))
                        .put(FIELD_BOOLEAN, true)
                        .put(FIELD_DECIMAL, Double.parseDouble(index + "." + index))
                        .put(FIELD_NULL, (String) null);
                node.putObject(FIELD_STRUCT)
                        .put(FIELD_INTEGER, (short) index)
                        .put(FIELD_LONG, Long.MAX_VALUE)
                        .put(FIELD_STRING, String.format("%d_%s", index, UUID.randomUUID()))
                        .put(FIELD_BOOLEAN, true)
                        .put(FIELD_DECIMAL, Double.parseDouble(index + "." + index))
                        .put(FIELD_NULL, (String) null);
                try {
                    writer.append(recordPerLine ? objectWriter.writeValueAsString(node).replaceAll("\n", "") + "\n" : objectWriter.writeValueAsString(node));
                } catch (JsonProcessingException jpe) {
                    throw new RuntimeException(jpe);
                }
                fsConfig.offsetsByIndex().put(index, (long) index);
            });
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), txtFile.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
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
    public void validFileEncoding(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(readerEncodingConfig(), "Cp1252");
        fsConfig.setReader(getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig));
        readAllData(fsConfig);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidDeserializationConfig(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(deserializationConfigPrefix() + "invalid", "false");
        fsConfig.setReader(getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig));
        readAllData(fsConfig);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidFileEncoding(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(readerEncodingConfig(), "invalid_charset");
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
        Path file = createDataFile(fsConfig, 1, false);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(recordPerLineConfig(), "false");
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(record, recordCount);
            recordCount++;
        }
        reader.close();
        assertEquals(1, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readDifferentCompressionTypes(ReaderFsTestConfig fsConfig) {
        Arrays.stream(CompressionType.values()).forEach(compressionType -> {
            try {
                Path file = createDataFile(fsConfig, NUM_RECORDS, true, compressionType);
                Map<String, Object> readerConfig = getReaderConfig();
                readerConfig.put(compressionTypeConfig(), compressionType.toString());
                readerConfig.put(compressionConcatenatedConfig(), "true");
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
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<String, Object>() {{
            String deserializationConfig = DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT.name();
            put(deserializationConfigPrefix() + deserializationConfig, "true");
        }};
    }

    protected abstract String readerEncodingConfig();

    protected abstract String recordPerLineConfig();

    protected abstract String compressionTypeConfig();

    protected abstract String compressionConcatenatedConfig();

    protected abstract String deserializationConfigPrefix();

    protected abstract ObjectMapper getObjectMapper();

}
