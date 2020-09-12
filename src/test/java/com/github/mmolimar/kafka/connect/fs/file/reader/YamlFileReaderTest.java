package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class YamlFileReaderTest extends JacksonFileReaderTest {

    private static final String FILE_EXTENSION = "yl";

    protected static final int NUM_RECORDS = 1;

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        CompressionType compression = args.length < 3 ? COMPRESSION_TYPE_DEFAULT : (CompressionType) args[2];
        return super.createDataFile(fsConfig, 1, false, compression);
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
        getReader(fsConfig.getFs(), path, getReaderConfig());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllData(ReaderFsTestConfig fsConfig) {
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
    @Disabled
    public void seekFile(ReaderFsTestConfig fsConfig) {
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Disabled
    public void exceededSeek(ReaderFsTestConfig fsConfig) {

    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Disabled
    public void readAllDataInBatches(ReaderFsTestConfig fsConfig) {

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
    protected void checkData(Struct record, long index) {
        List<Struct> array = record.getArray(FIELD_ARRAY_COMPLEX);
        Struct subrecord = record.getStruct(FIELD_STRUCT);
        assertAll(
                () -> assertEquals(index, (int) record.get(FIELD_INTEGER)),
                () -> assertEquals(new BigInteger("9999999999999999999").longValue(), record.get(FIELD_BIG_INTEGER)),
                () -> assertEquals(Long.MAX_VALUE, (long) record.get(FIELD_LONG)),
                () -> assertTrue(record.get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(record.get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), (Double) record.get(FIELD_DECIMAL), 0),
                () -> assertNull(record.get(FIELD_NULL)),
                () -> assertNotNull(record.schema().field(FIELD_NULL)),
                () -> assertEquals("test", new String((byte[]) record.get(FIELD_BINARY))),
                () -> assertEquals(Arrays.asList("elm[" + index + "]", "elm[" + (index + 1) + "]"), record.get(FIELD_ARRAY_SIMPLE)),

                () -> assertEquals(index, (int) array.get(0).get(FIELD_INTEGER)),
                () -> assertEquals(Long.MAX_VALUE, (long) array.get(0).get(FIELD_LONG)),
                () -> assertTrue(array.get(0).get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(array.get(0).get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), (Double) array.get(0).get(FIELD_DECIMAL), 0),
                () -> assertNull(array.get(0).get(FIELD_NULL)),
                () -> assertNotNull(array.get(0).schema().field(FIELD_NULL)),
                () -> assertEquals(index + 1, (int) array.get(1).get(FIELD_INTEGER)),
                () -> assertEquals(Long.MAX_VALUE, (long) array.get(1).get(FIELD_LONG)),
                () -> assertTrue(array.get(1).get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(array.get(1).get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), (Double) array.get(1).get(FIELD_DECIMAL), 0),
                () -> assertNull(array.get(1).get(FIELD_NULL)),
                () -> assertNotNull(array.get(1).schema().field(FIELD_NULL)),

                () -> assertEquals(index, (int) subrecord.get(FIELD_INTEGER)),
                () -> assertEquals(Long.MAX_VALUE, (long) subrecord.get(FIELD_LONG)),
                () -> assertTrue(subrecord.get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(subrecord.get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), (Double) subrecord.get(FIELD_DECIMAL), 0),
                () -> assertNull(subrecord.get(FIELD_NULL)),
                () -> assertNotNull(subrecord.schema().field(FIELD_NULL))
        );
    }

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return YamlFileReader.class;
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

    @Override
    protected String readerEncodingConfig() {
        return YamlFileReader.FILE_READER_YAML_ENCODING;
    }

    @Override
    protected String recordPerLineConfig() {
        return "UNKNOWN";
    }

    @Override
    protected String compressionTypeConfig() {
        return YamlFileReader.FILE_READER_YAML_COMPRESSION_TYPE;
    }

    @Override
    protected String compressionConcatenatedConfig() {
        return YamlFileReader.FILE_READER_YAML_COMPRESSION_CONCATENATED;
    }

    @Override
    protected String deserializationConfigPrefix() {
        return YamlFileReader.FILE_READER_YAML_DESERIALIZATION_CONFIGS;
    }

    @Override
    protected ObjectMapper getObjectMapper() {
        return new YAMLMapper();
    }
}
