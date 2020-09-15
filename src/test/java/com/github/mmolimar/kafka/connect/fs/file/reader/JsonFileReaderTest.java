package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Struct;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JsonFileReaderTest extends JacksonFileReaderTest {

    private static final String FILE_EXTENSION = "jsn";

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
                () -> assertEquals("dGVzdA==", record.get(FIELD_BINARY)),
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
        return JsonFileReader.class;
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

    @Override
    protected String readerEncodingConfig() {
        return JsonFileReader.FILE_READER_JSON_ENCODING;
    }

    @Override
    protected String recordPerLineConfig() {
        return JsonFileReader.FILE_READER_JSON_RECORD_PER_LINE;
    }

    @Override
    protected String compressionTypeConfig() {
        return JsonFileReader.FILE_READER_JSON_COMPRESSION_TYPE;
    }

    @Override
    protected String compressionConcatenatedConfig() {
        return JsonFileReader.FILE_READER_JSON_COMPRESSION_CONCATENATED;
    }

    @Override
    protected String deserializationConfigPrefix() {
        return JsonFileReader.FILE_READER_JSON_DESERIALIZATION_CONFIGS;
    }

    @Override
    protected ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }
}
