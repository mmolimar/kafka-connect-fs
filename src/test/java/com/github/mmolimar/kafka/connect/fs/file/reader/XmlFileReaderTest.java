package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.connect.data.Struct;

import static org.junit.jupiter.api.Assertions.*;

public class XmlFileReaderTest extends JacksonFileReaderTest {

    private static final String FILE_EXTENSION = "xl";

    @Override
    protected void checkData(Struct record, long index) {
        Struct array = record.getStruct(FIELD_ARRAY_COMPLEX);
        Struct subrecord = record.getStruct(FIELD_STRUCT);
        assertAll(
                () -> assertEquals(index, Integer.parseInt(record.getString(FIELD_INTEGER))),
                () -> assertEquals("9999999999999999999", record.get(FIELD_BIG_INTEGER)),
                () -> assertEquals(Long.MAX_VALUE, Long.parseLong(record.getString(FIELD_LONG))),
                () -> assertTrue(record.get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(record.get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), Double.parseDouble(record.getString(FIELD_DECIMAL))),
                () -> assertNull(record.get(FIELD_NULL)),
                () -> assertNotNull(record.schema().field(FIELD_NULL)),
                () -> assertEquals("dGVzdA==", record.get(FIELD_BINARY)),
                () -> assertEquals("elm[" + (index + 1) + "]", record.get(FIELD_ARRAY_SIMPLE)),

                () -> assertEquals(index + 1, Integer.parseInt(array.getString(FIELD_INTEGER))),
                () -> assertEquals(Long.MAX_VALUE, Long.parseLong(array.getString(FIELD_LONG))),
                () -> assertTrue(array.get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(array.get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), Double.parseDouble(array.getString(FIELD_DECIMAL))),
                () -> assertNull(array.get(FIELD_NULL)),
                () -> assertNotNull(array.schema().field(FIELD_NULL)),
                () -> assertEquals(index + 1, Integer.parseInt(array.getString(FIELD_INTEGER))),
                () -> assertEquals(Long.MAX_VALUE, Long.parseLong(array.getString(FIELD_LONG))),
                () -> assertTrue(array.get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(array.get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), Double.parseDouble(array.getString(FIELD_DECIMAL))),
                () -> assertNull(array.get(FIELD_NULL)),
                () -> assertNotNull(array.schema().field(FIELD_NULL)),

                () -> assertEquals(index, Integer.parseInt(subrecord.getString(FIELD_INTEGER))),
                () -> assertEquals(Long.MAX_VALUE, Long.parseLong(subrecord.getString(FIELD_LONG))),
                () -> assertTrue(subrecord.get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(subrecord.get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), Double.parseDouble(subrecord.getString(FIELD_DECIMAL))),
                () -> assertNull(subrecord.get(FIELD_NULL)),
                () -> assertNotNull(subrecord.schema().field(FIELD_NULL))
        );
    }

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return XmlFileReader.class;
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

    @Override
    protected String readerEncodingConfig() {
        return XmlFileReader.FILE_READER_XML_ENCODING;
    }

    @Override
    protected String recordPerLineConfig() {
        return XmlFileReader.FILE_READER_XML_RECORD_PER_LINE;
    }

    @Override
    protected String compressionTypeConfig() {
        return XmlFileReader.FILE_READER_XML_COMPRESSION_TYPE;
    }

    @Override
    protected String compressionConcatenatedConfig() {
        return XmlFileReader.FILE_READER_XML_COMPRESSION_CONCATENATED;
    }

    @Override
    protected String deserializationConfigPrefix() {
        return XmlFileReader.FILE_READER_XML_DESERIALIZATION_CONFIGS;
    }

    @Override
    protected ObjectMapper getObjectMapper() {
        return new XmlMapper();
    }
}
