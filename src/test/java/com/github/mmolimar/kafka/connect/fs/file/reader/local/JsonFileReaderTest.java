package com.github.mmolimar.kafka.connect.fs.file.reader.local;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.AgnosticFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.JsonFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class JsonFileReaderTest extends LocalFileReaderTestBase {

    private static final String FIELD_INTEGER = "integerField";
    private static final String FIELD_LONG = "longField";
    private static final String FIELD_BOOLEAN = "booleanField";
    private static final String FIELD_STRING = "stringField";
    private static final String FIELD_DECIMAL = "decimalField";
    private static final String FIELD_ARRAY = "arrayField";
    private static final String FIELD_STRUCT = "structField";
    private static final String FIELD_NULL = "nullField";
    private static final String FILE_EXTENSION = "jsn";

    @BeforeClass
    public static void setUp() throws IOException {
        readerClass = AgnosticFileReader.class;
        dataFile = createDataFile();
        readerConfig = new HashMap<String, Object>() {{
            String deserializationConfig = DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT.name();
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_JSON, FILE_EXTENSION);
            put(JsonFileReader.FILE_READER_JSON_DESERIALIZATION_CONFIGS + deserializationConfig, "true");
            put(JsonFileReader.FILE_READER_JSON_DESERIALIZATION_CONFIGS + "invalid", "false");
        }};
    }

    private static Path createDataFile() throws IOException {
        return createDataFile(NUM_RECORDS, true);
    }

    private static Path createDataFile(int numRecords, boolean recordPerLine) throws IOException {
        File txtFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (FileWriter writer = new FileWriter(txtFile)) {
            IntStream.range(0, numRecords).forEach(index -> {
                ObjectNode json = JsonNodeFactory.instance.objectNode()
                        .put(FIELD_INTEGER, index)
                        .put(FIELD_LONG, Long.MAX_VALUE)
                        .put(FIELD_STRING, String.format("%d_%s", index, UUID.randomUUID()))
                        .put(FIELD_BOOLEAN, true)
                        .put(FIELD_DECIMAL, Double.parseDouble(index + "." + index))
                        .put(FIELD_NULL, (String) null);
                json.putArray(FIELD_ARRAY)
                        .add("elm[" + index + "]")
                        .add("elm[" + index + "]");
                json.putObject(FIELD_STRUCT)
                        .put(FIELD_INTEGER, (short) index)
                        .put(FIELD_LONG, Long.MAX_VALUE)
                        .put(FIELD_STRING, String.format("%d_%s", index, UUID.randomUUID()))
                        .put(FIELD_BOOLEAN, true)
                        .put(FIELD_DECIMAL, Double.parseDouble(index + "." + index))
                        .put(FIELD_NULL, (String) null);
                try {
                    writer.append(recordPerLine ? json.toString() + "\n" : json.toPrettyString());
                    OFFSETS_BY_INDEX.put(index, (long) index);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsUri), txtFile.getName());
        fs.moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @Ignore(value = "This test does not apply for json files")
    @Test(expected = IOException.class)
    public void emptyFile() throws Throwable {
        super.emptyFile();
    }

    @Test
    public void readEmptyFile() throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        FileReader reader = getReader(fs, path, readerConfig);
        assertFalse(reader.hasNext());
    }

    @Test
    public void validFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(JsonFileReader.FILE_READER_JSON_ENCODING, "Cp1252");
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_JSON, getFileExtension());
        }};
        reader = getReader(fs, dataFile, cfg);
        readAllData();
    }

    @Test(expected = UnsupportedCharsetException.class)
    public void invalidFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(JsonFileReader.FILE_READER_JSON_ENCODING, "invalid_charset");
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_JSON, getFileExtension());
        }};
        getReader(fs, dataFile, cfg);
    }

    @Test
    public void readDataWithRecordPerLineDisabled() throws Throwable {
        Path file = createDataFile(1, false);
        FileReader reader = getReader(fs, file, new HashMap<String, Object>() {{
            put(JsonFileReader.FILE_READER_JSON_RECORD_PER_LINE, "false");
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_JSON, getFileExtension());
        }});

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(record, recordCount);
            recordCount++;
        }
        reader.close();
        assertEquals("The number of records in the file does not match", 1, recordCount);
    }

    @Override
    protected Offset getOffset(long offset) {
        return () -> offset;
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertEquals((int) (Integer) record.get(FIELD_INTEGER), index);
        assertEquals((long) (Long) record.get(FIELD_LONG), Long.MAX_VALUE);
        assertTrue(record.get(FIELD_STRING).toString().startsWith(index + "_"));
        assertTrue(Boolean.parseBoolean(record.get(FIELD_BOOLEAN).toString()));
        assertEquals((Double) record.get(FIELD_DECIMAL), Double.parseDouble(index + "." + index), 0);
        assertNull(record.get(FIELD_NULL));
        assertNotNull(record.schema().field(FIELD_NULL));
        assertEquals(record.get(FIELD_ARRAY), Arrays.asList("elm[" + index + "]", "elm[" + index + "]"));
        Struct subrecord = record.getStruct(FIELD_STRUCT);
        assertEquals((int) (Integer) subrecord.get(FIELD_INTEGER), index);
        assertEquals((long) (Long) subrecord.get(FIELD_LONG), Long.MAX_VALUE);
        assertTrue(subrecord.get(FIELD_STRING).toString().startsWith(index + "_"));
        assertTrue(Boolean.parseBoolean(subrecord.get(FIELD_BOOLEAN).toString()));
        assertEquals((Double) subrecord.get(FIELD_DECIMAL), Double.parseDouble(index + "." + index), 0);
        assertNull(subrecord.get(FIELD_NULL));
        assertNotNull(subrecord.schema().field(FIELD_NULL));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

}
