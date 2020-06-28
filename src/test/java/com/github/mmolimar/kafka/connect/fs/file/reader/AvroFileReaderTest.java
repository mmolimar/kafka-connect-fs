package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class AvroFileReaderTest extends FileReaderTestBase {

    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_SURNAME = "surname";
    private static final String FILE_EXTENSION = "avr";

    private static Schema schema;

    @BeforeAll
    public static void setUp() throws IOException {
        schema = new Schema.Parser().parse(AvroFileReaderTest.class.getResourceAsStream("/file/reader/schemas/people.avsc"));
    }

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        File avroFile = File.createTempFile("test-", "." + getFileExtension());
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(writer)) {
            dataFileWriter.setFlushOnEveryBlock(true);
            dataFileWriter.setSyncInterval(32);
            dataFileWriter.create(schema, avroFile);

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                GenericRecord datum = new GenericData.Record(schema);
                datum.put(FIELD_INDEX, index);
                datum.put(FIELD_NAME, String.format("%d_name_%s", index, UUID.randomUUID()));
                datum.put(FIELD_SURNAME, String.format("%d_surname_%s", index, UUID.randomUUID()));
                try {
                    fsConfig.offsetsByIndex().put(index, dataFileWriter.sync() - 16L);
                    dataFileWriter.append(datum);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), avroFile.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(avroFile.getAbsolutePath()), path);
        return path;
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readerWithSchema(ReaderFsTestConfig fsConfig) throws IOException {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(AvroFileReader.FILE_READER_AVRO_SCHEMA, schema.toString());
        FileSystem testFs = FileSystem.newInstance(fsConfig.getFsUri(), new Configuration());
        fsConfig.setReader(getReader(testFs, fsConfig.getDataFile(), readerConfig));
        readAllData(fsConfig);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readerWithInvalidSchema(ReaderFsTestConfig fsConfig) throws IOException {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(AvroFileReader.FILE_READER_AVRO_SCHEMA, Schema.create(Schema.Type.STRING).toString());
        FileSystem testFs = FileSystem.newInstance(fsConfig.getFsUri(), new Configuration());
        fsConfig.setReader(getReader(testFs, fsConfig.getDataFile(), readerConfig));
        assertThrows(ConnectException.class, () -> readAllData(fsConfig));
        assertThrows(AvroTypeException.class, () -> {
            try {
                readAllData(fsConfig);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readerWithUnparseableSchema(ReaderFsTestConfig fsConfig) throws IOException {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(AvroFileReader.FILE_READER_AVRO_SCHEMA, "invalid schema");
        FileSystem testFs = FileSystem.newInstance(fsConfig.getFsUri(), new Configuration());
        assertThrows(ConnectException.class, () -> getReader(testFs, fsConfig.getDataFile(), readerConfig));
        assertThrows(SchemaParseException.class, () -> {
            try {
                getReader(testFs, fsConfig.getDataFile(), readerConfig);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return AvroFileReader.class;
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<>();
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertAll(
                () -> assertEquals(index, (int) record.get(FIELD_INDEX)),
                () -> assertTrue(record.get(FIELD_NAME).toString().startsWith(index + "_")),
                () -> assertTrue(record.get(FIELD_SURNAME).toString().startsWith(index + "_"))
        );
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
