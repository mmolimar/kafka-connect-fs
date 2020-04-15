package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.InvalidRecordException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class ParquetFileReaderTest extends FileReaderTestBase {

    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_SURNAME = "surname";
    private static final String FILE_EXTENSION = "parquet";

    private static Schema readerSchema;
    private static Schema projectionSchema;

    @BeforeAll
    public static void setUp() throws IOException {
        readerSchema = new Schema.Parser().parse(
                ParquetFileReaderTest.class.getResourceAsStream("/file/reader/schemas/people.avsc"));
        projectionSchema = new Schema.Parser().parse(
                ParquetFileReaderTest.class.getResourceAsStream("/file/reader/schemas/people_projection.avsc"));
    }

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        FileSystem fs = fsConfig.getFs();
        File parquetFile = File.createTempFile("test-", "." + getFileExtension());

        try (ParquetWriter writer = AvroParquetWriter.<GenericRecord>builder(new Path(parquetFile.toURI()))
                .withConf(fs.getConf()).withWriteMode(ParquetFileWriter.Mode.OVERWRITE).withSchema(readerSchema).build()) {
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                GenericRecord datum = new GenericData.Record(readerSchema);
                datum.put(FIELD_INDEX, index);
                String uuid = UUID.randomUUID().toString();
                datum.put(FIELD_NAME, String.format("%d_name_%s", index, uuid));
                datum.put(FIELD_SURNAME, String.format("%d_surname_%s", index, uuid));
                try {
                    fsConfig.offsetsByIndex().put(index, (long) index);
                    writer.write(datum);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), parquetFile.getName());
        fs.moveFromLocalFile(new Path(parquetFile.getAbsolutePath()), path);
        return path;
    }

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
        getReader(fsConfig.getFs(), path, getReaderConfig());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readerWithSchema(ReaderFsTestConfig fsConfig) throws IOException {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(ParquetFileReader.FILE_READER_PARQUET_SCHEMA, readerSchema.toString());
        readerConfig.put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET, getFileExtension());
        FileSystem testFs = FileSystem.newInstance(fsConfig.getFsUri(), new Configuration());
        fsConfig.setReader(getReader(testFs, fsConfig.getDataFile(), readerConfig));
        readAllData(fsConfig);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readerWithProjection(ReaderFsTestConfig fsConfig) throws IOException {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(ParquetFileReader.FILE_READER_PARQUET_PROJECTION, projectionSchema.toString());
        readerConfig.put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET, getFileExtension());
        fsConfig.setReader(getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig));
        while (fsConfig.getReader().hasNext()) {
            Struct record = fsConfig.getReader().next();
            assertNotNull(record.schema().field(FIELD_INDEX));
            assertNotNull(record.schema().field(FIELD_NAME));
            assertNull(record.schema().field(FIELD_SURNAME));
        }
        FileSystem testFs = FileSystem.newInstance(fsConfig.getFsUri(), new Configuration());
        fsConfig.setReader(getReader(testFs, fsConfig.getDataFile(), readerConfig));
        assertThrows(DataException.class, () -> readAllData(fsConfig));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readerWithInvalidProjection(ReaderFsTestConfig fsConfig) throws IOException {
        Schema testSchema = SchemaBuilder.record("test_projection").namespace("test.avro")
                .fields()
                .name("field1").type("string").noDefault()
                .endRecord();
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(ParquetFileReader.FILE_READER_PARQUET_PROJECTION, testSchema.toString());
        readerConfig.put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET, getFileExtension());
        FileSystem testFs = FileSystem.newInstance(fsConfig.getFsUri(), new Configuration());
        fsConfig.setReader(getReader(testFs, fsConfig.getDataFile(), readerConfig));
        try {
            readAllData(fsConfig);
        } catch (Exception e) {
            assertEquals(ConnectException.class, e.getClass());
            assertEquals(InvalidRecordException.class, e.getCause().getClass());
        }
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readerWithInvalidSchema(ReaderFsTestConfig fsConfig) throws IOException {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(ParquetFileReader.FILE_READER_PARQUET_SCHEMA, Schema.create(Schema.Type.STRING).toString());
        readerConfig.put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET, getFileExtension());
        FileSystem testFs = FileSystem.newInstance(fsConfig.getFsUri(), new Configuration());
        fsConfig.setReader(getReader(testFs, fsConfig.getDataFile(), readerConfig));
        try {
            readAllData(fsConfig);
        } catch (Exception e) {
            assertEquals(ConnectException.class, e.getClass());
            assertEquals(AvroRuntimeException.class, e.getCause().getClass());
        }
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readerWithUnparseableSchema(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(ParquetFileReader.FILE_READER_PARQUET_SCHEMA, "invalid schema");
        readerConfig.put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET, getFileExtension());
        assertThrows(ConnectException.class, () ->
                getReader(FileSystem.newInstance(fsConfig.getFsUri(), new Configuration()),
                        fsConfig.getDataFile(), readerConfig));
        assertThrows(SchemaParseException.class, () -> {
            try {
                getReader(FileSystem.newInstance(fsConfig.getFsUri(), new Configuration()),
                        fsConfig.getDataFile(), readerConfig);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<>();
    }

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return ParquetFileReader.class;
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertEquals((int) (Integer) record.get(FIELD_INDEX), index);
        assertTrue(record.get(FIELD_NAME).toString().startsWith(index + "_"));
        assertTrue(record.get(FIELD_SURNAME).toString().startsWith(index + "_"));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

}
