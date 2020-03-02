package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.AgnosticFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.ParquetFileReader;
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
import org.apache.kafka.connect.errors.DataException;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.InvalidRecordException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class ParquetFileReaderTest extends HdfsFileReaderTestBase {

    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_SURNAME = "surname";
    private static final String FILE_EXTENSION = "parquet";

    private static Schema readerSchema;
    private static Schema projectionSchema;

    @BeforeAll
    public static void setUp() throws IOException {
        readerClass = AgnosticFileReader.class;
        dataFile = createDataFile();
        readerConfig = new HashMap<>();
    }

    private static Path createDataFile() throws IOException {
        File parquetFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        readerSchema = new Schema.Parser().parse(
                ParquetFileReaderTest.class.getResourceAsStream("/file/reader/schemas/people.avsc"));
        projectionSchema = new Schema.Parser().parse(
                ParquetFileReaderTest.class.getResourceAsStream("/file/reader/schemas/people_projection.avsc"));

        try (ParquetWriter writer = AvroParquetWriter.<GenericRecord>builder(new Path(parquetFile.toURI()))
                .withConf(fs.getConf()).withWriteMode(ParquetFileWriter.Mode.OVERWRITE).withSchema(readerSchema).build()) {
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                GenericRecord datum = new GenericData.Record(readerSchema);
                datum.put(FIELD_INDEX, index);
                datum.put(FIELD_NAME, String.format("%d_name_%s", index, UUID.randomUUID()));
                datum.put(FIELD_SURNAME, String.format("%d_surname_%s", index, UUID.randomUUID()));
                try {
                    OFFSETS_BY_INDEX.put(index, (long) index);
                    writer.write(datum);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsUri), parquetFile.getName());
        fs.moveFromLocalFile(new Path(parquetFile.getAbsolutePath()), path);
        return path;
    }

    @Test
    public void emptyFile() throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fs, path, readerConfig);
    }

    @Test
    public void invalidFileFormat() throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
            writer.write("test");
        }
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fs, path, readerConfig);
    }

    @Test
    public void readerWithSchema() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(ParquetFileReader.FILE_READER_PARQUET_SCHEMA, readerSchema.toString());
        }};
        reader = getReader(FileSystem.newInstance(fsUri, new Configuration()), dataFile, cfg);
        readAllData();
    }

    @Test
    public void readerWithProjection() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(ParquetFileReader.FILE_READER_PARQUET_PROJECTION, projectionSchema.toString());
        }};
        reader = getReader(FileSystem.newInstance(fsUri, new Configuration()), dataFile, cfg);
        while (reader.hasNext()) {
            Struct record = reader.next();
            assertNotNull(record.schema().field(FIELD_INDEX));
            assertNotNull(record.schema().field(FIELD_NAME));
            assertNull(record.schema().field(FIELD_SURNAME));
        }

        reader = getReader(FileSystem.newInstance(fsUri, new Configuration()), dataFile, cfg);
        assertThrows(DataException.class, this::readAllData);
    }

    @Test
    public void readerWithInvalidProjection() throws Throwable {
        Schema testSchema = SchemaBuilder.record("test_projection").namespace("test.avro")
                .fields()
                .name("field1").type("string").noDefault()
                .endRecord();
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(ParquetFileReader.FILE_READER_PARQUET_PROJECTION, testSchema.toString());
        }};
        reader = getReader(FileSystem.newInstance(fsUri, new Configuration()), dataFile, cfg);
        assertThrows(InvalidRecordException.class, this::readAllData);
    }

    @Test
    public void readerWithInvalidSchema() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(ParquetFileReader.FILE_READER_PARQUET_SCHEMA, Schema.create(Schema.Type.STRING).toString());
        }};
        reader = getReader(FileSystem.newInstance(fsUri, new Configuration()), dataFile, cfg);
        assertThrows(AvroRuntimeException.class, this::readAllData);
    }

    @Test
    public void readerWithUnparseableSchema() {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(ParquetFileReader.FILE_READER_PARQUET_SCHEMA, "invalid schema");
        }};
        assertThrows(SchemaParseException.class, () ->
                getReader(FileSystem.newInstance(fsUri, new Configuration()), dataFile, cfg));
    }

    @Override
    protected Offset getOffset(long offset) {
        return new ParquetFileReader.ParquetOffset(offset);
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
