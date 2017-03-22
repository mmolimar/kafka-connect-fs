package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.ParquetFileReader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class ParquetFileReaderTest extends HdfsFileReaderTestBase {

    private static final String FIELD_INDEX = "index";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_SURNAME = "surname";

    @BeforeClass
    public static void setUp() throws IOException {
        readerClass = ParquetFileReader.class;
        dataFile = createDataFile();
        readerConfig = new HashMap<>();
    }

    private static Path createDataFile() throws IOException {
        File parquetFile = File.createTempFile("test-", ".parquet");
        Schema avroSchema = new Schema.Parser().parse(ParquetFileReaderTest.class.getResourceAsStream("/file/reader/schemas/people.avsc"));

        try (ParquetWriter writer = AvroParquetWriter.<GenericRecord>builder(new Path(parquetFile.toURI()))
                .withConf(fs.getConf()).withWriteMode(ParquetFileWriter.Mode.OVERWRITE).withSchema(avroSchema).build()) {

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                GenericRecord datum = new GenericData.Record(avroSchema);
                datum.put(FIELD_INDEX, index);
                datum.put(FIELD_NAME, String.format("%d_name_%s", index, UUID.randomUUID()));
                datum.put(FIELD_SURNAME, String.format("%d_surname_%s", index, UUID.randomUUID()));
                try {
                    OFFSETS_BY_INDEX.put(index, Long.valueOf(index));
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

    @Override
    protected Offset getOffset(long offset) {
        return new ParquetFileReader.ParquetOffset(offset);
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertTrue((Integer) record.get(FIELD_INDEX) == index);
        assertTrue(record.get(FIELD_NAME).toString().startsWith(index + "_"));
        assertTrue(record.get(FIELD_SURNAME).toString().startsWith(index + "_"));
    }
}
