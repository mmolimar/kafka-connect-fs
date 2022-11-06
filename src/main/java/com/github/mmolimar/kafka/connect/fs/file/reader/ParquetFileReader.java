package com.github.mmolimar.kafka.connect.fs.file.reader;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class ParquetFileReader extends AbstractFileReader<GenericRecord> {

    private static final String FILE_READER_PARQUET = FILE_READER_PREFIX + "parquet.";

    public static final String FILE_READER_PARQUET_SCHEMA = FILE_READER_PARQUET + "schema";
    public static final String FILE_READER_PARQUET_PROJECTION = FILE_READER_PARQUET + "projection";

    private ParquetReader<GenericRecord> reader;
    private GenericRecord currentRecord;
    private Schema schema;
    private Schema projection;
    private boolean closed;

    public ParquetFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new GenericRecordToStruct(), config);

        this.reader = initReader();
        this.closed = false;
    }

    private ParquetReader<GenericRecord> initReader() throws IOException {
        Configuration configuration = getFs().getConf();
        if (this.schema != null) {
            AvroReadSupport.setAvroReadSchema(configuration, this.schema);
        }
        if (this.projection != null) {
            AvroReadSupport.setRequestedProjection(configuration, this.projection);
        }
        return AvroParquetReader
                .<GenericRecord>builder(HadoopInputFile.fromPath(getFilePath(), configuration))
                .build();
    }

    protected void configure(Map<String, String> config) {
        this.schema = Optional.ofNullable(config.get(FILE_READER_PARQUET_SCHEMA))
                .map(c -> new Schema.Parser().parse(c))
                .orElse(null);
        this.projection = Optional.ofNullable(config.get(FILE_READER_PARQUET_PROJECTION))
                .map(c -> new Schema.Parser().parse(c))
                .orElse(null);
    }

    @Override
    public boolean hasNextRecord() throws IOException {
        if (currentRecord == null) {
            currentRecord = reader.read();
        }
        return currentRecord != null;
    }

    @Override
    protected GenericRecord nextRecord() {
        GenericRecord record;
        if (this.projection != null) {
            record = new GenericData.Record(this.projection);
            this.projection.getFields().forEach(field -> record.put(field.name(), currentRecord.get(field.name())));
        } else {
            record = currentRecord;
        }
        currentRecord = null;
        incrementOffset();
        return record;
    }

    @Override
    public void seekFile(long offset) throws IOException {
        if (currentOffset() > offset) {
            this.reader = initReader();
            this.closed = false;
            setOffset(0);
        }
        while (hasNextRecord() && currentOffset() < offset) {
            nextRecord();
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        reader.close();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    static class GenericRecordToStruct implements ReaderAdapter<GenericRecord> {
        private static final int CACHE_SIZE = 100;
        private final AvroData avroData;

        GenericRecordToStruct() {
            this.avroData = new AvroData(CACHE_SIZE);
        }

        @Override
        public Struct apply(GenericRecord record) {
            return (Struct) avroData.toConnectData(record.getSchema(), record).value();
        }
    }
}
