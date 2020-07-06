package com.github.mmolimar.kafka.connect.fs.file.reader;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class AvroFileReader extends AbstractFileReader<GenericRecord> {

    private static final String FILE_READER_AVRO = FILE_READER_PREFIX + "avro.";

    public static final String FILE_READER_AVRO_SCHEMA = FILE_READER_AVRO + "schema";

    private final DataFileReader<GenericRecord> reader;
    private Schema schema;
    private boolean closed;

    public AvroFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new GenericRecordToStruct(), config);

        AvroFSInput input = new AvroFSInput(FileContext.getFileContext(filePath.toUri()), filePath);
        if (this.schema == null) {
            this.reader = new DataFileReader<>(input, new SpecificDatumReader<>());
        } else {
            this.reader = new DataFileReader<>(input, new SpecificDatumReader<>(this.schema));
        }
        this.closed = false;
    }

    @Override
    protected void configure(Map<String, String> config) {
        this.schema = Optional.ofNullable(config.get(FILE_READER_AVRO_SCHEMA))
                .map(c -> new Schema.Parser().parse(c))
                .orElse(null);
    }

    @Override
    public boolean hasNextRecord() {
        return reader.hasNext();
    }

    @Override
    protected GenericRecord nextRecord() {
        GenericRecord record = reader.next();
        incrementOffset();

        return record;
    }

    @Override
    public void seekFile(long offset) throws IOException {
        if (offset == currentOffset()) {
            return;
        } else if (offset < currentOffset()) {
            reader.sync(0L);
        }
        while (super.hasNext() && offset > currentOffset()) {
            super.next();
        }
        setOffset(offset);
    }

    @Override
    public void close() throws IOException {
        closed = true;
        reader.sync(0);
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
