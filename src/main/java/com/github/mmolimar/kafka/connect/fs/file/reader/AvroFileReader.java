package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
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
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.util.Map;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class AvroFileReader extends AbstractFileReader<GenericRecord> {

    private static final String FILE_READER_AVRO = FILE_READER_PREFIX + "avro.";

    public static final String FILE_READER_AVRO_SCHEMA = FILE_READER_AVRO + "schema";

    private final AvroOffset offset;
    private final DataFileReader<GenericRecord> reader;
    private Schema schema;

    public AvroFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new GenericRecordToStruct(), config);

        AvroFSInput input = new AvroFSInput(FileContext.getFileContext(filePath.toUri()), filePath);
        if (this.schema == null) {
            this.reader = new DataFileReader<>(input, new SpecificDatumReader<>());
        } else {
            this.reader = new DataFileReader<>(input, new SpecificDatumReader<>(this.schema));
        }
        this.offset = new AvroOffset(0);
    }

    protected void configure(Map<String, Object> config) {
        if (config.get(FILE_READER_AVRO_SCHEMA) != null) {
            this.schema = new Schema.Parser().parse(config.get(FILE_READER_AVRO_SCHEMA).toString());
        } else {
            this.schema = null;
        }
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    protected GenericRecord nextRecord() {
        GenericRecord record = reader.next();
        this.offset.inc();

        return record;
    }

    @Override
    public void seek(Offset offset) {
        try {
            reader.sync(offset.getRecordOffset());
            this.offset.setOffset(reader.previousSync() - 15);
        } catch (IOException ioe) {
            throw new ConnectException("Error seeking file " + getFilePath(), ioe);
        }
    }

    @Override
    public Offset currentOffset() {
        return offset;
    }

    @Override
    public void close() throws IOException {
        reader.sync(0);
        reader.close();
    }

    public static class AvroOffset implements Offset {
        private long offset;

        public AvroOffset(long offset) {
            this.offset = offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        void inc() {
            this.offset++;
        }

        @Override
        public long getRecordOffset() {
            return offset;
        }
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
