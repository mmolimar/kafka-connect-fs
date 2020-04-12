package com.github.mmolimar.kafka.connect.fs.file.reader;

import io.confluent.connect.avro.AvroData;
import org.apache.avro.AvroRuntimeException;
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
import java.util.Optional;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class AvroFileReader extends AbstractFileReader<GenericRecord> {

    private static final String FILE_READER_AVRO = FILE_READER_PREFIX + "avro.";

    public static final String FILE_READER_AVRO_SCHEMA = FILE_READER_AVRO + "schema";

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
    }

    @Override
    protected void configure(Map<String, String> config) {
        this.schema = Optional.ofNullable(config.get(FILE_READER_AVRO_SCHEMA))
                .map(c -> new Schema.Parser().parse(c))
                .orElse(null);
    }

    @Override
    public boolean hasNext() {
        try {
            return reader.hasNext();
        } catch (AvroRuntimeException are) {
            throw new IllegalStateException(are);
        }
    }

    @Override
    protected GenericRecord nextRecord() {
        try {
            GenericRecord record = reader.next();
            incrementOffset();

            return record;
        } catch (AvroRuntimeException are) {
            throw new IllegalStateException(are);
        }
    }

    @Override
    public void seek(long offset) {
        try {
            reader.sync(offset);
            setOffset(reader.previousSync() - 16L);
        } catch (IOException ioe) {
            throw new ConnectException("Error seeking file " + getFilePath(), ioe);
        }
    }

    @Override
    public void close() throws IOException {
        reader.sync(0);
        reader.close();
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
