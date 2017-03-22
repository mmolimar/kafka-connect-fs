package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import io.confluent.connect.avro.AvroData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

public class ParquetFileReader extends AbstractFileReader<GenericRecord> {

    private final ParquetOffset offset;

    private ParquetReader<GenericRecord> reader;
    private GenericRecord currentRecord;
    private boolean closed;


    public ParquetFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new GenericRecordToStruct(), config);

        this.offset = new ParquetOffset(0);
        this.reader = initReader();
        this.closed = false;
    }

    private ParquetReader<GenericRecord> initReader() throws IOException {
        ParquetReader reader = AvroParquetReader.<GenericRecord>builder(getFilePath())
                .withConf(getFs().getConf()).build();
        return reader;
    }

    protected void configure(Map<String, Object> config) {
    }

    @Override
    public boolean hasNext() {
        if (closed) return false;
        if (currentRecord == null) {
            try {
                currentRecord = reader.read();
                if (currentRecord != null) offset.inc();
            } catch (IOException ioe) {
                throw new ConnectException("Error reading parquet record", ioe);
            }
        }
        return currentRecord != null;
    }

    @Override
    protected GenericRecord nextRecord() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());
        }
        GenericRecord aux = currentRecord;
        currentRecord = null;
        return aux;
    }

    @Override
    public void seek(Offset offset) {
        if (closed) {
            throw new ConnectException("Stream is closed!");
        }
        if (offset.getRecordOffset() < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }
        if (this.offset.getRecordOffset() > offset.getRecordOffset()) {
            try {
                this.reader = initReader();
                this.offset.setOffset(0);
                this.closed = false;
            } catch (IOException ioe) {
                throw new ConnectException("Error initializing parquet reader", ioe);
            }
        }
        while (hasNext() && this.offset.getRecordOffset() <= offset.getRecordOffset()) {
            nextRecord();
        }
    }

    @Override
    public Offset currentOffset() {
        return offset;
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
        reader.close();
    }

    public static class ParquetOffset implements Offset {
        private long offset;

        public ParquetOffset(long offset) {
            this.offset = offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        protected void inc() {
            this.offset++;
        }

        @Override
        public long getRecordOffset() {
            return offset;
        }
    }

    static class GenericRecordToStruct implements ReaderAdapter<GenericRecord> {
        static final int CACHE_SIZE = 100;
        AvroData avroData;

        public GenericRecordToStruct() {
            this.avroData = new AvroData(CACHE_SIZE);
        }

        @Override
        public Struct apply(GenericRecord record) {
            return (Struct) avroData.toConnectData(record.getSchema(), record).value();
        }
    }
}
