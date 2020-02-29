package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.EOFException;
import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class SequenceFileReader extends AbstractFileReader<SequenceFileReader.SequenceRecord<Writable, Writable>> {

    public static final String FIELD_NAME_KEY_DEFAULT = "key";
    public static final String FIELD_NAME_VALUE_DEFAULT = "value";

    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private static final String FILE_READER_SEQUENCE = FILE_READER_PREFIX + "sequence.";
    private static final String FILE_READER_SEQUENCE_FIELD_NAME_PREFIX = FILE_READER_SEQUENCE + "field_name.";

    public static final String FILE_READER_BUFFER_SIZE = FILE_READER_SEQUENCE + "buffer_size";
    public static final String FILE_READER_SEQUENCE_FIELD_NAME_KEY = FILE_READER_SEQUENCE_FIELD_NAME_PREFIX + "key";
    public static final String FILE_READER_SEQUENCE_FIELD_NAME_VALUE = FILE_READER_SEQUENCE_FIELD_NAME_PREFIX + "value";

    private final SequenceFile.Reader reader;
    private final Writable key, value;
    private final SeqOffset offset;
    private final Schema schema;
    private String keyFieldName, valueFieldName;
    private long recordIndex, hasNextIndex;
    private boolean hasNext;

    public SequenceFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new SeqToStruct(), config);

        this.reader = new SequenceFile.Reader(fs.getConf(),
                SequenceFile.Reader.file(filePath),
                SequenceFile.Reader.bufferSize(fs.getConf().getInt(FILE_READER_BUFFER_SIZE, DEFAULT_BUFFER_SIZE)));
        this.key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
        this.value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());
        this.schema = SchemaBuilder.struct()
                .field(keyFieldName, getSchema(this.key))
                .field(valueFieldName, getSchema(this.value))
                .build();
        this.offset = new SeqOffset(0);
        this.recordIndex = this.hasNextIndex = -1;
        this.hasNext = false;
    }

    @Override
    protected void configure(Map<String, Object> config) {
        if (config.get(FILE_READER_SEQUENCE_FIELD_NAME_KEY) == null ||
                config.get(FILE_READER_SEQUENCE_FIELD_NAME_KEY).toString().equals("")) {
            this.keyFieldName = FIELD_NAME_KEY_DEFAULT;
        } else {
            this.keyFieldName = config.get(FILE_READER_SEQUENCE_FIELD_NAME_KEY).toString();
        }
        if (config.get(FILE_READER_SEQUENCE_FIELD_NAME_VALUE) == null ||
                config.get(FILE_READER_SEQUENCE_FIELD_NAME_VALUE).toString().equals("")) {
            this.valueFieldName = FIELD_NAME_VALUE_DEFAULT;
        } else {
            this.valueFieldName = config.get(FILE_READER_SEQUENCE_FIELD_NAME_VALUE).toString();
        }
    }

    private Schema getSchema(Writable writable) {
        if (writable instanceof ByteWritable) {
            return SchemaBuilder.INT8_SCHEMA;
        } else if (writable instanceof ShortWritable) {
            return SchemaBuilder.INT16_SCHEMA;
        } else if (writable instanceof IntWritable) {
            return SchemaBuilder.INT32_SCHEMA;
        } else if (writable instanceof LongWritable) {
            return SchemaBuilder.INT64_SCHEMA;
        } else if (writable instanceof FloatWritable) {
            return SchemaBuilder.FLOAT32_SCHEMA;
        } else if (writable instanceof DoubleWritable) {
            return SchemaBuilder.INT64_SCHEMA;
        } else if (writable instanceof BytesWritable) {
            return SchemaBuilder.BYTES_SCHEMA;
        } else if (writable instanceof BooleanWritable) {
            return SchemaBuilder.BOOLEAN_SCHEMA;
        }
        return SchemaBuilder.STRING_SCHEMA;
    }

    @Override
    public boolean hasNext() {
        try {
            if (hasNextIndex == -1 || hasNextIndex == recordIndex) {
                hasNextIndex++;
                offset.inc();
                return hasNext = reader.next(key, value);
            }
            return hasNext;
        } catch (EOFException eofe) {
            return false;
        } catch (IOException ioe) {
            throw new ConnectException(ioe);
        }
    }

    @Override
    protected SequenceRecord<Writable, Writable> nextRecord() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());
        }
        recordIndex++;
        return new SequenceRecord<>(schema, keyFieldName, key, valueFieldName, value);
    }

    @Override
    public void seek(Offset offset) {
        if (offset.getRecordOffset() < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }
        try {
            reader.sync(offset.getRecordOffset());
            hasNextIndex = recordIndex = offset.getRecordOffset();
            hasNext = false;
            this.offset.setOffset(offset.getRecordOffset());
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
        reader.close();
    }

    public static class SeqOffset implements Offset {
        private long offset;

        public SeqOffset(long offset) {
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

    static class SeqToStruct implements ReaderAdapter<SequenceRecord<Writable, Writable>> {

        @Override
        public Struct apply(SequenceRecord<Writable, Writable> record) {
            return new Struct(record.schema)
                    .put(record.keyFieldName, toSchemaValue(record.key))
                    .put(record.valueFieldName, toSchemaValue(record.value));
        }

        private Object toSchemaValue(Writable writable) {
            if (writable instanceof ByteWritable) {
                return ((ByteWritable) writable).get();
            } else if (writable instanceof ShortWritable) {
                return ((ShortWritable) writable).get();
            } else if (writable instanceof IntWritable) {
                return ((IntWritable) writable).get();
            } else if (writable instanceof LongWritable) {
                return ((LongWritable) writable).get();
            } else if (writable instanceof FloatWritable) {
                return ((FloatWritable) writable).get();
            } else if (writable instanceof DoubleWritable) {
                return ((DoubleWritable) writable).get();
            } else if (writable instanceof BytesWritable) {
                return ((BytesWritable) writable).getBytes();
            } else if (writable instanceof BooleanWritable) {
                return ((BooleanWritable) writable).get();
            }
            return writable.toString();
        }
    }

    static class SequenceRecord<T, U> {

        private final Schema schema;
        private final String keyFieldName;
        private final T key;
        private final String valueFieldName;
        private final U value;

        SequenceRecord(Schema schema, String keyFieldName, T key, String valueFieldName, U value) {
            this.schema = schema;
            this.keyFieldName = keyFieldName;
            this.key = key;
            this.valueFieldName = valueFieldName;
            this.value = value;
        }

    }
}
