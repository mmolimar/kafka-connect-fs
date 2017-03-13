package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.EOFException;
import java.io.IOException;
import java.util.NoSuchElementException;

public class SequenceFileReader extends AbstractFileReader<SequenceFileReader.SequenceRecord<Writable, Writable>> {

    private static final String FIELD_KEY = "key";
    private static final String FIELD_VALUE = "value";
    private static final String BUFFER_SIZE = "reader.sequencefile.buffer.bytes";
    private static final int DEFAULT_BUFF_SIZE = 4096;

    private final Path filePath;
    private final SequenceFile.Reader reader;
    private final Writable key, value;
    private final Schema schema;
    private final SeqOffset offset;
    private long recordIndex, hasNextIndex;
    private boolean hasNext;

    public SequenceFileReader(FileSystem fs, Path filePath) throws IOException {
        super(fs, filePath, new SeqToStruct());

        this.filePath = filePath;
        this.reader = new SequenceFile.Reader(fs.getConf(),
                SequenceFile.Reader.file(filePath),
                SequenceFile.Reader.bufferSize(fs.getConf().getInt(BUFFER_SIZE, DEFAULT_BUFF_SIZE)));
        this.key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
        this.value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());
        this.schema = SchemaBuilder.struct()
                .name("SEQ")
                .field(FIELD_KEY, getSchema(key)).field(FIELD_VALUE, getSchema(value)).build();
        this.offset = new SeqOffset(0);
        this.recordIndex = this.hasNextIndex = -1;
        this.hasNext = false;
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
            return SchemaBuilder.INT64_SCHEMA;
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
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public SequenceRecord<Writable, Writable> nextRecord() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + filePath);
        }
        recordIndex++;
        return new SequenceRecord<Writable, Writable>(schema, key, value);
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
            throw new RuntimeException(ioe);
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

        protected void inc() {
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
                    .put(FIELD_KEY, toSchemaValue(record.key))
                    .put(FIELD_VALUE, toSchemaValue(record.value));
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
        final Schema schema;
        final T key;
        final U value;

        public SequenceRecord(Schema schema, T key, U value) {
            this.schema = schema;
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("(%s, %s)", key, value);
        }
    }

}
