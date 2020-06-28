package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class SequenceFileReaderTest extends FileReaderTestBase {

    private static final String FIELD_NAME_KEY = "custom_field_key";
    private static final String FIELD_NAME_VALUE = "custom_field_name";
    private static final String FILE_EXTENSION = "sq";

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        FileSystem fs = fsConfig.getFs();
        File seqFile = File.createTempFile("test-", "." + getFileExtension());
        try (SequenceFile.Writer writer = SequenceFile.createWriter(fs.getConf(),
                SequenceFile.Writer.file(new Path(seqFile.getAbsolutePath())),
                SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class))) {
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                Writable key = new IntWritable(index);
                Writable value = new Text(String.format("%d_%s", index, UUID.randomUUID()));
                try {
                    writer.append(key, value);
                    writer.sync();
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(),
                SequenceFile.Reader.file(new Path(seqFile.getAbsolutePath())))) {
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());
            int index = 0;
            long pos = reader.getPosition() - 1;
            while (reader.next(key, value)) {
                fsConfig.offsetsByIndex().put(index++, pos);
                pos = reader.getPosition();
            }
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), seqFile.getName());
        fs.moveFromLocalFile(new Path(seqFile.getAbsolutePath()), path);
        return path;
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void defaultFieldNames(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(SequenceFileReader.FILE_READER_SEQUENCE_FIELD_NAME_KEY, null);
        readerConfig.put(SequenceFileReader.FILE_READER_SEQUENCE_FIELD_NAME_VALUE, null);
        FileReader reader = getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig);
        assertEquals(reader.getFilePath(), fsConfig.getDataFile());
        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(SequenceFileReader.FIELD_NAME_KEY_DEFAULT, SequenceFileReader.FIELD_NAME_VALUE_DEFAULT,
                    record, recordCount);
            recordCount++;
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void schemaMapper(ReaderFsTestConfig fsConfig) {
        SequenceFileReader reader = (SequenceFileReader) fsConfig.getReader();

        ByteWritable byteWritable = new ByteWritable((byte) 1);
        ShortWritable shortWritable = new ShortWritable((short) 123);
        IntWritable intWritable = new IntWritable(123);
        LongWritable longWritable = new LongWritable(123L);
        FloatWritable floatWritable = new FloatWritable(0.123F);
        DoubleWritable doubleWritable = new DoubleWritable(0.123D);
        BytesWritable bytesWritable = new BytesWritable(new byte[]{1, 2, 3});
        BooleanWritable booleanWritable = new BooleanWritable(true);
        Text textWritable = new Text("123");

        assertEquals(SchemaBuilder.INT8_SCHEMA, reader.getSchema(byteWritable));
        assertEquals(SchemaBuilder.INT16_SCHEMA, reader.getSchema(shortWritable));
        assertEquals(SchemaBuilder.INT32_SCHEMA, reader.getSchema(intWritable));
        assertEquals(SchemaBuilder.INT64_SCHEMA, reader.getSchema(longWritable));
        assertEquals(SchemaBuilder.FLOAT32_SCHEMA, reader.getSchema(floatWritable));
        assertEquals(SchemaBuilder.FLOAT64_SCHEMA, reader.getSchema(doubleWritable));
        assertEquals(SchemaBuilder.BYTES_SCHEMA, reader.getSchema(bytesWritable));
        assertEquals(SchemaBuilder.BOOLEAN_SCHEMA, reader.getSchema(booleanWritable));
        assertEquals(SchemaBuilder.STRING_SCHEMA, reader.getSchema(textWritable));
        assertEquals(SchemaBuilder.STRING_SCHEMA, reader.getSchema(new Writable() {

            @Override
            public void write(DataOutput out) {

            }

            @Override
            public void readFields(DataInput in) {

            }
        }));

        SequenceFileReader.SeqToStruct seqToStruct = new SequenceFileReader.SeqToStruct();
        assertEquals(seqToStruct.toSchemaValue(byteWritable), byteWritable.get());
        assertEquals(seqToStruct.toSchemaValue(shortWritable), shortWritable.get());
        assertEquals(seqToStruct.toSchemaValue(intWritable), intWritable.get());
        assertEquals(seqToStruct.toSchemaValue(longWritable), longWritable.get());
        assertEquals(seqToStruct.toSchemaValue(floatWritable), floatWritable.get());
        assertEquals(seqToStruct.toSchemaValue(doubleWritable), doubleWritable.get());
        assertEquals(seqToStruct.toSchemaValue(bytesWritable), bytesWritable.getBytes());
        assertEquals(seqToStruct.toSchemaValue(booleanWritable), booleanWritable.get());
        assertEquals(seqToStruct.toSchemaValue(textWritable), textWritable.toString());
    }

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return SequenceFileReader.class;
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<String, Object>() {{
            put(SequenceFileReader.FILE_READER_SEQUENCE_FIELD_NAME_KEY, FIELD_NAME_KEY);
            put(SequenceFileReader.FILE_READER_SEQUENCE_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
        }};
    }

    @Override
    protected void checkData(Struct record, long index) {
        checkData(FIELD_NAME_KEY, FIELD_NAME_VALUE, record, index);
    }

    private void checkData(String keyFieldName, String valueFieldName, Struct record, long index) {
        assertAll(
                () -> assertEquals(index, (int) record.get(keyFieldName)),
                () -> assertTrue(record.get(valueFieldName).toString().startsWith(index + "_"))
        );
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
