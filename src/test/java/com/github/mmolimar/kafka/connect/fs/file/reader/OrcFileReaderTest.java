package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.kafka.connect.data.Struct;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class OrcFileReaderTest extends FileReaderTestBase {

    private static final String FIELD_INTEGER = "integerField";
    private static final String FIELD_LONG = "longField";
    private static final String FIELD_BOOLEAN = "booleanField";
    private static final String FIELD_STRING = "stringField";
    private static final String FIELD_DECIMAL = "decimalField";
    private static final String FIELD_ARRAY = "arrayField";
    private static final String FIELD_MAP = "mapField";
    private static final String FIELD_STRUCT = "structField";
    private static final String FIELD_UNION = "unionField";
    private static final String FIELD_EMPTY = "emptyField";
    private static final String FIELD_BYTE = "byteField";
    private static final String FIELD_CHAR = "charField";
    private static final String FIELD_SHORT = "shortField";
    private static final String FIELD_FLOAT = "floatField";
    private static final String FIELD_BINARY = "binaryField";
    private static final String FIELD_DATE = "dateField";
    private static final String FIELD_TIMESTAMP = "timestampField";
    private static final String FIELD_TIMESTAMP_INSTANT = "timestampInstantField";
    private static final String FILE_EXTENSION = "rc";

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        int numRecords = args.length < 1 ? NUM_RECORDS : (int) args[0];
        File orcFile = File.createTempFile("test-", "." + getFileExtension());

        TypeDescription schema = TypeDescription.createStruct();
        schema.addField(FIELD_INTEGER, TypeDescription.createInt());
        schema.addField(FIELD_LONG, TypeDescription.createLong());
        schema.addField(FIELD_STRING, TypeDescription.createString());
        schema.addField(FIELD_BOOLEAN, TypeDescription.createBoolean());
        schema.addField(FIELD_DECIMAL, TypeDescription.createDecimal());
        schema.addField(FIELD_EMPTY, TypeDescription.createVarchar());
        schema.addField(FIELD_BYTE, TypeDescription.createByte());
        schema.addField(FIELD_CHAR, TypeDescription.createChar());
        schema.addField(FIELD_SHORT, TypeDescription.createShort());
        schema.addField(FIELD_FLOAT, TypeDescription.createFloat());
        schema.addField(FIELD_BINARY, TypeDescription.createBinary());
        schema.addField(FIELD_DATE, TypeDescription.createDate());
        schema.addField(FIELD_TIMESTAMP, TypeDescription.createTimestamp());
        schema.addField(FIELD_TIMESTAMP_INSTANT, TypeDescription.createTimestampInstant());
        schema.addField(FIELD_ARRAY, TypeDescription.createList(TypeDescription.createString()));
        schema.addField(FIELD_MAP, TypeDescription.createMap(TypeDescription.createString(), TypeDescription.createString()));
        TypeDescription struct = TypeDescription.createStruct();
        struct.addField(FIELD_INTEGER, TypeDescription.createInt());
        struct.addField(FIELD_LONG, TypeDescription.createLong());
        struct.addField(FIELD_STRING, TypeDescription.createString());
        struct.addField(FIELD_BOOLEAN, TypeDescription.createBoolean());
        struct.addField(FIELD_DECIMAL, TypeDescription.createDecimal());
        struct.addField(FIELD_EMPTY, TypeDescription.createVarchar());
        schema.addField(FIELD_STRUCT, struct);
        TypeDescription union = TypeDescription.createUnion();
        union.addUnionChild(TypeDescription.createInt());
        schema.addField(FIELD_UNION, union);

        Properties props = new Properties();
        props.put(OrcConf.OVERWRITE_OUTPUT_FILE.getAttribute(), "true");
        OrcFile.WriterOptions opts = OrcFile.writerOptions(props, fsConfig.getFs().getConf()).setSchema(schema);
        try (Writer writer = OrcFile.createWriter(new Path(orcFile.toURI()), opts)) {
            VectorizedRowBatch batch = schema.createRowBatch(numRecords);
            LongColumnVector f1 = (LongColumnVector) batch.cols[0];
            LongColumnVector f2 = (LongColumnVector) batch.cols[1];
            BytesColumnVector f3 = (BytesColumnVector) batch.cols[2];
            LongColumnVector f4 = (LongColumnVector) batch.cols[3];
            DecimalColumnVector f5 = (DecimalColumnVector) batch.cols[4];
            BytesColumnVector f6 = (BytesColumnVector) batch.cols[5];
            LongColumnVector f7 = (LongColumnVector) batch.cols[6];
            BytesColumnVector f8 = (BytesColumnVector) batch.cols[7];
            LongColumnVector f9 = (LongColumnVector) batch.cols[8];
            DoubleColumnVector f10 = (DoubleColumnVector) batch.cols[9];
            BytesColumnVector f11 = (BytesColumnVector) batch.cols[10];
            DateColumnVector f12 = (DateColumnVector) batch.cols[11];
            TimestampColumnVector f13 = (TimestampColumnVector) batch.cols[12];
            TimestampColumnVector f14 = (TimestampColumnVector) batch.cols[13];
            ListColumnVector f15 = (ListColumnVector) batch.cols[14];
            MapColumnVector f16 = (MapColumnVector) batch.cols[15];
            StructColumnVector f17 = (StructColumnVector) batch.cols[16];
            UnionColumnVector f18 = (UnionColumnVector) batch.cols[17];

            for (int index = 0; index < numRecords; index++) {
                f1.vector[index] = index;
                f2.vector[index] = Long.MAX_VALUE;
                f3.setVal(index, String.format("%d_%s", index, UUID.randomUUID()).getBytes());
                f4.vector[index] = 1;
                f5.vector[index] = new HiveDecimalWritable(HiveDecimal.create(Double.parseDouble(index + "." + index)));
                f6.setVal(index, new byte[0]);

                f7.vector[index] = index;
                f8.setVal(index, new byte[]{(byte) (index % 32)});
                f9.vector[index] = (short) index;
                f10.vector[index] = Float.parseFloat(index + "." + index);
                f11.setVal(index, String.format("%d_%s", index, UUID.randomUUID()).getBytes());
                f12.vector[index] = Calendar.getInstance().getTimeInMillis();
                f13.time[index] = Calendar.getInstance().getTimeInMillis();
                f14.time[index] = Calendar.getInstance().getTimeInMillis();

                ((BytesColumnVector) f15.child).setVal(index, ("elm[" + index + "]").getBytes());
                f15.lengths[index] = 1;
                f15.offsets[index] = f15.childCount++;

                ((BytesColumnVector) f16.keys).setVal(index, ("key[" + index + "]").getBytes());
                ((BytesColumnVector) f16.values).setVal(index, ("value[" + index + "]").getBytes());
                f16.lengths[index] = 1;
                f16.offsets[index] = f16.childCount++;

                ((LongColumnVector) f17.fields[0]).vector[index] = index;
                ((LongColumnVector) f17.fields[1]).vector[index] = Long.MAX_VALUE;
                ((BytesColumnVector) f17.fields[2]).setVal(index,
                        String.format("%d_%s", index, UUID.randomUUID()).getBytes());
                ((LongColumnVector) f17.fields[3]).vector[index] = 1;
                ((DecimalColumnVector) f17.fields[4]).vector[index] =
                        new HiveDecimalWritable(HiveDecimal.create(Double.parseDouble(index + "." + index)));
                ((BytesColumnVector) f17.fields[5]).setVal(index, new byte[0]);

                ((LongColumnVector) f18.fields[0]).vector[index] = index;

                fsConfig.offsetsByIndex().put(index, (long) index);
            }
            batch.size = numRecords;
            writer.addRowBatch(batch);
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), orcFile.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(orcFile.getAbsolutePath()), path);
        return path;
    }

    private Path createDataFileWithoutStruct(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        int numRecords = args.length < 1 ? NUM_RECORDS : (int) args[0];
        File orcFile = File.createTempFile("test-", "." + getFileExtension());

        TypeDescription schema = TypeDescription.createLong();
        Properties props = new Properties();
        props.put(OrcConf.OVERWRITE_OUTPUT_FILE.getAttribute(), "true");
        OrcFile.WriterOptions opts = OrcFile.writerOptions(props, fsConfig.getFs().getConf()).setSchema(schema);
        try (Writer writer = OrcFile.createWriter(new Path(orcFile.toURI()), opts)) {
            VectorizedRowBatch batch = schema.createRowBatch(numRecords);
            LongColumnVector longField = (LongColumnVector) batch.cols[0];

            for (int index = 0; index < numRecords; index++) {
                longField.vector[index] = index;

                fsConfig.offsetsByIndex().put(index, (long) index);
            }
            batch.size = numRecords;
            writer.addRowBatch(batch);
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), orcFile.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(orcFile.getAbsolutePath()), path);
        return path;
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFile(ReaderFsTestConfig fsConfig) throws IOException {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        FileReader reader = getReader(fsConfig.getFs(), path, getReaderConfig());
        assertFalse(reader.hasNext());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void useZeroCopyConfig(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(OrcFileReader.FILE_READER_ORC_USE_ZEROCOPY, "true");
        fsConfig.setReader(getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig));
        readAllData(fsConfig);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void skipCorruptRecordsConfig(ReaderFsTestConfig fsConfig) {
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(OrcFileReader.FILE_READER_ORC_SKIP_CORRUPT_RECORDS, "true");
        fsConfig.setReader(getReader(fsConfig.getFs(), fsConfig.getDataFile(), readerConfig));
        readAllData(fsConfig);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readDataWithoutStruct(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFileWithoutStruct(fsConfig, NUM_RECORDS);
        FileReader reader = getReader(fsConfig.getFs(), file, getReaderConfig());

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkDataWithoutStruct(record, recordCount);
            recordCount++;
        }
        reader.close();
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return OrcFileReader.class;
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<>();
    }

    @Override
    protected void checkData(Struct record, long index) {
        Struct struct = record.getStruct(FIELD_STRUCT);
        Struct union = record.getStruct(FIELD_UNION);
        assertAll(
                () -> assertEquals(index, (int) record.get(FIELD_INTEGER)),
                () -> assertEquals(Long.MAX_VALUE, (long) record.get(FIELD_LONG)),
                () -> assertTrue(record.get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(record.get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), (Double) record.get(FIELD_DECIMAL), 0),
                () -> assertEquals("", record.get(FIELD_EMPTY)),
                () -> assertEquals(index, ((Byte) record.get(FIELD_BYTE)).intValue()),
                () -> assertEquals((byte) (index % 32), record.get(FIELD_CHAR)),
                () -> assertEquals((short) index, (short) record.get(FIELD_SHORT)),
                () -> assertEquals(Float.parseFloat(index + "." + index), record.get(FIELD_FLOAT)),
                () -> assertTrue(new String((byte[]) record.get(FIELD_BINARY)).startsWith(index + "_")),
                () -> assertNotNull(record.get(FIELD_TIMESTAMP)),
                () -> assertNotNull(record.get(FIELD_TIMESTAMP_INSTANT)),
                () -> assertEquals(Collections.singletonMap("key[" + index + "]", "value[" + index + "]"), record.get(FIELD_MAP)),
                () -> assertEquals(Collections.singletonList("elm[" + index + "]"), record.get(FIELD_ARRAY)),
                () -> assertEquals(index, (int) struct.get(FIELD_INTEGER)),
                () -> assertEquals(Long.MAX_VALUE, (long) struct.get(FIELD_LONG)),
                () -> assertTrue(struct.get(FIELD_STRING).toString().startsWith(index + "_")),
                () -> assertTrue(Boolean.parseBoolean(struct.get(FIELD_BOOLEAN).toString())),
                () -> assertEquals(Double.parseDouble(index + "." + index), (Double) struct.get(FIELD_DECIMAL), 0),
                () -> assertEquals("", struct.get(FIELD_EMPTY)),
                () -> assertNotNull(struct.schema().field(FIELD_EMPTY)),
                () -> assertEquals((int) index, union.get("field1"))
        );
    }

    private void checkDataWithoutStruct(Struct record, long index) {
        assertEquals(index, (long) record.get("bigint"));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
