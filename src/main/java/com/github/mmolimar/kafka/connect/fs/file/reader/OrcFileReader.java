package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class OrcFileReader extends AbstractFileReader<OrcFileReader.OrcRecord> {

    private static final String FILE_READER_ORC = FILE_READER_PREFIX + "orc.";

    public static final String FILE_READER_ORC_USE_ZEROCOPY = FILE_READER_ORC + "use_zerocopy";
    public static final String FILE_READER_ORC_SKIP_CORRUPT_RECORDS = FILE_READER_ORC + "skip_corrupt_records";

    private final RecordReader reader;
    private final VectorizedRowBatch batch;
    private final Schema schema;
    private final long numberOfRows;
    private boolean useZeroCopy;
    private boolean skipCorruptRecords;
    private int vectorIndex;
    private boolean closed;

    public OrcFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new OrcToStruct(), config);

        Reader orcReader = OrcFile.createReader(filePath, OrcFile.readerOptions(fs.getConf()));
        Reader.Options options = new Reader.Options(fs.getConf())
                .useZeroCopy(this.useZeroCopy)
                .skipCorruptRecords(this.skipCorruptRecords);
        this.reader = orcReader.rows(options);
        this.numberOfRows = orcReader.getNumberOfRows();
        this.batch = orcReader.getSchema().createRowBatch();
        this.schema = hasNext() ? buildSchema(orcReader.getSchema()) : SchemaBuilder.struct().build();
        this.vectorIndex = 0;
        this.closed = false;
    }

    @Override
    protected void configure(Map<String, String> config) {
        this.useZeroCopy = Boolean.parseBoolean(config.getOrDefault(FILE_READER_ORC_USE_ZEROCOPY, "false"));
        this.skipCorruptRecords = Boolean.parseBoolean(config.getOrDefault(FILE_READER_ORC_SKIP_CORRUPT_RECORDS, "false"));
    }

    private Schema buildSchema(TypeDescription typeDescription) {
        TypeDescription td;
        if (typeDescription.getChildren() == null || typeDescription.getChildren().isEmpty()) {
            td = TypeDescription.createStruct().addField(typeDescription.getCategory().getName(), typeDescription);
        } else {
            td = typeDescription;
        }
        return extractSchema(td);
    }

    private Schema extractSchema(TypeDescription typeDescription) {
        switch (typeDescription.getCategory()) {
            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case BYTE:
            case CHAR:
                return Schema.OPTIONAL_INT8_SCHEMA;
            case SHORT:
                return Schema.OPTIONAL_INT16_SCHEMA;
            case INT:
                return Schema.OPTIONAL_INT32_SCHEMA;
            case DATE:
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
            case LONG:
                return Schema.OPTIONAL_INT64_SCHEMA;
            case FLOAT:
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            case DECIMAL:
            case DOUBLE:
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            case VARCHAR:
            case STRING:
                return Schema.OPTIONAL_STRING_SCHEMA;
            case BINARY:
                return Schema.OPTIONAL_BYTES_SCHEMA;
            case LIST:
                Schema arraySchema = typeDescription.getChildren().stream()
                        .findFirst().map(this::extractSchema)
                        .orElse(SchemaBuilder.struct().build());
                return SchemaBuilder.array(arraySchema).build();
            case UNION:
                // union data types are mapped as structs with a faked key.
                SchemaBuilder mapBuilder = SchemaBuilder.struct();
                IntStream.range(0, typeDescription.getChildren().size())
                        .forEach(index -> mapBuilder.field(
                                "field" + (index + 1),
                                extractSchema(typeDescription.getChildren().get(index))
                        ));
                return mapBuilder.build();
            case STRUCT:
                SchemaBuilder structBuilder = SchemaBuilder.struct();
                IntStream.range(0, typeDescription.getChildren().size())
                        .forEach(index ->
                                structBuilder.field(typeDescription.getFieldNames().get(index),
                                        extractSchema(typeDescription.getChildren().get(index))));
                return structBuilder.build();
            case MAP:
                return SchemaBuilder.map(extractSchema(typeDescription.getChildren().get(0)),
                        extractSchema(typeDescription.getChildren().get(1)))
                        .build();
            default:
                throw new ConnectException("Data type '" + typeDescription.getCategory() + "' in ORC file " +
                        "is not supported.");
        }
    }

    @Override
    public boolean hasNextRecord() throws IOException {
        if (vectorIndex >= batch.size) {
            vectorIndex = 0;
            reader.nextBatch(batch);
        }
        return batch.size > 0;
    }

    @Override
    protected OrcRecord nextRecord() {
        incrementOffset();
        return new OrcRecord(schema, batch, vectorIndex++);
    }

    @Override
    public void seekFile(long offset) throws IOException {
        reader.seekToRow(Math.min(offset, numberOfRows - 1));
        if (offset >= numberOfRows) {
            reader.nextBatch(batch);
        }
        setOffset(offset);
        vectorIndex = Integer.MAX_VALUE;
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

    static class OrcToStruct implements ReaderAdapter<OrcRecord> {

        @Override
        public Struct apply(OrcRecord record) {
            return toStruct(record.schema, record.batch, record.vectorIndex);
        }

        private Struct toStruct(Schema schema, VectorizedRowBatch batch, int vectorIndex) {
            Struct struct = new Struct(schema);
            IntStream.range(0, schema.fields().size())
                    .forEach(index -> {
                        Field field = schema.fields().get(index);
                        struct.put(field.name(), mapValue(field.schema(), batch.cols[index], vectorIndex));
                    });
            return struct;
        }

        private Object mapValue(Schema schema, ColumnVector value, long vectorIndex) {
            switch (value.getClass().getSimpleName()) {
                case "BytesColumnVector":
                    BytesColumnVector bytes = ((BytesColumnVector) value);
                    if (bytes.vector[(int) vectorIndex] == null) {
                        return null;
                    }
                    String content = new String(
                            bytes.vector[(int) vectorIndex],
                            bytes.start[(int) vectorIndex],
                            bytes.length[(int) vectorIndex]
                    );
                    switch (schema.type()) {
                        case INT8:
                            return (byte) content.charAt(0);
                        case BYTES:
                            return content.getBytes();
                        default:
                            return content;
                    }
                case "DecimalColumnVector":
                    return ((DecimalColumnVector) value).vector[(int) vectorIndex].doubleValue();
                case "DoubleColumnVector":
                    if (schema.type() == Schema.Type.FLOAT32) {
                        return (float) ((DoubleColumnVector) value).vector[(int) vectorIndex];
                    }
                    return ((DoubleColumnVector) value).vector[(int) vectorIndex];
                case "IntervalDayTimeColumnVector":
                    return ((IntervalDayTimeColumnVector) value).getNanos(0);
                case "Decimal64ColumnVector":
                case "DateColumnVector":
                case "LongColumnVector":
                    long castedValue = ((LongColumnVector) value).vector[(int) vectorIndex];
                    switch (schema.type()) {
                        case BOOLEAN:
                            return castedValue != 0;
                        case INT8:
                            return (byte) castedValue;
                        case INT16:
                            return (short) castedValue;
                        case INT32:
                            return (int) castedValue;
                        default:
                            return castedValue;
                    }
                case "ListColumnVector":
                    ListColumnVector list = ((ListColumnVector) value);
                    return LongStream.range(0, list.lengths[(int) vectorIndex])
                            .mapToObj(index -> mapValue(schema.valueSchema(), list.child, list.offsets[(int) vectorIndex]))
                            .collect(Collectors.toList());
                case "MapColumnVector":
                    MapColumnVector map = ((MapColumnVector) value);
                    return Collections.singletonMap(
                            mapValue(schema.keySchema(), map.keys, map.offsets[(int) vectorIndex]),
                            mapValue(schema.valueSchema(), map.values, map.offsets[(int) vectorIndex])
                    );
                case "UnionColumnVector":
                case "StructColumnVector":
                    ColumnVector[] fields;
                    if (value instanceof StructColumnVector) {
                        fields = ((StructColumnVector) value).fields;
                    } else {
                        fields = ((UnionColumnVector) value).fields;
                    }
                    Struct struct = new Struct(schema);
                    IntStream.range(0, fields.length)
                            .forEach(index -> {
                                String structKey = schema.fields().get(index).name();
                                Object structValue = mapValue(schema.fields().get(index).schema(),
                                        fields[index], vectorIndex);
                                struct.put(structKey, structValue);
                            });
                    return struct;
                case "TimestampColumnVector":
                    return ((TimestampColumnVector) value).time[(int) vectorIndex];
                case "VoidColumnVector":
                default:
                    return null;
            }
        }
    }

    static class OrcRecord {

        private final Schema schema;
        private final VectorizedRowBatch batch;
        private final int vectorIndex;

        OrcRecord(Schema schema, VectorizedRowBatch batch, int vectorIndex) {
            this.schema = schema;
            this.batch = batch;
            this.vectorIndex = vectorIndex;
        }

    }
}
