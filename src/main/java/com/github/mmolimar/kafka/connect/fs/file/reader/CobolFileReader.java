package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import scala.collection.Seq;
import scala.math.BigDecimal;
import scala.math.ScalaNumber;
import za.co.absa.cobrix.cobol.parser.ast.Group;
import za.co.absa.cobrix.cobol.parser.ast.Primitive;
import za.co.absa.cobrix.cobol.parser.ast.Statement;
import za.co.absa.cobrix.cobol.parser.ast.datatype.AlphaNumeric;
import za.co.absa.cobrix.cobol.parser.ast.datatype.COMP1;
import za.co.absa.cobrix.cobol.parser.ast.datatype.Decimal;
import za.co.absa.cobrix.cobol.parser.ast.datatype.Integral;
import za.co.absa.cobrix.cobol.parser.common.Constants;
import za.co.absa.cobrix.cobol.parser.decoders.FloatingPointFormat$;
import za.co.absa.cobrix.cobol.parser.encoding.RAW$;
import za.co.absa.cobrix.cobol.parser.policies.CommentPolicy;
import za.co.absa.cobrix.cobol.parser.policies.DebugFieldsPolicy$;
import za.co.absa.cobrix.cobol.parser.policies.StringTrimmingPolicy$;
import za.co.absa.cobrix.cobol.reader.VarLenReader;
import za.co.absa.cobrix.cobol.reader.extractors.record.RecordHandler;
import za.co.absa.cobrix.cobol.reader.parameters.ReaderParameters;
import za.co.absa.cobrix.cobol.reader.policies.SchemaRetentionPolicy$;
import za.co.absa.cobrix.cobol.reader.schema.CobolSchema;
import za.co.absa.cobrix.cobol.reader.stream.SimpleStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;
import static scala.collection.JavaConverters.*;

public class CobolFileReader extends AbstractFileReader<CobolFileReader.CobolRecord> {

    private static final String FILE_READER_COBOL = FILE_READER_PREFIX + "cobol.";
    private static final String FILE_READER_COBOL_READER = FILE_READER_COBOL + "reader.";
    private static final String FILE_READER_COBOL_COPYBOOK_PREFIX = FILE_READER_COBOL + "copybook.";

    public static final String FILE_READER_COBOL_COPYBOOK_CONTENT = FILE_READER_COBOL_COPYBOOK_PREFIX + "content";
    public static final String FILE_READER_COBOL_COPYBOOK_PATH = FILE_READER_COBOL_COPYBOOK_PREFIX + "path";

    public static final String FILE_READER_COBOL_READER_IS_EBCDIC = FILE_READER_COBOL_READER + "is_ebcdic";
    public static final String FILE_READER_COBOL_READER_EBCDIC_CODE_PAGE = FILE_READER_COBOL_READER + "ebcdic_code_page";
    public static final String FILE_READER_COBOL_READER_EBCDIC_CODE_PAGE_CLASS = FILE_READER_COBOL_READER + "ebcdic_code_page_class";
    public static final String FILE_READER_COBOL_READER_ASCII_CHARSET = FILE_READER_COBOL_READER + "ascii_charset";
    public static final String FILE_READER_COBOL_READER_IS_UFT16_BIG_ENDIAN = FILE_READER_COBOL_READER + "is_uft16_big_endian";
    public static final String FILE_READER_COBOL_READER_FLOATING_POINT_FORMAT = FILE_READER_COBOL_READER + "floating_point_format";
    public static final String FILE_READER_COBOL_READER_VARIABLE_SIZE_OCCURS = FILE_READER_COBOL_READER + "variable_size_occurs";
    public static final String FILE_READER_COBOL_READER_LENGTH_FIELD_NAME = FILE_READER_COBOL_READER + "length_field_name";
    public static final String FILE_READER_COBOL_READER_IS_RECORD_SEQUENCE = FILE_READER_COBOL_READER + "is_record_sequence";
    public static final String FILE_READER_COBOL_READER_IS_RDW_BIG_ENDIAN = FILE_READER_COBOL_READER + "is_rdw_big_endian";
    public static final String FILE_READER_COBOL_READER_IS_RDW_PART_REC_LENGTH = FILE_READER_COBOL_READER + "is_rdw_part_rec_length";
    public static final String FILE_READER_COBOL_READER_RDW_ADJUSTMENT = FILE_READER_COBOL_READER + "rdw_adjustment";
    public static final String FILE_READER_COBOL_READER_IS_INDEX_GENERATION_NEEDED = FILE_READER_COBOL_READER + "is_index_generation_needed";
    public static final String FILE_READER_COBOL_READER_INPUT_SPLIT_RECORDS = FILE_READER_COBOL_READER + "input_split_records";
    public static final String FILE_READER_COBOL_READER_INPUT_SPLIT_SIZE_MB = FILE_READER_COBOL_READER + "input_split_size_mb";
    public static final String FILE_READER_COBOL_READER_HDFS_DEFAULT_BLOCK_SIZE = FILE_READER_COBOL_READER + "hdfs_default_block_size";
    public static final String FILE_READER_COBOL_READER_START_OFFSET = FILE_READER_COBOL_READER + "start_offset";
    public static final String FILE_READER_COBOL_READER_END_OFFSET = FILE_READER_COBOL_READER + "end_offset";
    public static final String FILE_READER_COBOL_READER_FILE_START_OFFSET = FILE_READER_COBOL_READER + "file_start_offset";
    public static final String FILE_READER_COBOL_READER_FILE_END_OFFSET = FILE_READER_COBOL_READER + "file_end_offset";
    public static final String FILE_READER_COBOL_READER_SCHEMA_POLICY = FILE_READER_COBOL_READER + "schema_policy";
    public static final String FILE_READER_COBOL_READER_STRING_TRIMMING_POLICY = FILE_READER_COBOL_READER + "string_trimming_policy";
    public static final String FILE_READER_COBOL_READER_DROP_GROUP_FILLERS = FILE_READER_COBOL_READER + "drop_group_fillers";
    public static final String FILE_READER_COBOL_READER_DROP_VALUE_FILLERS = FILE_READER_COBOL_READER + "drop_value_fillers";
    public static final String FILE_READER_COBOL_READER_NON_TERMINALS = FILE_READER_COBOL_READER + "non_terminals";
    public static final String FILE_READER_COBOL_READER_DEBUG_FIELDS_POLICY = FILE_READER_COBOL_READER + "debug_fields_policy";
    public static final String FILE_READER_COBOL_READER_RECORD_HEADER_PARSER = FILE_READER_COBOL_READER + "record_header_parser";
    public static final String FILE_READER_COBOL_READER_RHP_ADDITIONAL_INFO = FILE_READER_COBOL_READER + "rhp_additional_info";
    public static final String FILE_READER_COBOL_READER_INPUT_FILE_NAME_COLUMN = FILE_READER_COBOL_READER + "input_file_name_column";


    private final Schema schema;
    private final VarLenReader reader;
    private SimpleStream stream;
    private String copybook;
    private Iterator<List<Object>> iterator;
    private ReaderParameters params;
    private boolean closed;

    public CobolFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws Exception {
        super(fs, filePath, new CobolToStruct(), config);

        this.reader = CobrixReader.varLenReader(copybook, params);
        this.schema = extractSchema(reader.getCobolSchema());
        this.iterator = initIterator();
        this.closed = false;
    }

    private Iterator<List<Object>> initIterator() throws Exception {
        if (stream != null) {
            stream.close();
        }
        stream = new FSStream(getFs(), getFilePath());
        return asJavaIterator(reader.getRecordIterator(stream, 0, 0, 0).map(it -> seqAsJavaList(it.seq())));
    }

    private Schema extractSchema(CobolSchema cobolSchema) {
        SchemaBuilder builder = SchemaBuilder.struct();
        Group group;
        if (params.schemaPolicy().id() == SchemaRetentionPolicy$.MODULE$.CollapseRoot().id()) {
            group = (Group) cobolSchema.getCobolSchema().ast().children().head();
        } else {
            group = cobolSchema.getCobolSchema().ast();
        }
        seqAsJavaList(group.children())
                .forEach(child -> builder.field(child.name(), schemaForField(child)));

        return builder.build();
    }

    private Schema schemaForField(Statement statement) {
        if (statement instanceof Group) {
            Group group = (Group) statement;
            SchemaBuilder childrenBuilder = SchemaBuilder.struct();
            seqAsJavaList(group.children()).forEach(child -> childrenBuilder.field(child.name(), schemaForField(child)));
            SchemaBuilder builder;
            if (group.isArray()) {
                builder = SchemaBuilder.array(childrenBuilder.build());
            } else {
                builder = childrenBuilder;
            }
            return builder.build();
        }
        Primitive primitive = (Primitive) statement;
        if (primitive.dataType() instanceof Integral) {
            Integral dt = (Integral) primitive.dataType();
            if (dt.precision() > Constants.maxLongPrecision()) {
                return Schema.OPTIONAL_FLOAT64_SCHEMA;
            } else if (dt.precision() > Constants.maxIntegerPrecision()) {
                return Schema.OPTIONAL_INT64_SCHEMA;
            } else {
                return Schema.OPTIONAL_INT32_SCHEMA;
            }
        } else if (primitive.dataType() instanceof Decimal) {
            Decimal dt = (Decimal) primitive.dataType();
            if (dt.compact().exists(c -> c instanceof COMP1)) {
                return Schema.OPTIONAL_FLOAT32_SCHEMA;
            }
            return Schema.OPTIONAL_FLOAT64_SCHEMA;
        } else {
            AlphaNumeric dt = (AlphaNumeric) primitive.dataType();
            if (dt.enc().exists(enc -> enc instanceof RAW$)) {
                return Schema.OPTIONAL_BYTES_SCHEMA;
            }
            return Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    @Override
    protected void configure(Map<String, String> config) {
        copybook = copybookContent(config);
        params = getReaderParameters(config);
    }

    private String copybookContent(Map<String, String> config) {
        String content = Optional.ofNullable(config.get(FILE_READER_COBOL_COPYBOOK_PATH))
                .map(Path::new)
                .map(path -> {
                    StringBuilder sb = new StringBuilder();
                    try (InputStream is = getFs().open(path);
                         BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                        br.lines().forEach(line -> sb.append(line).append("\n"));
                    } catch (IOException ioe) {
                        throw new ConnectException("Cannot read Copybook file: " + path, ioe);
                    }
                    return sb.toString();
                })
                .orElseGet(() -> config.get(FILE_READER_COBOL_COPYBOOK_CONTENT));

        if (content == null || content.trim().isEmpty()) {
            throw new ConnectException("Copybook is not specified.");
        }
        return content;
    }

    private ReaderParameters getReaderParameters(Map<String, String> config) {
        return new ReaderParameters(
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_IS_EBCDIC, "true")),  // isEbcdic
                config.getOrDefault(FILE_READER_COBOL_READER_EBCDIC_CODE_PAGE, "common"),  // ebcdicCodePage
                scala.Option.apply(config.get(FILE_READER_COBOL_READER_EBCDIC_CODE_PAGE_CLASS)),  // ebcdicCodePageClass
                config.getOrDefault(FILE_READER_COBOL_READER_ASCII_CHARSET, ""),  // asciiCharset
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_IS_UFT16_BIG_ENDIAN, "true")),  // isUtf16BigEndian
                FloatingPointFormat$.MODULE$.withNameOpt(config.getOrDefault(FILE_READER_COBOL_READER_FLOATING_POINT_FORMAT, "ibm")).get(),  // floatingPointFormat
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_VARIABLE_SIZE_OCCURS, "false")), // variableSizeOccurs
                scala.Option.apply(config.get(FILE_READER_COBOL_READER_LENGTH_FIELD_NAME)),  // lengthFieldName
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_IS_RECORD_SEQUENCE, "false")), // isRecordSequence
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_IS_RDW_BIG_ENDIAN, "false")),  // isRdwBigEndian
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_IS_RDW_PART_REC_LENGTH, "false")), // isRdwPartRecLength
                Integer.parseInt(config.getOrDefault(FILE_READER_COBOL_READER_RDW_ADJUSTMENT, "0")),  // rdwAdjustment
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_IS_INDEX_GENERATION_NEEDED, "false")),  // isIndexGenerationNeeded
                scala.Option.apply(config.get(FILE_READER_COBOL_READER_INPUT_SPLIT_RECORDS)),  // inputSplitRecords
                scala.Option.apply(config.get(FILE_READER_COBOL_READER_INPUT_SPLIT_SIZE_MB)),  // inputSplitSizeMB
                scala.Option.apply(config.get(FILE_READER_COBOL_READER_HDFS_DEFAULT_BLOCK_SIZE)),  // hdfsDefaultBlockSize
                Integer.parseInt(config.getOrDefault(FILE_READER_COBOL_READER_START_OFFSET, "0")),  // startOffset
                Integer.parseInt(config.getOrDefault(FILE_READER_COBOL_READER_END_OFFSET, "0")),  // endOffset
                Integer.parseInt(config.getOrDefault(FILE_READER_COBOL_READER_FILE_START_OFFSET, "0")),  // fileStartOffset
                Integer.parseInt(config.getOrDefault(FILE_READER_COBOL_READER_FILE_END_OFFSET, "0")),  // fileEndOffset
                false, // generateRecordId
                SchemaRetentionPolicy$.MODULE$.withNameOpt(config.getOrDefault(FILE_READER_COBOL_READER_SCHEMA_POLICY, "keep_original")).get(),  // schemaPolicy
                StringTrimmingPolicy$.MODULE$.withNameOpt(config.getOrDefault(FILE_READER_COBOL_READER_STRING_TRIMMING_POLICY, "both")).get(),  // stringTrimmingPolicy
                scala.Option.apply(null),  // multisegment
                new CommentPolicy(true, 6, 72),  // commentPolicy
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_DROP_GROUP_FILLERS, "false")),  // dropGroupFillers
                Boolean.parseBoolean(config.getOrDefault(FILE_READER_COBOL_READER_DROP_VALUE_FILLERS, "true")),  // dropValueFillers
                asScalaBuffer(Arrays.asList(config.getOrDefault(FILE_READER_COBOL_READER_NON_TERMINALS, "").split(","))),  // nonTerminals
                scala.collection.immutable.Map$.MODULE$.empty(),  // occursMappings
                DebugFieldsPolicy$.MODULE$.withNameOpt(config.getOrDefault(FILE_READER_COBOL_READER_DEBUG_FIELDS_POLICY, "none")).get(),  // debugFieldsPolicy
                scala.Option.apply(config.get(FILE_READER_COBOL_READER_RECORD_HEADER_PARSER)),  // recordHeaderParser
                scala.Option.apply(config.get(FILE_READER_COBOL_READER_RHP_ADDITIONAL_INFO)),  // rhpAdditionalInfo
                config.getOrDefault(FILE_READER_COBOL_READER_INPUT_FILE_NAME_COLUMN, "")  // inputFileNameColumn
        );
    }

    @Override
    protected boolean hasNextRecord() {
        return iterator.hasNext();
    }

    @Override
    protected CobolRecord nextRecord() {
        incrementOffset();
        return new CobolRecord(schema, iterator.next());
    }

    @Override
    protected void seekFile(long offset) {
        if (currentOffset() > offset) {
            try {
                iterator = initIterator();
            } catch (Exception e) {
                throw new ConnectException("Error seeking file: " + getFilePath(), e);
            }
            closed = false;
            setOffset(0);
        }
        while (hasNext() && currentOffset() < offset) {
            nextRecord();
        }
    }

    @Override
    protected boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        try {
            stream.close();
        } catch (Exception e) {
            log.warn("{} An error has occurred while closing file stream.", this, e);
        }
        closed = true;
    }

    private static class FSStream implements SimpleStream {

        private final FileSystem fs;
        private final Path file;
        private final FSDataInputStream stream;
        private final long size;
        private long offset;

        FSStream(FileSystem fs, Path file) throws IOException {
            this.fs = fs;
            this.file = file;
            this.stream = this.fs.open(file);
            this.size = fs.getContentSummary(file).getLength();
            this.offset = stream.getPos();
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public String inputFileName() {
            return file.toString();
        }

        @Override
        public byte[] next(int numberOfBytes) throws IOException {
            int bytesToRead = (int) Math.min(numberOfBytes, size() - offset());
            byte[] bytes = new byte[bytesToRead];
            stream.readFully(bytes);
            offset += bytesToRead;
            return bytes;
        }

        @Override
        public void close() throws IOException {
            stream.close();
        }
    }

    static class CobolToStruct implements ReaderAdapter<CobolRecord> {

        public Struct apply(CobolRecord record) {
            Struct struct = new Struct(record.schema);
            record.row.stream()
                    .filter(col -> col instanceof Map)
                    .forEach(col -> {
                        Map<String, Object> column = (Map<String, Object>) col;
                        column.forEach((k, v) -> struct.put(k, mapValue(record.schema.field(k).schema(), k, v)));
                    });
            return struct;
        }

        private Object mapValue(Schema schema, String fieldName, Object value) {
            if (value == null) {
                return null;
            } else if (schema.type() == Schema.Type.ARRAY) {
                List<Object> items = (List<Object>) value;
                return items.stream()
                        .map(item -> mapValue(schema.valueSchema(), fieldName, ((Map<String, Object>) item).get(fieldName)))
                        .collect(Collectors.toList());
            } else if (schema.type() != Schema.Type.STRUCT) {
                return value;
            }
            Struct struct = new Struct(schema);
            Map<String, Object> map = (Map<String, Object>) value;
            map.forEach((k, v) -> struct.put(k, mapValue(schema.field(k).schema(), k, v)));
            return struct;
        }
    }

    static class CobolRecord {

        final Schema schema;
        final List<Object> row;

        CobolRecord(Schema schema, List<Object> row) {
            this.schema = schema;
            this.row = row;
        }

    }

    static class StructHandler implements RecordHandler<Map<String, Object>> {

        @Override
        public Map<String, Object> create(Object[] values, Group group) {
            return Collections.singletonMap(group.name(), mapValues(group, values));
        }

        @Override
        public Seq<Object> toSeq(Map<String, Object> record) {
            return asScalaBuffer(new ArrayList<>(record.values())).toSeq();
        }

        private Map<String, Object> mapValues(Group group, Object[] values) {
            List<Statement> statements = seqAsJavaList(group.children().toSeq());
            return IntStream.range(0, values.length)
                    .mapToObj(index -> new AbstractMap.SimpleEntry<>(statements.get(index), values[index]))
                    .map(entry -> transform(entry.getKey(), entry.getValue()))
                    .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
        }

        private Map.Entry<String, Object> transform(Statement child, Object value) {
            Object childValue;
            if (child instanceof Group && value instanceof Map) {
                childValue = ((Map<String, Object>) value).get(child.name());
            } else if (value instanceof Object[]) {
                childValue = Arrays.asList((Object[]) value);
            } else if (value instanceof ScalaNumber) {
                childValue = value instanceof scala.math.BigDecimal ?
                        ((BigDecimal) value).doubleValue() : ((ScalaNumber) value).longValue();
            } else {
                childValue = value;
            }
            return new AbstractMap.SimpleEntry<>(child.name(), childValue);
        }

    }
}
