package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import scala.collection.Seq;
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
import java.util.stream.IntStream;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;
import static scala.collection.JavaConverters.*;

public class CobolFileReader extends AbstractFileReader<CobolFileReader.CobolRecord> {

    private static final String FILE_READER_COBOL = FILE_READER_PREFIX + "cobol.";
    private static final String FILE_READER_COPYBOOK_PREFIX = FILE_READER_COBOL + "copybook.";

    public static final String FILE_READER_COPYBOOK_CONTENT = FILE_READER_COPYBOOK_PREFIX + "content";
    public static final String FILE_READER_COPYBOOK_PATH = FILE_READER_COPYBOOK_PREFIX + "path";

    private final VarLenReader reader;
    private final SimpleStream stream;
    private final Schema schema;
    private String copybook;
    private Iterator<List<Object>> iterator;
    private ReaderParameters params;
    private boolean closed;

    public CobolFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws Exception {
        super(fs, filePath, new CobolToStruct(), config);

        this.reader = CobolReader.varLenReader(copybook, params);
        this.stream = new FSStream(getFs(), getFilePath());
        this.schema = extractSchema(reader.getCobolSchema());
        this.iterator = initIterator();
        this.closed = false;
    }

    private Iterator<List<Object>> initIterator() throws Exception {
        return asJavaIterator(reader.getRecordIterator(stream, 0, 0, 0)
                .map(it -> seqAsJavaList(it.seq())));
    }

    private Schema extractSchema(CobolSchema cobolSchema) {
        SchemaBuilder builder = SchemaBuilder.struct();
        seqAsJavaList(cobolSchema.getCobolSchema().ast().children())
                .forEach(child -> builder.field(child.name(), schemaForField(child)));

        return builder.build();
    }

    private Schema schemaForField(Statement statement) {
        if (statement instanceof Group) {
            Group group = (Group) statement;
            SchemaBuilder builder = SchemaBuilder.struct();
            seqAsJavaList(group.children()).forEach(child -> builder.field(child.name(), schemaForField(child)));

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
        String content = Optional.ofNullable(config.get(FILE_READER_COPYBOOK_PATH))
                .map(Path::new)
                .map(path -> {
                    StringBuilder sb = new StringBuilder();
                    try (InputStream is = getFs().open(path);
                         BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                        br.lines().forEach(sb::append);
                    } catch (IOException ioe) {
                        throw new ConnectException("Cannot read Copybook file: " + path, ioe);
                    }
                    return sb.toString();
                })
                .orElseGet(() -> config.get(FILE_READER_COPYBOOK_CONTENT));

        if (content == null || content.trim().isEmpty()) {
            throw new ConnectException("Copybook is not specified.");
        }
        return content;
    }

    private ReaderParameters getReaderParameters(Map<String, String> config) {
        return new ReaderParameters(
                true,
                "common",
                scala.Option.apply(null),
                "",
                true,
                FloatingPointFormat$.MODULE$.IBM(),
                false, // variableSizeOccurs
                scala.Option.apply(null),
                true, // isRecordSequence
                false,
                false,
                0,
                false,
                scala.Option.apply(null),
                scala.Option.apply(null),
                scala.Option.apply(null),
                0,
                0,
                0,
                0,
                false,
                SchemaRetentionPolicy$.MODULE$.KeepOriginal(),
                StringTrimmingPolicy$.MODULE$.TrimBoth(),
                scala.Option.apply(null),
                new CommentPolicy(true, 6, 72),
                false,
                true,
                scala.collection.immutable.Nil$.empty(),
                null,
                DebugFieldsPolicy$.MODULE$.NoDebug(),
                scala.Option.apply(null),
                scala.Option.apply(null),
                ""
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

        FSStream(FileSystem fs, Path file) throws IOException {
            this.fs = fs;
            this.file = file;
            this.stream = this.fs.open(file);
        }

        @Override
        public long size() {
            try {
                return fs.getContentSummary(file).getLength();
            } catch (IOException ioe) {
                throw new ConnectException("Cannot retrieve length for file: " + file, ioe);
            }
        }

        @Override
        public long offset() {
            try {
                return stream.getPos();
            } catch (IOException ioe) {
                throw new ConnectException("Cannot get current position for file: " + file, ioe);
            }
        }

        @Override
        public String inputFileName() {
            return file.toString();
        }

        @Override
        public byte[] next(int numberOfBytes) throws IOException {
            byte[] bytes = new byte[(int) Math.min(numberOfBytes, size() - offset())];
            stream.readFully(bytes);
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
            record.row.forEach(col -> {
                Map<String, Object> column = (Map<String, Object>) col;
                column.forEach((k, v) -> struct.put(k, mapValue(record.schema.field(k).schema(), v)));
            });
            return struct;
        }

        private Object mapValue(Schema schema, Object value) {
            if (value == null) return null;

            if (value instanceof String || value instanceof Integer || value instanceof Double) {
                return value;
            }
            Struct struct = new Struct(schema);
            Map<String, Object> map = (Map<String, Object>) value;
            map.forEach((k, v) -> struct.put(k, mapValue(schema.field(k).schema(), v)));
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
            Object childValue = child instanceof Group ? ((Map<String, Object>) value).get(child.name()) : value;
            return new AbstractMap.SimpleEntry<>(child.name(), childValue);
        }

    }
}
