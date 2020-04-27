package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.common.CommonParserSettings;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.common.record.Record;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

abstract class UnivocityFileReader<T extends CommonParserSettings<?>>
        extends AbstractFileReader<UnivocityFileReader.UnivocityRecord> {

    private static final String FILE_READER_DELIMITED = FILE_READER_PREFIX + "delimited.";
    private static final String FILE_READER_COMPRESSION = FILE_READER_DELIMITED + "compression.";

    protected static final String FILE_READER_DELIMITED_SETTINGS = FILE_READER_DELIMITED + "settings.";
    protected static final String FILE_READER_DELIMITED_SETTINGS_FORMAT = FILE_READER_DELIMITED_SETTINGS + "format.";

    public static final String FILE_READER_DELIMITED_SETTINGS_HEADER = FILE_READER_DELIMITED_SETTINGS + "header";
    public static final String FILE_READER_DELIMITED_SETTINGS_SCHEMA = FILE_READER_DELIMITED_SETTINGS + "schema";
    public static final String FILE_READER_DELIMITED_SETTINGS_DATA_TYPE_MAPPING_ERROR = FILE_READER_DELIMITED_SETTINGS + "data_type_mapping_error";
    public static final String FILE_READER_DELIMITED_SETTINGS_ALLOW_NULLS = FILE_READER_DELIMITED_SETTINGS + "allow_nulls";
    public static final String FILE_READER_DELIMITED_SETTINGS_HEADER_NAMES = FILE_READER_DELIMITED_SETTINGS + "header_names";
    public static final String FILE_READER_DELIMITED_SETTINGS_LINE_SEPARATOR_DETECTION = FILE_READER_DELIMITED_SETTINGS + "line_separator_detection";
    public static final String FILE_READER_DELIMITED_SETTINGS_NULL_VALUE = FILE_READER_DELIMITED_SETTINGS + "null_value";
    public static final String FILE_READER_DELIMITED_SETTINGS_MAX_COLUMNS = FILE_READER_DELIMITED_SETTINGS + "max_columns";
    public static final String FILE_READER_DELIMITED_SETTINGS_MAX_CHARS_PER_COLUMN = FILE_READER_DELIMITED_SETTINGS + "max_chars_per_column";
    public static final String FILE_READER_DELIMITED_SETTINGS_ROWS_TO_SKIP = FILE_READER_DELIMITED_SETTINGS + "rows_to_skip";
    public static final String FILE_READER_DELIMITED_SETTINGS_ILW = FILE_READER_DELIMITED_SETTINGS + "ignore_leading_whitespaces";
    public static final String FILE_READER_DELIMITED_SETTINGS_ITW = FILE_READER_DELIMITED_SETTINGS + "ignore_trailing_whitespaces";

    public static final String FILE_READER_DELIMITED_SETTINGS_FORMAT_LINE_SEP = FILE_READER_DELIMITED_SETTINGS_FORMAT + "line_separator";
    public static final String FILE_READER_DELIMITED_SETTINGS_FORMAT_COMMENT = FILE_READER_DELIMITED_SETTINGS_FORMAT + "comment";

    public static final String FILE_READER_DELIMITED_COMPRESSION_TYPE = FILE_READER_COMPRESSION + "type";
    public static final String FILE_READER_DELIMITED_COMPRESSION_CONCATENATED = FILE_READER_COMPRESSION + "concatenated";
    public static final String FILE_READER_DELIMITED_ENCODING = FILE_READER_DELIMITED + "encoding";

    private static final String DEFAULT_COLUMN_NAME = "column_";

    private T settings;
    private Schema schema;
    private Charset charset;
    private CompressionType compression;
    private boolean dataTypeMappingError;
    private boolean allowNulls;
    private boolean closed;

    private ResultIterator<Record, ParsingContext> iterator;

    public enum DataType {
        BYTE,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE,
        BOOLEAN,
        BYTES,
        STRING
    }

    public UnivocityFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new UnivocityToStruct(), config);

        this.iterator = iterateRecords();
        this.schema = buildSchema(this.iterator, settings.isHeaderExtractionEnabled(), config);
    }

    private Schema buildSchema(ResultIterator<Record, ParsingContext> it, boolean hasHeader, Map<String, Object> config) {
        SchemaBuilder builder = SchemaBuilder.struct();
        if (it.hasNext() && !hasHeader) {
            Record first = it.next();
            List<Schema> dataTypes = getDataTypes(config, first.getValues());
            IntStream.range(0, first.getValues().length)
                    .forEach(index -> builder.field(DEFAULT_COLUMN_NAME + (index + 1), dataTypes.get(index)));
            seek(0);
        } else if (hasHeader) {
            Optional.ofNullable(it.getContext().headers()).ifPresent(headers -> {
                List<Schema> dataTypes = getDataTypes(config, headers);
                IntStream.range(0, headers.length)
                        .forEach(index -> builder.field(headers[index], dataTypes.get(index)));
            });
        }
        return builder.build();
    }

    @Override
    protected void configure(Map<String, String> config) {
        String cType = config.getOrDefault(FILE_READER_DELIMITED_COMPRESSION_TYPE, CompressionType.NONE.toString());
        boolean concatenated = Boolean.parseBoolean(config.getOrDefault(FILE_READER_DELIMITED_COMPRESSION_CONCATENATED,
                "true"));
        this.compression = CompressionType.fromName(cType, concatenated);
        this.charset = Charset.forName(config.getOrDefault(FILE_READER_DELIMITED_ENCODING, Charset.defaultCharset().name()));
        this.settings = allSettings(config);
        this.dataTypeMappingError = Boolean.parseBoolean(
                config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_DATA_TYPE_MAPPING_ERROR, "true"));
        if (this.dataTypeMappingError) {
            this.allowNulls = Boolean.parseBoolean(
                    config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_ALLOW_NULLS, "false"));
        } else {
            this.allowNulls = true;
        }

    }

    private List<Schema> getDataTypes(Map<String, Object> config, String[] headers) {
        List<Schema> dataTypes = Arrays
                .stream(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_SCHEMA, "").toString().split(","))
                .filter(dt -> !dt.trim().isEmpty())
                .map(this::strToSchema)
                .collect(Collectors.toList());
        if (dataTypes.size() > 0 && dataTypes.size() != headers.length) {
            throw new IllegalArgumentException("The schema defined in property '" + FILE_READER_DELIMITED_SETTINGS_SCHEMA +
                    "' does not match the number of fields inferred in the file.");
        } else if (dataTypes.size() == 0) {
            return IntStream.range(0, headers.length)
                    .mapToObj(index -> Schema.STRING_SCHEMA)
                    .collect(Collectors.toList());
        }
        return dataTypes;
    }

    private Schema strToSchema(String dataType) {
        switch (DataType.valueOf(dataType.trim().toUpperCase())) {
            case BYTE:
                return dataTypeMappingError && !allowNulls ? Schema.INT8_SCHEMA : Schema.OPTIONAL_INT8_SCHEMA;
            case SHORT:
                return dataTypeMappingError && !allowNulls ? Schema.INT16_SCHEMA : Schema.OPTIONAL_INT16_SCHEMA;
            case INT:
                return dataTypeMappingError && !allowNulls ? Schema.INT32_SCHEMA : Schema.OPTIONAL_INT32_SCHEMA;
            case LONG:
                return dataTypeMappingError && !allowNulls ? Schema.INT64_SCHEMA : Schema.OPTIONAL_INT64_SCHEMA;
            case FLOAT:
                return dataTypeMappingError && !allowNulls ? Schema.FLOAT32_SCHEMA : Schema.OPTIONAL_FLOAT32_SCHEMA;
            case DOUBLE:
                return dataTypeMappingError && !allowNulls ? Schema.FLOAT64_SCHEMA : Schema.OPTIONAL_FLOAT64_SCHEMA;
            case BOOLEAN:
                return dataTypeMappingError && !allowNulls ? Schema.BOOLEAN_SCHEMA : Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case BYTES:
                return dataTypeMappingError && !allowNulls ? Schema.BYTES_SCHEMA : Schema.OPTIONAL_BYTES_SCHEMA;
            case STRING:
            default:
                return dataTypeMappingError && !allowNulls ? Schema.STRING_SCHEMA : Schema.OPTIONAL_STRING_SCHEMA;
        }
    }

    private T allSettings(Map<String, String> config) {
        T settings = parserSettings(config);
        settings.setHeaderExtractionEnabled(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_HEADER, false));
        settings.setHeaders(Optional.ofNullable(config.get(FILE_READER_DELIMITED_SETTINGS_HEADER_NAMES))
                .map(headers -> headers.split(",")).orElse(null));
        settings.setLineSeparatorDetectionEnabled(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_LINE_SEPARATOR_DETECTION, false));
        settings.setNullValue(config.get(FILE_READER_DELIMITED_SETTINGS_NULL_VALUE));
        settings.setMaxColumns(Integer.parseInt(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_MAX_COLUMNS, "512")));
        settings.setMaxCharsPerColumn(Integer.parseInt(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_MAX_CHARS_PER_COLUMN, "4096")));
        settings.setNumberOfRowsToSkip(Long.parseLong(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_ROWS_TO_SKIP, "0")));
        settings.setIgnoreLeadingWhitespaces(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_ILW, true));
        settings.setIgnoreTrailingWhitespaces(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_ITW, true));
        settings.getFormat().setLineSeparator(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_FORMAT_LINE_SEP, "\n"));
        settings.getFormat().setComment(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_FORMAT_COMMENT, "#").charAt(0));

        return settings;
    }

    protected boolean getBoolean(Map<String, String> config, String property, boolean defaultValue) {
        return Boolean.parseBoolean(config.getOrDefault(property, String.valueOf(defaultValue)));
    }

    protected abstract T parserSettings(Map<String, String> config);

    protected abstract AbstractParser<T> createParser(T settings);

    private Reader getFileReader(InputStream is, CompressionType compression, Charset charset) throws IOException {
        final InputStreamReader isr;
        switch (compression) {
            case BZIP2:
                isr = new InputStreamReader(new BZip2CompressorInputStream(is, compression.isConcatenated()), charset);
                break;
            case GZIP:
                isr = new InputStreamReader(new GzipCompressorInputStream(is, compression.isConcatenated()), charset);
                break;
            default:
                isr = new InputStreamReader(is, charset);
                break;
        }
        return isr;
    }

    private ResultIterator<Record, ParsingContext> iterateRecords() throws IOException {
        return createParser(settings)
                .iterateRecords(getFileReader(getFs().open(getFilePath()), this.compression, this.charset))
                .iterator();
    }

    @Override
    protected final UnivocityRecord nextRecord() {
        incrementOffset();
        return new UnivocityRecord(schema, iterator.next(), dataTypeMappingError);
    }

    @Override
    public final boolean hasNextRecord() {
        return iterator.hasNext();
    }

    @Override
    public final void seekFile(long offset) throws IOException {
        if (offset > currentOffset()) {
            iterator.hasNext();
            iterator.getContext().skipLines(offset - currentOffset() - 1);
            iterator.next();
        } else {
            iterator = iterateRecords();
            iterator.hasNext();
            iterator.getContext().skipLines(offset);
        }
        setOffset(offset);
    }

    @Override
    public final void close() {
        iterator.getContext().stop();
        closed = true;
    }

    @Override
    public final boolean isClosed() {
        return closed;
    }

    static class UnivocityToStruct implements ReaderAdapter<UnivocityRecord> {

        @Override
        public Struct apply(UnivocityRecord record) {
            Struct struct = new Struct(record.schema);
            IntStream.range(0, record.schema.fields().size())
                    .filter(index -> index < record.value.getValues().length)
                    .forEach(index -> {
                        Schema.Type type = record.schema.fields().get(index).schema().type();
                        String fieldName = record.schema.fields().get(index).name();
                        struct.put(fieldName, mapDatatype(type, record.value, index, record.dataTypeMappingError));
                    });
            return struct;
        }

        private Object mapDatatype(Schema.Type type, Record record, int fieldIndex, boolean dataTypeMappingError) {
            try {
                switch (type) {
                    case INT8:
                        return record.getByte(fieldIndex);
                    case INT16:
                        return record.getShort(fieldIndex);
                    case INT32:
                        return record.getInt(fieldIndex);
                    case INT64:
                        return record.getLong(fieldIndex);
                    case FLOAT32:
                        return record.getFloat(fieldIndex);
                    case FLOAT64:
                        return record.getDouble(fieldIndex);
                    case BOOLEAN:
                        return record.getBoolean(fieldIndex);
                    case BYTES:
                        return record.getString(fieldIndex).getBytes();
                    case ARRAY:
                    case MAP:
                    case STRUCT:
                    case STRING:
                    default:
                        return record.getString(fieldIndex);
                }
            } catch (RuntimeException re) {
                if (dataTypeMappingError) {
                    throw re;
                }
                return null;
            }
        }
    }

    static class UnivocityRecord {
        private final Schema schema;
        private final Record value;
        private final boolean dataTypeMappingError;

        UnivocityRecord(Schema schema, Record value, boolean dataTypeMappingError) {
            this.schema = schema;
            this.value = value;
            this.dataTypeMappingError = dataTypeMappingError;
        }
    }
}
