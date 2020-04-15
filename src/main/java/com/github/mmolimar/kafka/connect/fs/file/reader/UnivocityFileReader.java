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
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

abstract class UnivocityFileReader<T extends CommonParserSettings<?>>
        extends AbstractFileReader<UnivocityFileReader.UnivocityRecord> {

    private static final String FILE_READER_DELIMITED = FILE_READER_PREFIX + "delimited.";
    private static final String FILE_READER_COMPRESSION = FILE_READER_DELIMITED + "compression.";

    protected static final String FILE_READER_DELIMITED_SETTINGS = FILE_READER_DELIMITED + "settings.";
    protected static final String FILE_READER_DELIMITED_SETTINGS_FORMAT = FILE_READER_DELIMITED_SETTINGS + "format.";

    public static final String FILE_READER_DELIMITED_SETTINGS_HEADER = FILE_READER_DELIMITED_SETTINGS + "header";
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
    private boolean closed;

    private ResultIterator<Record, ParsingContext> iterator;

    public UnivocityFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new UnivocityToStruct(), config);

        this.iterator = iterateRecords();
        this.schema = buildSchema(this.iterator, settings.isHeaderExtractionEnabled());
    }

    private Schema buildSchema(ResultIterator<Record, ParsingContext> it, boolean hasHeader) {
        SchemaBuilder builder = SchemaBuilder.struct();
        if (it.hasNext() && !hasHeader) {
            Record first = it.next();
            IntStream.range(0, first.getValues().length)
                    .forEach(index -> builder.field(DEFAULT_COLUMN_NAME + ++index, SchemaBuilder.STRING_SCHEMA));
            seek(0);
        } else if (hasHeader) {
            Optional.ofNullable(it.getContext().headers()).ifPresent(headers -> {
                IntStream.range(0, headers.length)
                        .forEach(index -> builder.field(headers[index], SchemaBuilder.STRING_SCHEMA));
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
        Record record = iterator.next();
        return new UnivocityRecord(schema, record.getValues());
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
                    .filter(index -> index < record.values.length)
                    .forEach(index -> struct.put(record.schema.fields().get(index).name(), record.values[index]));
            return struct;
        }
    }

    static class UnivocityRecord {
        private final Schema schema;
        private final String[] values;

        UnivocityRecord(Schema schema, String[] values) {
            this.schema = schema;
            this.values = values;
        }
    }
}
