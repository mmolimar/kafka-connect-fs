package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class TextFileReader extends AbstractFileReader<TextFileReader.TextRecord> {

    private static final String FILE_READER_TEXT = FILE_READER_PREFIX + "text.";
    private static final String FILE_READER_FIELD_NAME_PREFIX = FILE_READER_TEXT + "field_name.";
    private static final String FILE_READER_TEXT_COMPRESSION = FILE_READER_TEXT + "compression.";

    public static final String FIELD_NAME_VALUE_DEFAULT = "value";

    public static final String FILE_READER_TEXT_FIELD_NAME_VALUE = FILE_READER_FIELD_NAME_PREFIX + "value";
    public static final String FILE_READER_TEXT_RECORD_PER_LINE = FILE_READER_TEXT + "record_per_line";
    public static final String FILE_READER_TEXT_COMPRESSION_TYPE = FILE_READER_TEXT_COMPRESSION + "type";
    public static final String FILE_READER_TEXT_COMPRESSION_CONCATENATED = FILE_READER_TEXT_COMPRESSION + "concatenated";
    public static final String FILE_READER_TEXT_ENCODING = FILE_READER_TEXT + "encoding";

    private String current;
    private boolean finished = false;
    private LineNumberReader reader;
    private Schema schema;
    private Charset charset;
    private CompressionType compression;
    private boolean recordPerLine;
    private boolean closed;

    public TextFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new TxtToStruct(), config);
        this.reader = new LineNumberReader(getFileReader(fs.open(filePath)));
        this.closed = false;
    }

    @Override
    protected void configure(Map<String, String> config) {
        this.schema = SchemaBuilder.struct()
                .field(config.getOrDefault(FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE_DEFAULT),
                        Schema.STRING_SCHEMA)
                .build();
        this.recordPerLine = Boolean.parseBoolean(config.getOrDefault(FILE_READER_TEXT_RECORD_PER_LINE, "true"));
        String cType = config.getOrDefault(FILE_READER_TEXT_COMPRESSION_TYPE, CompressionType.NONE.toString());
        boolean concatenated = Boolean.parseBoolean(config.getOrDefault(FILE_READER_TEXT_COMPRESSION_CONCATENATED,
                "true"));
        this.compression = CompressionType.fromName(cType, concatenated);
        this.charset = Charset.forName(config.getOrDefault(FILE_READER_TEXT_ENCODING, Charset.defaultCharset().name()));
    }

    private Reader getFileReader(InputStream inputStream) throws IOException {
        final InputStreamReader isr;
        switch (this.compression) {
            case BZIP2:
                isr = new InputStreamReader(new BZip2CompressorInputStream(inputStream,
                        this.compression.isConcatenated()), this.charset);
                break;
            case GZIP:
                isr = new InputStreamReader(new GzipCompressorInputStream(inputStream,
                        this.compression.isConcatenated()), this.charset);
                break;
            default:
                isr = new InputStreamReader(inputStream, this.charset);
                break;
        }
        return isr;
    }

    @Override
    public boolean hasNextRecord() throws IOException {
        if (current != null) {
            return true;
        } else if (finished) {
            return false;
        } else {
            if (!recordPerLine) {
                List<String> lines = new BufferedReader(reader).lines().collect(Collectors.toList());
                current = String.join("\n", lines);
                finished = true;
                return true;
            }
            for (; ; ) {
                String line = reader.readLine();
                if (line == null) {
                    finished = true;
                    return false;
                }
                current = line;
                return true;
            }
        }
    }

    @Override
    protected TextRecord nextRecord() {
        String aux = current;
        current = null;
        incrementOffset();
        return new TextRecord(schema, aux);
    }

    @Override
    public void seekFile(long offset) throws IOException {
        current = null;
        if (offset < reader.getLineNumber()) {
            finished = false;
            reader.close();
            reader = new LineNumberReader(getFileReader(getFs().open(getFilePath())));
        }
        while (reader.getLineNumber() < offset) {
            reader.readLine();
        }
        setOffset(reader.getLineNumber());
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

    static class TxtToStruct implements ReaderAdapter<TextRecord> {

        @Override
        public Struct apply(TextRecord record) {
            return new Struct(record.schema)
                    .put(record.schema.fields().get(0), record.value);
        }
    }

    static class TextRecord {
        private final Schema schema;
        private final String value;

        TextRecord(Schema schema, String value) {
            this.schema = schema;
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
