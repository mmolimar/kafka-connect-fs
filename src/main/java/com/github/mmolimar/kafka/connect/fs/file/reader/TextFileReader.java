package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class TextFileReader extends AbstractFileReader<TextFileReader.TextRecord> {

    public static final String FIELD_NAME_VALUE_DEFAULT = "value";

    private static final String FILE_READER_TEXT = FILE_READER_PREFIX + "text.";
    private static final String FILE_READER_SEQUENCE_FIELD_NAME_PREFIX = FILE_READER_TEXT + "field_name.";

    public static final String FILE_READER_TEXT_FIELD_NAME_VALUE = FILE_READER_SEQUENCE_FIELD_NAME_PREFIX + "value";
    public static final String FILE_READER_TEXT_ENCODING = FILE_READER_TEXT + "encoding";

    private final TextOffset offset;
    private String currentLine;
    private boolean finished = false;
    private LineNumberReader reader;
    private Schema schema;
    private Charset charset;

    public TextFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new TxtToStruct(), config);
        this.reader = new LineNumberReader(new InputStreamReader(fs.open(filePath), this.charset));
        this.offset = new TextOffset(0);
    }

    @Override
    protected void configure(Map<String, Object> config) {
        String valueFieldName;
        if (config.get(FILE_READER_TEXT_FIELD_NAME_VALUE) == null ||
                config.get(FILE_READER_TEXT_FIELD_NAME_VALUE).toString().equals("")) {
            valueFieldName = FIELD_NAME_VALUE_DEFAULT;
        } else {
            valueFieldName = config.get(FILE_READER_TEXT_FIELD_NAME_VALUE).toString();
        }
        this.schema = SchemaBuilder.struct()
                .field(valueFieldName, Schema.STRING_SCHEMA)
                .build();

        if (config.get(FILE_READER_TEXT_ENCODING) == null ||
                config.get(FILE_READER_TEXT_ENCODING).toString().equals("")) {
            this.charset = Charset.defaultCharset();
        } else {
            this.charset = Charset.forName(config.get(FILE_READER_TEXT_ENCODING).toString());
        }
    }

    @Override
    public boolean hasNext() {
        if (currentLine != null) {
            return true;
        } else if (finished) {
            return false;
        } else {
            try {
                while (true) {
                    String line = reader.readLine();
                    offset.setOffset(reader.getLineNumber());
                    if (line == null) {
                        finished = true;
                        return false;
                    }
                    currentLine = line;
                    return true;
                }
            } catch (IOException ioe) {
                throw new IllegalStateException(ioe);
            }
        }
    }

    @Override
    protected TextRecord nextRecord() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());
        }
        String aux = currentLine;
        currentLine = null;

        return new TextRecord(schema, aux);
    }

    @Override
    public void seek(Offset offset) {
        if (offset.getRecordOffset() < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0");
        }
        try {
            if (offset.getRecordOffset() < reader.getLineNumber()) {
                this.reader = new LineNumberReader(new InputStreamReader(getFs().open(getFilePath())));
                currentLine = null;
            }
            while ((currentLine = reader.readLine()) != null) {
                if (reader.getLineNumber() - 1 == offset.getRecordOffset()) {
                    this.offset.setOffset(reader.getLineNumber());
                    return;
                }
            }
            this.offset.setOffset(reader.getLineNumber());
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

    public static class TextOffset implements Offset {
        private long offset;

        public TextOffset(long offset) {
            this.offset = offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        @Override
        public long getRecordOffset() {
            return offset;
        }
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

        public TextRecord(Schema schema, String value) {
            this.schema = schema;
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
