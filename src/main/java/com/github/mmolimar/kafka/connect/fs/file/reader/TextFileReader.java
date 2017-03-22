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
import java.util.Map;
import java.util.NoSuchElementException;

public class TextFileReader extends AbstractFileReader<String> {

    public static final String FIELD_VALUE = "value";

    private final TextOffset offset;
    private String currentLine;
    private boolean finished = false;
    private LineNumberReader reader;

    public TextFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new TxtToStruct(), config);
        this.reader = new LineNumberReader(new InputStreamReader(fs.open(filePath)));
        this.offset = new TextOffset(0);
    }

    @Override
    protected void configure(Map<String, Object> config) {
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
    protected String nextRecord() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());
        }
        String aux = currentLine;
        currentLine = null;

        return aux;
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

    static class TxtToStruct implements ReaderAdapter<String> {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD_VALUE, SchemaBuilder.STRING_SCHEMA).build();

        @Override
        public Struct apply(String record) {
            return new Struct(schema).put(FIELD_VALUE, record);
        }
    }
}
