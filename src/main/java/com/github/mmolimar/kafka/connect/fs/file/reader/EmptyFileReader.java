package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Map;

public class EmptyFileReader extends AbstractFileReader<Void> {
    /*
    An empty file reader that will always return no records
    Used as a null object instead of returning null
     */
    private boolean closed;

    public EmptyFileReader(FileSystem fs, Path filePath, Map<String, Object> config) {
        super(fs, filePath, (Void record) -> null, config);
        this.closed = false;
    }

    @Override
    protected void configure(Map<String, String> config) {
    }

    @Override
    protected Void nextRecord() {
        return null;
    }

    @Override
    protected boolean hasNextRecord() {
        return false;
    }

    @Override
    public void seekFile(long offset) {
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public void close() {
        closed = true;
    }
}
