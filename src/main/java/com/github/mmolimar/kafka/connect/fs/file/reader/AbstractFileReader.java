package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;

public abstract class AbstractFileReader<T> implements FileReader {
    private final Path filePath;
    private ReaderAdapter<T> adapter;

    public AbstractFileReader(FileSystem fs, Path filePath, ReaderAdapter adapter) {
        if (fs == null || filePath == null) {
            throw new IllegalArgumentException("filesystem and filePath are required");
        }
        this.filePath = filePath;
        this.adapter = adapter;
    }

    @Override
    public Path getFilePath() {
        return filePath;
    }

    public final Struct next() {
        return adapter.apply(nextRecord());
    }

    protected abstract T nextRecord();

}
