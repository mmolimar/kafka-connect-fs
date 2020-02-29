package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public abstract class AbstractFileReader<T> implements FileReader {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final FileSystem fs;
    private final Path filePath;
    private ReaderAdapter<T> adapter;

    public AbstractFileReader(FileSystem fs, Path filePath, ReaderAdapter<T> adapter, Map<String, Object> config) {
        if (fs == null || filePath == null) {
            throw new IllegalArgumentException("fileSystem and filePath are required");
        }
        this.fs = fs;
        this.filePath = filePath;
        this.adapter = adapter;

        Map<String, Object> readerConf = config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FILE_READER_PREFIX))
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        configure(readerConf);
    }

    protected abstract void configure(Map<String, Object> config);

    protected FileSystem getFs() {
        return fs;
    }

    @Override
    public Path getFilePath() {
        return filePath;
    }

    public final Struct next() {
        return adapter.apply(nextRecord());
    }

    protected abstract T nextRecord();

    protected ReaderAdapter<T> getAdapter() {
        return adapter;
    }
}
