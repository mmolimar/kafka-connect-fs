package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public abstract class AbstractFileReader<T> implements FileReader {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final FileSystem fs;
    private final Path filePath;
    private final ReaderAdapter<T> adapter;
    private long offset;

    public AbstractFileReader(FileSystem fs, Path filePath, ReaderAdapter<T> adapter, Map<String, Object> config) {
        if (fs == null || filePath == null) {
            throw new IllegalArgumentException("File system and file path are required.");
        }
        this.fs = fs;
        this.filePath = filePath;
        this.adapter = adapter;
        this.offset = 0;

        configure(readerConfig(config));
        log.trace("Initialized file reader [{}] for file [{}].", getClass().getName(), filePath);
    }

    protected final Map<String, String> readerConfig(Map<String, Object> config) {
        return config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FILE_READER_PREFIX))
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
    }

    protected abstract void configure(Map<String, String> config);

    protected FileSystem getFs() {
        return fs;
    }

    @Override
    public Path getFilePath() {
        return filePath;
    }

    @Override
    public final Struct next() {
        if (!hasNext()) {
            throw new NoSuchElementException("There are no more records in file: " + getFilePath());
        }
        try {
            return adapter.apply(nextRecord());
        } catch (ConnectException ce) {
            throw ce;
        } catch (Exception e) {
            throw new ConnectException("Error processing next record in file: " + getFilePath(), e);
        }
    }

    @Override
    public long currentOffset() {
        return offset;
    }

    protected void incrementOffset() {
        this.offset++;
    }

    protected void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public final void seek(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Record offset must be greater than 0.");
        }
        checkClosed();
        try {
            seekFile(offset);
        } catch (IOException ioe) {
            throw new ConnectException("Error seeking file: " + getFilePath(), ioe);
        }
    }

    @Override
    public final boolean hasNext() {
        checkClosed();
        try {
            return hasNextRecord();
        } catch (ConnectException ce) {
            throw ce;
        } catch (Exception e) {
            throw new ConnectException("Error when checking if the reader has more records.", e);
        }
    }

    protected ReaderAdapter<T> getAdapter() {
        return adapter;
    }

    private void checkClosed() {
        if (isClosed()) {
            throw new ConnectException("File stream is closed!");
        }
    }

    protected abstract T nextRecord() throws IOException;

    protected abstract boolean hasNextRecord() throws IOException;

    protected abstract void seekFile(long offset) throws IOException;

    protected abstract boolean isClosed();

}
