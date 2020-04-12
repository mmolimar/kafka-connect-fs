package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class AgnosticFileReader extends AbstractFileReader<AgnosticFileReader.AgnosticRecord> {

    private static final String FILE_READER_AGNOSTIC = FILE_READER_PREFIX + "agnostic.";
    private static final String FILE_READER_AGNOSTIC_EXTENSIONS = FILE_READER_AGNOSTIC + "extensions.";

    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET = FILE_READER_AGNOSTIC_EXTENSIONS + "parquet";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_AVRO = FILE_READER_AGNOSTIC_EXTENSIONS + "avro";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_SEQUENCE = FILE_READER_AGNOSTIC_EXTENSIONS + "sequence";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_JSON = FILE_READER_AGNOSTIC_EXTENSIONS + "json";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_CSV = FILE_READER_AGNOSTIC_EXTENSIONS + "csv";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_TSV = FILE_READER_AGNOSTIC_EXTENSIONS + "tsv";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_FIXED = FILE_READER_AGNOSTIC_EXTENSIONS + "fixed";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_TEXT = FILE_READER_AGNOSTIC_EXTENSIONS + "text";

    private final AbstractFileReader<Object> reader;
    private Set<String> parquetExtensions, avroExtensions, sequenceExtensions,
            jsonExtensions, csvExtensions, tsvExtensions, fixedExtensions;

    public AgnosticFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new AgnosticAdapter(), config);

        try {
            reader = readerByExtension(fs, filePath, config);
        } catch (RuntimeException | IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("An error has occurred when creating a concrete reader", t);
        }
    }

    private AbstractFileReader<Object> readerByExtension(FileSystem fs, Path filePath, Map<String, Object> config)
            throws Throwable {
        int index = filePath.getName().lastIndexOf('.');
        String extension = index == -1 || index == filePath.getName().length() - 1 ? "" :
                filePath.getName().substring(index + 1).toLowerCase();

        Class<? extends AbstractFileReader> clz;
        if (parquetExtensions.contains(extension)) {
            clz = ParquetFileReader.class;
        } else if (avroExtensions.contains(extension)) {
            clz = AvroFileReader.class;
        } else if (sequenceExtensions.contains(extension)) {
            clz = SequenceFileReader.class;
        } else if (jsonExtensions.contains(extension)) {
            clz = JsonFileReader.class;
        } else if (csvExtensions.contains(extension)) {
            clz = CsvFileReader.class;
        } else if (tsvExtensions.contains(extension)) {
            clz = TsvFileReader.class;
        } else if (fixedExtensions.contains(extension)) {
            clz = FixedWidthFileReader.class;
        } else {
            clz = TextFileReader.class;
        }

        return (AbstractFileReader<Object>) ReflectionUtils.makeReader(clz, fs, filePath, config);
    }

    @Override
    protected void configure(Map<String, String> config) {
        this.parquetExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET, "parquet")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.avroExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_AVRO, "avro")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.sequenceExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_SEQUENCE, "seq")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.jsonExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_JSON, "json")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.csvExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_CSV, "csv")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.tsvExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_TSV, "tsv")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.fixedExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_FIXED, "fixed")
                .toLowerCase().split(",")).collect(Collectors.toSet());
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public void seek(long offset) {
        reader.seek(offset);
    }

    @Override
    public long currentOffset() {
        return reader.currentOffset();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    protected AgnosticRecord nextRecord() {
        return new AgnosticRecord(reader.getAdapter(), reader.nextRecord());
    }

    static class AgnosticAdapter implements ReaderAdapter<AgnosticRecord> {

        @Override
        public Struct apply(AgnosticRecord ag) {
            return ag.adapter.apply(ag.record);
        }
    }

    static class AgnosticRecord {
        private final ReaderAdapter<Object> adapter;
        private final Object record;

        AgnosticRecord(ReaderAdapter<Object> adapter, Object record) {
            this.adapter = adapter;
            this.record = record;
        }
    }
}
