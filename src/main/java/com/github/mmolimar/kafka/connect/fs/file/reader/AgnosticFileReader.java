package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

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
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_ORC = FILE_READER_AGNOSTIC_EXTENSIONS + "orc";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_COBOL = FILE_READER_AGNOSTIC_EXTENSIONS + "dat";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_BINARY = FILE_READER_AGNOSTIC_EXTENSIONS + "bin";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_JSON = FILE_READER_AGNOSTIC_EXTENSIONS + "json";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_XML = FILE_READER_AGNOSTIC_EXTENSIONS + "xml";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_YAML = FILE_READER_AGNOSTIC_EXTENSIONS + "yaml";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_CSV = FILE_READER_AGNOSTIC_EXTENSIONS + "csv";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_TSV = FILE_READER_AGNOSTIC_EXTENSIONS + "tsv";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_FIXED = FILE_READER_AGNOSTIC_EXTENSIONS + "fixed";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_TEXT = FILE_READER_AGNOSTIC_EXTENSIONS + "text";

    private final AbstractFileReader<Object> reader;
    private Set<String> parquetExtensions, avroExtensions, sequenceExtensions, orcExtensions, cobolExtensions,
            binaryExtensions, csvExtensions, tsvExtensions, fixedExtensions, jsonExtensions, xmlExtensions,
            yamlExtensions;

    public AgnosticFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws Exception {
        super(fs, filePath, new AgnosticAdapter(), config);

        try {
            reader = readerByExtension(fs, filePath, config);
        } catch (ConnectException ce) {
            throw (Exception) ce.getCause();
        }
    }

    @SuppressWarnings("unchecked")
    private AbstractFileReader<Object> readerByExtension(FileSystem fs, Path filePath, Map<String, Object> config) {
        int index = filePath.getName().lastIndexOf('.');
        String extension = index == -1 || index == filePath.getName().length() - 1 ? "" :
                filePath.getName().substring(index + 1).toLowerCase();

        Class<? extends AbstractFileReader<?>> clz;
        if (parquetExtensions.contains(extension)) {
            clz = ParquetFileReader.class;
        } else if (avroExtensions.contains(extension)) {
            clz = AvroFileReader.class;
        } else if (sequenceExtensions.contains(extension)) {
            clz = SequenceFileReader.class;
        } else if (orcExtensions.contains(extension)) {
            clz = OrcFileReader.class;
        } else if (cobolExtensions.contains(extension)) {
            clz = CobolFileReader.class;
        } else if (binaryExtensions.contains(extension)) {
            clz = BinaryFileReader.class;
        } else if (csvExtensions.contains(extension)) {
            clz = CsvFileReader.class;
        } else if (tsvExtensions.contains(extension)) {
            clz = TsvFileReader.class;
        } else if (fixedExtensions.contains(extension)) {
            clz = FixedWidthFileReader.class;
        } else if (jsonExtensions.contains(extension)) {
            clz = JsonFileReader.class;
        } else if (xmlExtensions.contains(extension)) {
            clz = XmlFileReader.class;
        } else if (yamlExtensions.contains(extension)) {
            clz = YamlFileReader.class;
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
        this.orcExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_ORC, "orc")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.cobolExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_COBOL, "dat")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.binaryExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_BINARY, "bin")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.csvExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_CSV, "csv")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.tsvExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_TSV, "tsv")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.fixedExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_FIXED, "fixed")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.jsonExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_JSON, "json")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.xmlExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_XML, "xml")
                .toLowerCase().split(",")).collect(Collectors.toSet());
        this.yamlExtensions = Arrays.stream(config.getOrDefault(FILE_READER_AGNOSTIC_EXTENSIONS_YAML, "yaml")
                .toLowerCase().split(",")).collect(Collectors.toSet());
    }

    @Override
    public boolean hasNextRecord() {
        return reader.hasNext();
    }

    @Override
    public void seekFile(long offset) {
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
    public boolean isClosed() {
        return reader.isClosed();
    }

    @Override
    protected AgnosticRecord nextRecord() throws IOException {
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
