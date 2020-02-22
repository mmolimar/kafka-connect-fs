package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class AgnosticFileReader extends AbstractFileReader<AgnosticFileReader.AgnosticRecord> {

    private static final String FILE_READER_AGNOSTIC = FILE_READER_PREFIX + "agnostic.";
    private static final String FILE_READER_AGNOSTIC_EXTENSIONS = FILE_READER_AGNOSTIC + "extensions.";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET = FILE_READER_AGNOSTIC_EXTENSIONS + "parquet";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_AVRO = FILE_READER_AGNOSTIC_EXTENSIONS + "avro";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_SEQUENCE = FILE_READER_AGNOSTIC_EXTENSIONS + "sequence";
    public static final String FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED = FILE_READER_AGNOSTIC_EXTENSIONS + "delimited";

    private final AbstractFileReader reader;
    private List<String> parquetExtensions, avroExtensions, sequenceExtensions, delimitedExtensions;

    public AgnosticFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new AgnosticAdapter(), config);

        try {
            reader = (AbstractFileReader) readerByExtension(fs, filePath, config);
        } catch (RuntimeException | IOException e) {
            throw e;
        } catch (Throwable t) {
            throw new IOException("An error has occurred when creating a concrete reader", t);
        }
    }

    private FileReader readerByExtension(FileSystem fs, Path filePath, Map<String, Object> config)
            throws Throwable {
        int index = filePath.getName().lastIndexOf('.');
        String extension = index == -1 || index == filePath.getName().length() - 1 ? "" :
                filePath.getName().substring(index + 1).toLowerCase();

        Class<? extends FileReader> clz;
        if (parquetExtensions.contains(extension)) {
            clz = ParquetFileReader.class;
        } else if (avroExtensions.contains(extension)) {
            clz = AvroFileReader.class;
        } else if (sequenceExtensions.contains(extension)) {
            clz = SequenceFileReader.class;
        } else if (delimitedExtensions.contains(extension)) {
            clz = DelimitedTextFileReader.class;
        } else {
            clz = TextFileReader.class;
        }

        return ReflectionUtils.makeReader(clz, fs, filePath, config);
    }

    @Override
    protected void configure(Map<String, Object> config) {
        this.parquetExtensions = config.get(FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET) == null ?
                Collections.singletonList("parquet") :
                Arrays.asList(config.get(FILE_READER_AGNOSTIC_EXTENSIONS_PARQUET).toString().toLowerCase().split(","));
        this.avroExtensions = config.get(FILE_READER_AGNOSTIC_EXTENSIONS_AVRO) == null ?
                Collections.singletonList("avro") :
                Arrays.asList(config.get(FILE_READER_AGNOSTIC_EXTENSIONS_AVRO).toString().toLowerCase().split(","));
        this.sequenceExtensions = config.get(FILE_READER_AGNOSTIC_EXTENSIONS_SEQUENCE) == null ?
                Collections.singletonList("seq") :
                Arrays.asList(config.get(FILE_READER_AGNOSTIC_EXTENSIONS_SEQUENCE).toString().toLowerCase().split(","));
        this.delimitedExtensions = config.get(FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED) == null ?
                Arrays.asList("tsv", "csv") :
                Arrays.asList(config.get(FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED).toString().toLowerCase().split(","));
    }

    @Override
    public boolean hasNext() {
        return reader.hasNext();
    }

    @Override
    public void seek(Offset offset) {
        reader.seek(offset);
    }

    @Override
    public Offset currentOffset() {
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

        AgnosticAdapter() {
        }

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
