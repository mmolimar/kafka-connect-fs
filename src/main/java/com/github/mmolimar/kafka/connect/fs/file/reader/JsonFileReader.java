package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class JsonFileReader extends JacksonFileReader {

    private static final String FILE_READER_JSON = FILE_READER_PREFIX + "json.";
    private static final String FILE_READER_JSON_COMPRESSION = FILE_READER_JSON + "compression.";

    static final String FILE_READER_JSON_DESERIALIZATION_CONFIGS = FILE_READER_JSON + "deserialization.";

    public static final String FILE_READER_JSON_RECORD_PER_LINE = FILE_READER_JSON + "record_per_line";
    public static final String FILE_READER_JSON_COMPRESSION_TYPE = FILE_READER_JSON_COMPRESSION + "type";
    public static final String FILE_READER_JSON_COMPRESSION_CONCATENATED = FILE_READER_JSON_COMPRESSION + "concatenated";
    public static final String FILE_READER_JSON_ENCODING = FILE_READER_JSON + "encoding";

    public JsonFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, config);
    }

    @Override
    protected Object readerEncodingConfig(Map<String, Object> config) {
        return config.get(FILE_READER_JSON_ENCODING);
    }

    @Override
    protected Object recordPerLineConfig(Map<String, Object> config) {
        return config.get(FILE_READER_JSON_RECORD_PER_LINE);
    }

    @Override
    protected Object compressionTypeConfig(Map<String, Object> config) {
        return config.get(FILE_READER_JSON_COMPRESSION_TYPE);
    }

    @Override
    protected Object compressionConcatenatedConfig(Map<String, Object> config) {
        return config.get(FILE_READER_JSON_COMPRESSION_CONCATENATED);
    }

    @Override
    protected String deserializationConfigPrefix() {
        return FILE_READER_JSON_DESERIALIZATION_CONFIGS;
    }

    @Override
    protected ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }
}
