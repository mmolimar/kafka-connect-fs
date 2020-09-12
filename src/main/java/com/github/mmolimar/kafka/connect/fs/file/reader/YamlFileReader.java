package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class YamlFileReader extends JacksonFileReader {

    private static final String FILE_READER_YAML = FILE_READER_PREFIX + "yaml.";
    private static final String FILE_READER_YAML_COMPRESSION = FILE_READER_YAML + "compression.";

    static final String FILE_READER_YAML_DESERIALIZATION_CONFIGS = FILE_READER_YAML + "deserialization.";

    public static final String FILE_READER_YAML_COMPRESSION_TYPE = FILE_READER_YAML_COMPRESSION + "type";
    public static final String FILE_READER_YAML_COMPRESSION_CONCATENATED = FILE_READER_YAML_COMPRESSION + "concatenated";
    public static final String FILE_READER_YAML_ENCODING = FILE_READER_YAML + "encoding";

    public YamlFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, config);
    }

    @Override
    protected Object readerEncodingConfig(Map<String, Object> config) {
        return config.get(FILE_READER_YAML_ENCODING);
    }

    @Override
    protected Object recordPerLineConfig(Map<String, Object> config) {
        return false;
    }

    @Override
    protected Object compressionTypeConfig(Map<String, Object> config) {
        return config.get(FILE_READER_YAML_COMPRESSION_TYPE);
    }

    @Override
    protected Object compressionConcatenatedConfig(Map<String, Object> config) {
        return config.get(FILE_READER_YAML_COMPRESSION_CONCATENATED);
    }

    @Override
    protected String deserializationConfigPrefix() {
        return FILE_READER_YAML_DESERIALIZATION_CONFIGS;
    }

    @Override
    protected ObjectMapper getObjectMapper() {
        return new YAMLMapper();
    }
}
