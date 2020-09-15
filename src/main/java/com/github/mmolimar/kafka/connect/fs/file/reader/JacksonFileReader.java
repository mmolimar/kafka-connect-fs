package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

abstract class JacksonFileReader extends AbstractFileReader<JacksonFileReader.JacksonRecord> {

    private final TextFileReader inner;
    private final Schema schema;
    private ObjectMapper mapper;

    public JacksonFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new JacksonToStruct(), config);

        config.put(TextFileReader.FILE_READER_TEXT_ENCODING, readerEncodingConfig(config));
        config.put(TextFileReader.FILE_READER_TEXT_RECORD_PER_LINE, recordPerLineConfig(config));
        config.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, compressionTypeConfig(config));
        config.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_CONCATENATED, compressionConcatenatedConfig(config));

        this.inner = new TextFileReader(fs, filePath, config);

        if (hasNext()) {
            String line = inner.nextRecord().getValue();
            this.schema = extractSchema(mapper.readTree(line));
            // back to the first line
            inner.seek(0);
        } else {
            this.schema = SchemaBuilder.struct().build();
        }
    }

    protected abstract Object readerEncodingConfig(Map<String, Object> config);

    protected abstract Object recordPerLineConfig(Map<String, Object> config);

    protected abstract Object compressionTypeConfig(Map<String, Object> config);

    protected abstract Object compressionConcatenatedConfig(Map<String, Object> config);

    protected abstract String deserializationConfigPrefix();

    protected abstract ObjectMapper getObjectMapper();

    @Override
    protected void configure(Map<String, String> config) {
        mapper = getObjectMapper();
        Set<String> deserializationFeatures = Arrays.stream(DeserializationFeature.values())
                .map(Enum::name)
                .collect(Collectors.toSet());
        config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(deserializationConfigPrefix()))
                .forEach(entry -> {
                    String feature = entry.getKey().replaceAll(deserializationConfigPrefix(), "");
                    if (deserializationFeatures.contains(feature)) {
                        mapper.configure(DeserializationFeature.valueOf(feature),
                                Boolean.parseBoolean(entry.getValue()));
                    } else {
                        log.warn("{} Ignoring deserialization configuration [{}] due to it does not exist.",
                                this, feature);
                    }
                });
    }

    @Override
    protected JacksonRecord nextRecord() throws IOException {
        JsonNode value = mapper.readTree(inner.nextRecord().getValue());
        return new JacksonRecord(schema, value);
    }

    @Override
    public boolean hasNextRecord() throws IOException {
        return inner.hasNextRecord();
    }

    @Override
    public void seekFile(long offset) throws IOException {
        inner.seekFile(offset);
    }

    @Override
    public long currentOffset() {
        return inner.currentOffset();
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }

    @Override
    public boolean isClosed() {
        return inner.isClosed();
    }

    private static Schema extractSchema(JsonNode jsonNode) {
        switch (jsonNode.getNodeType()) {
            case BOOLEAN:
                return Schema.OPTIONAL_BOOLEAN_SCHEMA;
            case NUMBER:
                if (jsonNode.isShort()) {
                    return Schema.OPTIONAL_INT8_SCHEMA;
                } else if (jsonNode.isInt()) {
                    return Schema.OPTIONAL_INT32_SCHEMA;
                } else if (jsonNode.isLong()) {
                    return Schema.OPTIONAL_INT64_SCHEMA;
                } else if (jsonNode.isBigInteger()) {
                    return Schema.OPTIONAL_INT64_SCHEMA;
                } else {
                    return Schema.OPTIONAL_FLOAT64_SCHEMA;
                }
            case STRING:
                return Schema.OPTIONAL_STRING_SCHEMA;
            case BINARY:
                return Schema.OPTIONAL_BYTES_SCHEMA;
            case ARRAY:
                Iterable<JsonNode> elements = jsonNode::elements;
                Schema arraySchema = StreamSupport.stream(elements.spliterator(), false)
                        .findFirst().map(JacksonFileReader::extractSchema)
                        .orElse(SchemaBuilder.struct().build());
                return SchemaBuilder.array(arraySchema).build();
            case OBJECT:
                SchemaBuilder builder = SchemaBuilder.struct();
                jsonNode.fields()
                        .forEachRemaining(field -> builder.field(field.getKey(), extractSchema(field.getValue())));
                return builder.build();
            default:
                return SchemaBuilder.struct().optional().build();
        }
    }

    static class JacksonToStruct implements ReaderAdapter<JacksonRecord> {

        @Override
        public Struct apply(JacksonRecord record) {
            return toStruct(record.schema, record.value);
        }

        private Struct toStruct(Schema schema, JsonNode jsonNode) {
            if (jsonNode.isNull()) return null;
            Struct struct = new Struct(schema);
            jsonNode.fields()
                    .forEachRemaining(field -> struct.put(
                            field.getKey(),
                            mapValue(struct.schema().field(field.getKey()).schema(), field.getValue())
                    ));
            return struct;
        }

        private Object mapValue(Schema schema, JsonNode value) {
            if (value == null) return null;

            switch (value.getNodeType()) {
                case BOOLEAN:
                    return value.booleanValue();
                case NUMBER:
                    if (value.isShort()) {
                        return value.shortValue();
                    } else if (value.isInt()) {
                        return value.intValue();
                    } else if (value.isLong()) {
                        return value.longValue();
                    } else if (value.isBigInteger()) {
                        return value.bigIntegerValue().longValue();
                    } else {
                        return value.numberValue().doubleValue();
                    }
                case STRING:
                    return value.asText();
                case BINARY:
                    try {
                        return value.binaryValue();
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                case OBJECT:
                case POJO:
                    Struct struct = new Struct(schema);
                    Iterable<Map.Entry<String, JsonNode>> fields = value::fields;
                    StreamSupport.stream(fields.spliterator(), false)
                            .forEach(field -> struct.put(field.getKey(),
                                    mapValue(extractSchema(field.getValue()), field.getValue()))
                            );
                    return struct;
                case ARRAY:
                    Iterable<JsonNode> arrayElements = value::elements;
                    return StreamSupport.stream(arrayElements.spliterator(), false)
                            .map(elm -> mapValue(schema.valueSchema(), elm))
                            .collect(Collectors.toList());
                case NULL:
                case MISSING:
                default:
                    return null;
            }
        }
    }

    static class JacksonRecord {
        private final Schema schema;
        private final JsonNode value;

        JacksonRecord(Schema schema, JsonNode value) {
            this.schema = schema;
            this.value = value;
        }
    }
}
