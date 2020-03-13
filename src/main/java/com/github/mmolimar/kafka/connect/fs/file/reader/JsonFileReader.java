package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
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

import static com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig.FILE_READER_PREFIX;

public class JsonFileReader extends AbstractFileReader<JsonFileReader.JsonRecord> {

    private static final String FILE_READER_JSON = FILE_READER_PREFIX + "json.";
    private static final String FILE_READER_JSON_COMPRESSION = FILE_READER_JSON + "compression.";

    public static final String FILE_READER_JSON_DESERIALIZATION_CONFIGS = FILE_READER_JSON + "deserialization.";
    public static final String FILE_READER_JSON_RECORD_PER_LINE = FILE_READER_JSON + "record_per_line";
    public static final String FILE_READER_JSON_ENCODING = FILE_READER_JSON + "encoding";
    public static final String FILE_READER_JSON_COMPRESSION_TYPE = FILE_READER_JSON_COMPRESSION + "type";
    public static final String FILE_READER_JSON_COMPRESSION_CONCATENATED = FILE_READER_JSON_COMPRESSION + "concatenated";

    private final TextFileReader inner;
    private final Schema schema;
    private ObjectMapper mapper;

    public JsonFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new JsonToStruct(), config);

        config.put(TextFileReader.FILE_READER_TEXT_ENCODING, config.get(FILE_READER_JSON_ENCODING));
        config.put(TextFileReader.FILE_READER_TEXT_RECORD_PER_LINE, config.get(FILE_READER_JSON_RECORD_PER_LINE));
        config.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, config.get(FILE_READER_JSON_COMPRESSION_TYPE));
        config.put(TextFileReader.FILE_READER_TEXT_COMPRESSION_CONCATENATED, config.get(FILE_READER_JSON_COMPRESSION_CONCATENATED));

        this.inner = new TextFileReader(fs, filePath, config);

        if (hasNext()) {
            String line = inner.nextRecord().getValue();
            this.schema = extractSchema(mapper.readTree(line));
            //back to the first line
            inner.seek(() -> 0);
        } else {
            this.schema = SchemaBuilder.struct().build();
        }
    }

    @Override
    protected void configure(Map<String, String> config) {
        mapper = new ObjectMapper();
        Set<String> deserializationFeatures = Arrays.stream(DeserializationFeature.values())
                .map(Enum::name)
                .collect(Collectors.toSet());
        config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FILE_READER_JSON_DESERIALIZATION_CONFIGS))
                .forEach(entry -> {
                    String feature = entry.getKey().replaceAll(FILE_READER_JSON_DESERIALIZATION_CONFIGS, "");
                    if (deserializationFeatures.contains(feature)) {
                        mapper.configure(DeserializationFeature.valueOf(feature),
                                Boolean.parseBoolean(entry.getValue()));
                    } else {
                        log.warn("Ignoring deserialization configuration '" + feature + "' due to it does not exist.");
                    }
                });
    }

    @Override
    protected JsonRecord nextRecord() {
        try {
            JsonNode value = mapper.readTree(inner.nextRecord().getValue());
            return new JsonRecord(schema, value);
        } catch (JsonProcessingException jpe) {
            throw new IllegalStateException(jpe);
        }
    }

    @Override
    public boolean hasNext() {
        return inner.hasNext();
    }

    @Override
    public void seek(Offset offset) {
        inner.seek(offset);
    }

    @Override
    public Offset currentOffset() {
        return inner.currentOffset();
    }

    @Override
    public void close() throws IOException {
        inner.close();
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
                } else if (jsonNode.isFloat()) {
                    return Schema.OPTIONAL_FLOAT32_SCHEMA;
                } else if (jsonNode.isDouble()) {
                    return Schema.OPTIONAL_FLOAT64_SCHEMA;
                } else if (jsonNode.isBigInteger()) {
                    return Schema.OPTIONAL_INT64_SCHEMA;
                } else if (jsonNode.isBigDecimal()) {
                    return Schema.OPTIONAL_FLOAT64_SCHEMA;
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
                        .findFirst().map(JsonFileReader::extractSchema)
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

    static class JsonToStruct implements ReaderAdapter<JsonRecord> {

        @Override
        public Struct apply(JsonRecord record) {
            return toStruct(record.schema, record.value);
        }

        private Struct toStruct(Schema schema, JsonNode jsonNode) {
            if (jsonNode.isNull()) return null;
            Struct struct = new Struct(schema);
            jsonNode.fields()
                    .forEachRemaining(field -> struct.put(field.getKey(),
                            mapValue(struct.schema().field(field.getKey()).schema(), field.getValue())));
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
                    } else if (value.isFloat()) {
                        return value.floatValue();
                    } else if (value.isDouble()) {
                        return value.doubleValue();
                    } else if (value.isBigInteger()) {
                        return value.bigIntegerValue();
                    } else {
                        return value.numberValue();
                    }
                case STRING:
                    return value.asText();
                case BINARY:
                    try {
                        return value.binaryValue();
                    } catch (IOException ioe) {
                        throw new IllegalStateException(ioe);
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
                            .map(elm -> mapValue(schema, elm))
                            .collect(Collectors.toList());
                case NULL:
                case MISSING:
                default:
                    return null;
            }
        }
    }

    static class JsonRecord {
        private final Schema schema;
        private final JsonNode value;

        JsonRecord(Schema schema, JsonNode value) {
            this.schema = schema;
            this.value = value;
        }
    }
}
