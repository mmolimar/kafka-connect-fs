package com.github.mmolimar.kafka.connect.fs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;
import java.util.Map;


public class FsSourceConnectorConfig extends AbstractConfig {

    public static final String FS_URIS = "fs.uris";
    private static final String FS_URIS_DOC = "Comma-separated URIs of the FS(s).";
    private static final String FS_URIS_DISPLAY = "File system URIs";

    public static final String TOPIC = "topic";
    private static final String TOPIC_DOC = "Topic to copy data to.";
    private static final String TOPIC_DISPLAY = "Topic";

    private static final String CONNECTOR_GROUP = "Connector";

    public FsSourceConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FsSourceConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        int order = 0;
        return new ConfigDef()
                .define(
                        FS_URIS,
                        Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        FS_URIS_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        FS_URIS_DISPLAY
                ).define(
                        TOPIC,
                        Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        TOPIC_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.LONG,
                        TOPIC_DISPLAY
                );
    }

    public List<String> getFsUris() {
        return this.getList(FS_URIS);
    }

    public String getTopic() {
        return this.getString(TOPIC);
    }
}
