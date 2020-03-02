package com.github.mmolimar.kafka.connect.fs.connector;

import com.github.mmolimar.kafka.connect.fs.FsSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FsSourceConnectorConfigTest {

    @Test
    public void checkDocumentation() {
        ConfigDef config = FsSourceConnectorConfig.conf();
        config.names().forEach(key -> {
            assertFalse(config.configKeys().get(key).documentation == null ||
                            "".equals(config.configKeys().get(key).documentation.trim()),
                    () -> "Property " + key + " should be documented");
        });
    }

    @Test
    public void toRst() {
        assertNotNull(FsSourceConnectorConfig.conf().toRst());
    }
}
