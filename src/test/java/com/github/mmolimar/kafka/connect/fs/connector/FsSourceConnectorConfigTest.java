package com.github.mmolimar.kafka.connect.fs.connector;

import com.github.mmolimar.kafka.connect.fs.FsSourceConnectorConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class FsSourceConnectorConfigTest {

    @Test
    public void checkDocumentation() {
        ConfigDef config = FsSourceConnectorConfig.conf();
        config.names().forEach(key -> {
            assertFalse("Property " + key + " should be documented",
                    config.configKeys().get(key).documentation == null ||
                            "".equals(config.configKeys().get(key).documentation.trim()));
        });
    }

    @Test
    public void toRst() {
        assertNotNull(FsSourceConnectorConfig.conf().toRst());
    }
}