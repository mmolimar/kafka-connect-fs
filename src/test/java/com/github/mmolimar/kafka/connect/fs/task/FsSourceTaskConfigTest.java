package com.github.mmolimar.kafka.connect.fs.task;

import com.github.mmolimar.kafka.connect.fs.FsSourceConnectorConfig;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class FsSourceTaskConfigTest {

    @Test
    public void checkDocumentation() {
        ConfigDef config = FsSourceTaskConfig.conf();
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
