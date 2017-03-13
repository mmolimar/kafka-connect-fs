package com.github.mmolimar.kafka.connect.fs.task;

import com.github.mmolimar.kafka.connect.fs.FsSourceConnectorConfig;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class FsSourceTaskConfigTest {

    @Test
    public void checkDocumentation() {
        ConfigDef config = FsSourceTaskConfig.conf();
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