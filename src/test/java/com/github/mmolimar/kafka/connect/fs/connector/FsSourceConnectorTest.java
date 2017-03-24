package com.github.mmolimar.kafka.connect.fs.connector;

import com.github.mmolimar.kafka.connect.fs.FsSourceConnector;
import com.github.mmolimar.kafka.connect.fs.FsSourceTask;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class FsSourceConnectorTest {
    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private FsSourceConnector connector;
    private Map<String, String> connProps;

    @Before
    public void setup() throws IOException {
        connector = new FsSourceConnector();

        Map<String, String> cfg = new HashMap<String, String>() {{
            put(FsSourceTaskConfig.FS_URIS, String.join(",",
                    temporaryFolder.getRoot().toURI() + File.separator + "dir1",
                    temporaryFolder.getRoot().toURI() + File.separator + "dir2",
                    temporaryFolder.getRoot().toURI() + File.separator + "dir3"));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
        }};
        connProps = new HashMap<>(cfg);
    }

    @Test(expected = ConnectException.class)
    public void nullProperties() {
        connector.start(null);
    }

    @Test(expected = ConnectException.class)
    public void expectedFsUris() {
        Map<String, String> testProps = new HashMap<>(connProps);
        testProps.remove(FsSourceTaskConfig.FS_URIS);
        connector.start(testProps);
    }

    @Test
    public void minimunConfig() {
        connector.start(connProps);
        connector.stop();
    }

    @Test
    public void checkTaskClass() {
        assertEquals(FsSourceTask.class, connector.taskClass());
    }

    @Test(expected = ConnectException.class)
    public void configTasksWithoutStart() {
        connector.taskConfigs(1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidConfigTaskNumber() {
        connector.start(connProps);
        connector.taskConfigs(0);
    }

    @Test
    public void configTasks() {
        connector.start(connProps);
        int uris = connProps.get(FsSourceTaskConfig.FS_URIS).split(",").length;
        IntStream.range(1, connProps.get(FsSourceTaskConfig.FS_URIS).split(",").length + 1)
                .forEach(index -> {
                    List<Map<String, String>> taskConfigs = connector.taskConfigs(index);
                    assertTrue(taskConfigs.size() == (index > uris ? uris : index));
                });
        connector.stop();
    }

    @Test
    public void checkVersion() {
        assertNotNull(connector.version());
        assertFalse("unknown".equalsIgnoreCase(connector.version()));
    }

    @Test
    public void checkDefaultConf() {
        assertNotNull(connector.config());
    }

}