package com.github.mmolimar.kafka.connect.fs.connector;

import com.github.mmolimar.kafka.connect.fs.FsSourceConnector;
import com.github.mmolimar.kafka.connect.fs.FsSourceTask;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class FsSourceConnectorTest {
    @TempDir
    public static File temporaryFolder;

    private FsSourceConnector connector;
    private Map<String, String> connProps;

    @BeforeEach
    public void setup() {
        connector = new FsSourceConnector();

        Map<String, String> cfg = new HashMap<String, String>() {{
            put(FsSourceTaskConfig.FS_URIS, String.join(",",
                    temporaryFolder.toURI() + File.separator + "dir1",
                    temporaryFolder.toURI() + File.separator + "dir2",
                    temporaryFolder.toURI() + File.separator + "dir3"));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
        }};
        connProps = new HashMap<>(cfg);
    }

    @Test
    public void nullProperties() {
        assertThrows(ConnectException.class, () -> connector.start(null));
    }

    @Test
    public void expectedFsUris() {
        Map<String, String> testProps = new HashMap<>(connProps);
        testProps.remove(FsSourceTaskConfig.FS_URIS);
        assertThrows(ConnectException.class, () -> connector.start(testProps));
    }

    @Test
    public void minimumConfig() {
        connector.start(connProps);
        connector.stop();
    }

    @Test
    public void checkTaskClass() {
        assertEquals(FsSourceTask.class, connector.taskClass());
    }

    @Test
    public void configTasksWithoutStart() {
        assertThrows(ConnectException.class, () -> connector.taskConfigs(1));
    }

    @Test
    public void invalidConfigTaskNumber() {
        connector.start(connProps);
        assertThrows(IllegalArgumentException.class, () -> connector.taskConfigs(0));
    }

    @Test
    public void configTasks() {
        connector.start(connProps);
        int uris = connProps.get(FsSourceTaskConfig.FS_URIS).split(",").length;
        IntStream.range(1, connProps.get(FsSourceTaskConfig.FS_URIS).split(",").length + 1)
                .forEach(index -> {
                    List<Map<String, String>> taskConfigs = connector.taskConfigs(index);
                    assertEquals(taskConfigs.size(), Math.min(index, uris));
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
