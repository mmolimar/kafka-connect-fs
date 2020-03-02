package com.github.mmolimar.kafka.connect.fs.task;

import com.github.mmolimar.kafka.connect.fs.FsSourceTask;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class FsSourceTaskTest {
    @TempDir
    public static File temporaryFolder;

    private FsSourceTask task;
    private Map<String, String> taskConfig;

    @BeforeEach
    public void setup() {
        task = new FsSourceTask();

        taskConfig = new HashMap<String, String>() {{
            put(FsSourceTaskConfig.FS_URIS, String.join(",",
                    temporaryFolder.toURI() + File.separator + "dir1",
                    temporaryFolder.toURI() + File.separator + "dir2",
                    temporaryFolder.toURI() + File.separator + "dir3"));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, SimplePolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
        }};
    }

    @Test
    public void nullProperties() {
        assertThrows(ConnectException.class, () -> task.start(null));
    }

    @Test
    public void expectedFsUris() {
        Map<String, String> testProps = new HashMap<>(taskConfig);
        testProps.remove(FsSourceTaskConfig.FS_URIS);
        assertThrows(ConnectException.class, () -> task.start(testProps));
    }

    @Test
    public void expectedPolicyClass() {
        Map<String, String> testProps = new HashMap<>(taskConfig);
        testProps.remove(FsSourceTaskConfig.POLICY_CLASS);
        assertThrows(ConnectException.class, () -> task.start(testProps));
    }

    @Test
    public void invalidPolicyClass() {
        Map<String, String> testProps = new HashMap<>(taskConfig);
        testProps.put(FsSourceTaskConfig.POLICY_CLASS, Object.class.getName());
        assertThrows(ConnectException.class, () -> task.start(testProps));
    }

    @Test
    public void expectedReaderClass() {
        Map<String, String> testProps = new HashMap<>(taskConfig);
        testProps.remove(FsSourceTaskConfig.FILE_READER_CLASS);
        assertThrows(ConnectException.class, () -> task.start(testProps));
    }

    @Test
    public void invalidReaderClass() {
        Map<String, String> testProps = new HashMap<>(taskConfig);
        testProps.put(FsSourceTaskConfig.FILE_READER_CLASS, Object.class.getName());
        assertThrows(ConnectException.class, () -> task.start(testProps));
    }

    @Test
    public void minimumConfig() {
        task.start(taskConfig);
        task.stop();
    }

    @Test
    public void pollWithoutStart() {
        assertNull(task.poll());
        task.stop();
    }

    @Test
    public void checkVersion() {
        assertNotNull(task.version());
        assertFalse("unknown".equalsIgnoreCase(task.version()));
    }
}
