package com.github.mmolimar.kafka.connect.fs.task;

import com.github.mmolimar.kafka.connect.fs.FsSourceTask;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class FsSourceTaskTest {

    private FsSourceTask task;
    private Map<String, String> taskProps;

    @Before
    public void setup() throws IOException {
        task = new FsSourceTask();

        taskProps = new HashMap<String, String>() {{
            put(FsSourceTaskConfig.FS_URIS, String.join(",",
                    System.getProperty("java.io.tmpdir") + "/dir1",
                    System.getProperty("java.io.tmpdir") + "/dir2",
                    System.getProperty("java.io.tmpdir") + "/dir3"));
            put(FsSourceTaskConfig.POLICY_CLASS, SimplePolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
        }};
    }

    @Test(expected = ConnectException.class)
    public void nullProperties() {
        task.start(null);
    }

    @Test(expected = ConnectException.class)
    public void expectedFsUris() {
        Map<String, String> testProps = new HashMap<>(taskProps);
        testProps.remove(FsSourceTaskConfig.FS_URIS);
        task.start(testProps);
    }

    @Test(expected = ConnectException.class)
    public void expectedPolicyClass() {
        Map<String, String> testProps = new HashMap<>(taskProps);
        testProps.remove(FsSourceTaskConfig.POLICY_CLASS);
        task.start(testProps);
    }

    @Test(expected = ConnectException.class)
    public void invalidPolicyClass() {
        Map<String, String> testProps = new HashMap<>(taskProps);
        testProps.put(FsSourceTaskConfig.POLICY_CLASS, Object.class.getName());
        task.start(testProps);
    }

    @Test(expected = ConnectException.class)
    public void expectedReaderClass() {
        Map<String, String> testProps = new HashMap<>(taskProps);
        testProps.remove(FsSourceTaskConfig.FILE_READER_CLASS);
        task.start(testProps);
    }

    @Test(expected = ConnectException.class)
    public void invalidReaderClass() {
        Map<String, String> testProps = new HashMap<>(taskProps);
        testProps.put(FsSourceTaskConfig.FILE_READER_CLASS, Object.class.getName());
        task.start(testProps);
    }

    @Test
    public void minimunConfig() {
        task.start(taskProps);
        task.stop();
    }

    @Test
    public void pollWithoutStart() throws InterruptedException {
        assertNull(task.poll());
        task.stop();
    }

    @Test
    public void checkVersion() {
        assertNotNull(task.version());
        assertFalse("unknown".equalsIgnoreCase(task.version()));
    }

}