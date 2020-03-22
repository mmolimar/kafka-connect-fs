package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class HdfsFileWatcherPolicyTest extends PolicyTestBase {

    static {
        TEST_FILE_SYSTEMS = Collections.singletonList(
                new HdfsFsConfig()
        );
    }

    @BeforeAll
    public static void initFs() throws IOException {
        for (PolicyFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.initFs();
        }
    }

    @Override
    protected FsSourceTaskConfig buildSourceTaskConfig(List<Path> directories) {
        Map<String, String> cfg = new HashMap<String, String>() {{
            String[] uris = directories.stream().map(Path::toString)
                    .toArray(String[]::new);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, HdfsFileWatcherPolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "hdfs://test");
        }};
        return new FsSourceTaskConfig(cfg);
    }

    //This policy does not throw any exception. Just stop watching those nonexistent dirs
    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Override
    public void invalidDirectory(PolicyFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            fsConfig.getFs().delete(dir, true);
        }
        try {
            fsConfig.getPolicy().execute();
        } finally {
            for (Path dir : fsConfig.getDirectories()) {
                fsConfig.getFs().mkdirs(dir);
            }
        }
    }

    //This policy never ends. We have to interrupt it
    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Override
    public void execPolicyAlreadyEnded(PolicyFsTestConfig fsConfig) throws IOException {
        fsConfig.getPolicy().execute();
        assertFalse(fsConfig.getPolicy().hasEnded());
        fsConfig.getPolicy().interrupt();
        assertTrue(fsConfig.getPolicy().hasEnded());
        assertThrows(IllegalWorkerStateException.class, () -> fsConfig.getPolicy().execute());
    }

}
