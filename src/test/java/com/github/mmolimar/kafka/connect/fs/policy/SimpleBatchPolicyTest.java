package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleBatchPolicyTest extends PolicyTestBase {

    @Override
    protected FsSourceTaskConfig buildSourceTaskConfig(List<Path> directories) {
        return new FsSourceTaskConfig(buildConfigMap(directories));
    }

    private Map<String, String> buildConfigMap(List<Path> directories) {
        return new HashMap<String, String>() {
            {
                String[] uris = directories.stream().map(Path::toString).toArray(String[]::new);
                put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
                put(FsSourceTaskConfig.TOPIC, "topic_test");
                put(FsSourceTaskConfig.POLICY_CLASS, SimpleBatchPolicy.class.getName());
                put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
                put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
                put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
                put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "hdfs://test/");
            }
        };
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void execPolicyBatchesFiles(PolicyFsTestConfig fsConfig) throws IOException {
        Map<String, String> configMap = buildConfigMap(fsConfig.getDirectories());
        configMap.put(SimpleBatchPolicy.BATCH_POLICY_BATCH_SIZE, "1");
        FsSourceTaskConfig sourceTaskConfig = new FsSourceTaskConfig(configMap);

        Path dir = fsConfig.getDirectories().get(0);

        fsConfig.getFs().createNewFile(new Path(dir, System.nanoTime() + ".txt"));
        // this file does not match the regexp
        fsConfig.getFs().createNewFile(new Path(dir, System.nanoTime() + ".txt"));

        Policy policy = ReflectionUtils.makePolicy(
                (Class<? extends Policy>) fsConfig.getSourceTaskConfig().getClass(FsSourceTaskConfig.POLICY_CLASS),
                sourceTaskConfig);
        fsConfig.setPolicy(policy);

        Iterator<FileMetadata> it = fsConfig.getPolicy().execute();

        // First batch of files (1 file)
        assertFalse(fsConfig.getPolicy().hasEnded());
        assertTrue(it.hasNext());
        String firstPath = it.next().getPath();

        assertFalse(it.hasNext());
        assertFalse(fsConfig.getPolicy().hasEnded());

        // Second batch of files (1 file)
        it = fsConfig.getPolicy().execute();
        assertTrue(it.hasNext());

        assertNotEquals(firstPath, it.next().getPath());

        assertFalse(it.hasNext());
        assertTrue(fsConfig.getPolicy().hasEnded());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidBatchSize(PolicyFsTestConfig fsConfig) {
        Map<String, String> configMap = buildConfigMap(fsConfig.getDirectories());
        configMap.put(SimpleBatchPolicy.BATCH_POLICY_BATCH_SIZE, "one");
        FsSourceTaskConfig sourceTaskConfig = new FsSourceTaskConfig(configMap);
        assertThrows(ConfigException.class, () -> {
            try {
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), sourceTaskConfig);
            } catch (Exception e) {
                throw e.getCause();
            }
        });

    }
}
