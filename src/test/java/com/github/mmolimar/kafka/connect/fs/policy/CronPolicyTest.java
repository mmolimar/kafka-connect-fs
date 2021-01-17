package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class CronPolicyTest extends PolicyTestBase {

    @Override
    protected FsSourceTaskConfig buildSourceTaskConfig(List<Path> directories) {
        Map<String, String> cfg = new HashMap<String, String>() {{
            String[] uris = directories.stream().map(Path::toString)
                    .toArray(String[]::new);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, CronPolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "hdfs://test");
            put(CronPolicy.CRON_POLICY_EXPRESSION, "0/2 * * * * ?");
            put(CronPolicy.CRON_POLICY_END_DATE, LocalDateTime.now().plusDays(1).toString());
        }};
        return new FsSourceTaskConfig(cfg);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @Override
    public void execPolicyAlreadyEnded(PolicyFsTestConfig fsConfig) throws IOException {
        fsConfig.getPolicy().execute();
        fsConfig.getPolicy().interrupt();
        assertTrue(fsConfig.getPolicy().hasEnded());
        assertThrows(IllegalWorkerStateException.class, () -> fsConfig.getPolicy().execute());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void invalidCronExpression(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(CronPolicy.CRON_POLICY_EXPRESSION, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(ConnectException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
        assertThrows(ConfigException.class, () -> {
            try {
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void invalidEndDate(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(CronPolicy.CRON_POLICY_END_DATE, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(ConnectException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
        assertThrows(ConfigException.class, () -> {
            try {
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
            } catch (Exception e) {
                throw e.getCause();
            }
        });
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    @SuppressWarnings("unchecked")
    public void canBeInterrupted(PolicyFsTestConfig fsConfig) throws IOException {
        try (Policy policy = ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS),
                fsConfig.getSourceTaskConfig())) {

            for (int i = 0; i < 5; i++) {
                assertFalse(policy.hasEnded());
                policy.execute();
            }
            policy.interrupt();
            assertTrue(policy.hasEnded());
        }
    }
}
