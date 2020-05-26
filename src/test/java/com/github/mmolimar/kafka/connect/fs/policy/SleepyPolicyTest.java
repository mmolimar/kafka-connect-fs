package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SleepyPolicyTest extends PolicyTestBase {

    @Override
    protected FsSourceTaskConfig buildSourceTaskConfig(List<Path> directories) {
        Map<String, String> cfg = new HashMap<String, String>() {{
            String[] uris = directories.stream().map(Path::toString)
                    .toArray(String[]::new);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, SleepyPolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "hdfs://test");
            put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "100");
            put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "1");
        }};
        return new FsSourceTaskConfig(cfg);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidSleepTime(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "invalid");
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
    public void invalidMaxExecs(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "invalid");
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
    public void invalidSleepFraction(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_FRACTION, "invalid");
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
    public void sleepExecution(PolicyFsTestConfig fsConfig) throws IOException {
        Map<String, String> tConfig = fsConfig.getSourceTaskConfig().originalsStrings();
        tConfig.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "1000");
        tConfig.put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "2");
        FsSourceTaskConfig sleepConfig = new FsSourceTaskConfig(tConfig);

        try (Policy policy = ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                .getClass(FsSourceTaskConfig.POLICY_CLASS), sleepConfig)) {
            assertFalse(policy.hasEnded());
            policy.execute();
            assertFalse(policy.hasEnded());
            policy.execute();
            assertTrue(policy.hasEnded());
        }
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void defaultExecutions(PolicyFsTestConfig fsConfig) throws IOException {
        Map<String, String> tConfig = fsConfig.getSourceTaskConfig().originalsStrings();
        tConfig.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "1");
        tConfig.remove(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS);
        FsSourceTaskConfig sleepConfig = new FsSourceTaskConfig(tConfig);

        try (Policy policy = ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                .getClass(FsSourceTaskConfig.POLICY_CLASS), sleepConfig)) {

            //it never ends
            for (int i = 0; i < 100; i++) {
                assertFalse(policy.hasEnded());
                policy.execute();
            }
            policy.interrupt();
            assertTrue(policy.hasEnded());
        }
    }

}
