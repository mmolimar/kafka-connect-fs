package com.github.mmolimar.kafka.connect.fs.policy.local;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.CronPolicy;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class CronPolicyTest extends LocalPolicyTestBase {

    @BeforeAll
    public static void setUp() throws IOException {
        directories = new ArrayList<Path>() {{
            add(new Path(fsUri.toString(), UUID.randomUUID().toString()));
            add(new Path(fsUri.toString(), UUID.randomUUID().toString()));
        }};
        for (Path dir : directories) {
            fs.mkdirs(dir);
        }

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
        taskConfig = new FsSourceTaskConfig(cfg);
    }

    @Test
    @Override
    public void execPolicyAlreadyEnded() throws IOException {
        policy.execute();
        policy.interrupt();
        assertTrue(policy.hasEnded());
        assertThrows(IllegalWorkerStateException.class, () -> policy.execute());
    }

    @Test
    public void invalidCronExpression() {
        Map<String, String> originals = taskConfig.originalsStrings();
        originals.put(CronPolicy.CRON_POLICY_EXPRESSION, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(ConfigException.class, () -> ReflectionUtils.makePolicy(
                (Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
    }

    @Test
    public void invalidEndDate() {
        Map<String, String> originals = taskConfig.originalsStrings();
        originals.put(CronPolicy.CRON_POLICY_END_DATE, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(ConfigException.class, () -> ReflectionUtils.makePolicy(
                (Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
    }

    @Test
    public void canBeInterrupted() throws Throwable {
        policy = ReflectionUtils.makePolicy(
                (Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), taskConfig);

        for (int i = 0; i < 5; i++) {
            assertFalse(policy.hasEnded());
            policy.execute();
        }
        policy.interrupt();
        assertTrue(policy.hasEnded());
    }
}
