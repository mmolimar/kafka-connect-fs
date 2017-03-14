package com.github.mmolimar.kafka.connect.fs.policy.hdfs;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.policy.SleepyPolicy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SleepyPolicyTest extends HdfsPolicyTestBase {

    @BeforeClass
    public static void setUp() throws IOException {
        directories = new ArrayList<Path>() {{
            add(new Path(fsUri + String.valueOf(System.nanoTime())));
            add(new Path(fsUri + String.valueOf(System.nanoTime())));
        }};
        for (Path dir : directories) {
            fs.mkdirs(dir);
        }

        Map<String, String> cfg = new HashMap<String, String>() {{
            String uris[] = directories.stream().map(dir -> dir.toString())
                    .toArray(size -> new String[size]);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, SleepyPolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.FILE_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "test");
            put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "100");
            put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "1");
        }};
        taskConfig = new FsSourceTaskConfig(cfg);
    }

    @Test(expected = ConfigException.class)
    public void invalidSleepTime() throws Throwable {
        Map<String, String> originals = taskConfig.originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        ReflectionUtils.makePolicy((Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
    }

    @Test(expected = ConfigException.class)
    public void invalidMaxExecs() throws Throwable {
        Map<String, String> originals = taskConfig.originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        ReflectionUtils.makePolicy((Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
    }

    @Test(expected = ConfigException.class)
    public void invalidSleepFraction() throws Throwable {
        Map<String, String> originals = taskConfig.originalsStrings();
        originals.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_FRACTION_MS, "invalid");
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        ReflectionUtils.makePolicy((Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
    }

    @Test
    public void sleepExecution() throws Throwable {
        Map<String, String> tConfig = taskConfig.originalsStrings();
        tConfig.put(SleepyPolicy.SLEEPY_POLICY_SLEEP_MS, "1000");
        tConfig.put(SleepyPolicy.SLEEPY_POLICY_MAX_EXECS, "2");
        FsSourceTaskConfig sleepConfig = new FsSourceTaskConfig(tConfig);

        policy = ReflectionUtils.makePolicy((Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS),
                sleepConfig);
        assertFalse(policy.hasEnded());
        policy.execute();
        assertFalse(policy.hasEnded());
        policy.execute();
        assertTrue(policy.hasEnded());
    }

}
