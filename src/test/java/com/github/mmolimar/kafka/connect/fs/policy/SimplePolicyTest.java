package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimplePolicyTest extends PolicyTestBase {

    @Override
    protected FsSourceTaskConfig buildSourceTaskConfig(List<Path> directories) {
        Map<String, String> cfg = new HashMap<String, String>() {{
            String[] uris = directories.stream().map(Path::toString)
                    .toArray(String[]::new);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, SimplePolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "hdfs://test/");
        }};
        return new FsSourceTaskConfig(cfg);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void execPolicyEndsAfterBatching(PolicyFsTestConfig fsConfig) throws IOException, InterruptedException {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(FsSourceTaskConfig.POLICY_BATCH_SIZE, "1");
        FsSourceTaskConfig sourceTaskConfig = new FsSourceTaskConfig(originals);

        try (Policy policy = ReflectionUtils.makePolicy(
                (Class<? extends Policy>) fsConfig.getSourceTaskConfig().getClass(FsSourceTaskConfig.POLICY_CLASS),
                sourceTaskConfig)) {

            FileSystem fs = fsConfig.getFs();
            for (Path dir : fsConfig.getDirectories()) {
                fs.createNewFile(new Path(dir, System.nanoTime() + ".txt"));
                //this file does not match the regexp
                fs.createNewFile(new Path(dir, System.nanoTime() + ".invalid"));

                //we wait till FS has registered the files
                Thread.sleep(3000);
            }
            
            Iterator<FileMetadata> it = policy.execute();

            // First batch of files (1 file)
            assertFalse(policy.hasEnded());
            assertTrue(it.hasNext());
            String firstPath = it.next().getPath();
            assertFalse(it.hasNext());
            assertFalse(policy.hasEnded());

            // Second batch of files (1 file)
            it = policy.execute();
            assertTrue(it.hasNext());
            assertNotEquals(firstPath, it.next().getPath());
            assertFalse(it.hasNext());
            assertTrue(policy.hasEnded());
        }
    }

}
