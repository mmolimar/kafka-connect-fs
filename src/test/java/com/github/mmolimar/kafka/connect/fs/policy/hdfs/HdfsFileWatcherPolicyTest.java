package com.github.mmolimar.kafka.connect.fs.policy.hdfs;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.HdfsFileWatcherPolicy;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HdfsFileWatcherPolicyTest extends HdfsPolicyTestBase {

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
            put(FsSourceTaskConfig.POLICY_CLASS, HdfsFileWatcherPolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.FILE_REGEXP, "^[0-9]*\\.txt$");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "dfs.data.dir", "test");
            put(FsSourceTaskConfig.POLICY_PREFIX_FS + "fs.default.name", "test");
        }};
        taskConfig = new FsSourceTaskConfig(cfg);
    }

    //This does not throw any exception. Just stop watching those nonexistent dirs
    @Test
    @Override
    public void invalidDirectory() throws IOException {
        super.invalidDirectory();
    }

    //This policy never ends at least all watchers die
    @Test
    @Override
    public void hasEnded() throws IOException {
        policy.execute();
        assertFalse(policy.hasEnded());
        policy.interrupt();
        assertTrue(policy.hasEnded());
    }

    //This policy never ends. We have to interrupt it")
    @Test(expected = IllegalWorkerStateException.class)
    @Override
    public void execPolicyAlreadyEnded() throws IOException {
        policy.execute();
        assertFalse(policy.hasEnded());
        policy.interrupt();
        assertTrue(policy.hasEnded());
        policy.execute();
    }

    //TODO
    //@Ignore(value = "Needs synchronization. Sometimes fails")
    @Test
    public void oneFilePerFs() throws IOException {
        for (Path dir : directories) {
            fs.createNewFile(new Path(dir, String.valueOf(System.nanoTime() + ".txt")));
            //this file does not match the regexp
            fs.createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        Iterator<FileMetadata> it = policy.execute();
    }

    //TODO
    //@Ignore(value = "Needs synchronization. Sometimes fails")
    @Test
    public void recursiveDirectory() throws IOException {
        for (Path dir : directories) {
            Path tmpDir = new Path(dir, String.valueOf(System.nanoTime()));
            fs.mkdirs(tmpDir);
            fs.createNewFile(new Path(tmpDir, String.valueOf(System.nanoTime() + ".txt")));
            //this file does not match the regexp
            fs.createNewFile(new Path(tmpDir, String.valueOf(System.nanoTime())));
        }

        Iterator<FileMetadata> it = policy.execute();
    }

}
