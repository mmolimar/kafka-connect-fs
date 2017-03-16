package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public abstract class PolicyTestBase {

    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected static FileSystem fs;
    protected static Policy policy;
    protected static List<Path> directories;
    protected static FsSourceTaskConfig taskConfig;
    protected static URI fsUri;

    @AfterClass
    public static void tearDown() throws Exception {
        policy.close();
        fs.close();
    }

    @Before
    public void initPolicy() throws Throwable {
        policy = ReflectionUtils.makePolicy((Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS),
                taskConfig);
    }

    @After
    public void cleanDirs() throws IOException {
        for (Path dir : directories) {
            fs.delete(dir, true);
            fs.mkdirs(dir);
        }
        policy.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidArgs() throws Exception {
        taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS).getConstructor(taskConfig.getClass()).newInstance(null);
    }

    @Test(expected = ConfigException.class)
    public void invalidConfig() throws Throwable {
        ReflectionUtils.makePolicy((Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS),
                new FsSourceTaskConfig(new HashedMap()));
    }

    @Test
    public void checkConfig() throws IOException {
        for (Path dir : directories) {
            FileSystem fs = FileSystem.get(dir.toUri(), new Configuration());
            assertEquals("test", fs.getConf().get("dfs.data.dir"));
            assertEquals("test", fs.getConf().get("fs.default.name"));
        }
        assertTrue(taskConfig.equals(policy.getConf()));
    }

    @Test
    public void interruptPolicy() throws Throwable {
        policy.execute();
        policy.interrupt();
        assertTrue(policy.hasEnded());
    }

    @Test(expected = FileNotFoundException.class)
    public void invalidDirectory() throws IOException {
        for (Path dir : directories) {
            fs.delete(dir, true);
        }
        try {
            policy.execute();
        } finally {
            for (Path dir : directories) {
                fs.mkdirs(dir);
            }
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void listEmptyDirectories() throws IOException {
        Iterator<FileMetadata> it = policy.execute();
        assertFalse(it.hasNext());
        it.next();
    }

    @Test
    public void oneFilePerFs() throws IOException {
        for (Path dir : directories) {
            fs.createNewFile(new Path(dir, String.valueOf(System.nanoTime() + ".txt")));
            //this file does not match the regexp
            fs.createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        Iterator<FileMetadata> it = policy.execute();
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
    }

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
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
    }

    @Test
    public void hasEnded() throws IOException {
        policy.execute();
        assertTrue(policy.hasEnded());
    }

    @Test(expected = IllegalWorkerStateException.class)
    public void execPolicyAlreadyEnded() throws IOException {
        policy.execute();
        assertTrue(policy.hasEnded());
        policy.execute();
    }

}
