package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public abstract class PolicyTestBase {

    protected static FileSystem fs;
    protected static Policy policy;
    protected static List<Path> directories;
    protected static FsSourceTaskConfig taskConfig;
    protected static URI fsUri;

    @AfterAll
    public static void tearDown() throws Exception {
        policy.close();
        fs.close();
    }

    @BeforeEach
    public void initPolicy() throws Throwable {
        policy = ReflectionUtils.makePolicy(
                (Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), taskConfig);
    }

    @AfterEach
    public void cleanDirs() throws IOException {
        for (Path dir : directories) {
            fs.delete(dir, true);
            fs.mkdirs(dir);
        }
        policy.close();
    }

    @Test
    public void invalidArgs() {
        assertThrows(IllegalArgumentException.class, () -> taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS)
                .getConstructor(taskConfig.getClass()).newInstance(null));
    }

    @Test
    public void invalidConfig() {
        assertThrows(ConfigException.class, () -> ReflectionUtils.makePolicy(
                (Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS),
                new FsSourceTaskConfig(new HashMap<>())));
    }

    @Test
    public void interruptPolicy() throws Throwable {
        policy.execute();
        policy.interrupt();
        assertTrue(policy.hasEnded());
    }

    @Test
    public void invalidDirectory() throws IOException {
        for (Path dir : directories) {
            fs.delete(dir, true);
        }
        try {
            assertThrows(FileNotFoundException.class, () -> policy.execute());
        } finally {
            for (Path dir : directories) {
                fs.mkdirs(dir);
            }
        }
    }

    @Test
    public void listEmptyDirectories() throws IOException {
        Iterator<FileMetadata> it = policy.execute();
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    public void oneFilePerFs() throws IOException, InterruptedException {
        for (Path dir : directories) {
            fs.createNewFile(new Path(dir, System.nanoTime() + ".txt"));
            //this file does not match the regexp
            fs.createNewFile(new Path(dir, System.nanoTime() + ".invalid"));
        }
        //we wait till FS has registered the files
        Thread.sleep(500);

        Iterator<FileMetadata> it = policy.execute();
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
    }

    @Test
    public void recursiveDirectory() throws IOException, InterruptedException {
        for (Path dir : directories) {
            Path tmpDir = new Path(dir, String.valueOf(System.nanoTime()));
            fs.mkdirs(tmpDir);
            fs.createNewFile(new Path(tmpDir, System.nanoTime() + ".txt"));
            //this file does not match the regexp
            fs.createNewFile(new Path(tmpDir, System.nanoTime() + ".invalid"));
        }
        //we wait till FS has registered the files
        Thread.sleep(500);

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

    @Test
    public void execPolicyAlreadyEnded() throws IOException {
        policy.execute();
        assertTrue(policy.hasEnded());
        assertThrows(IllegalWorkerStateException.class, () -> policy.execute());
    }

    @Test
    public void dynamicURIs() throws Throwable {
        Path dynamic = new Path(fsUri.toString(), "${G}/${yyyy}/${MM}/${W}");
        fs.create(dynamic);
        Map<String, String> originals = taskConfig.originalsStrings();
        originals.put(FsSourceTaskConfig.FS_URIS, dynamic.toString());
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        policy = ReflectionUtils.makePolicy(
                (Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
        assertEquals(1, policy.getURIs().size());

        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("G");
        StringBuilder uri = new StringBuilder(dateTime.format(formatter));
        uri.append("/");
        formatter = DateTimeFormatter.ofPattern("yyyy");
        uri.append(dateTime.format(formatter));
        uri.append("/");
        formatter = DateTimeFormatter.ofPattern("MM");
        uri.append(dateTime.format(formatter));
        uri.append("/");
        formatter = DateTimeFormatter.ofPattern("W");
        uri.append(dateTime.format(formatter));
        assertTrue(policy.getURIs().get(0).endsWith(uri.toString()));
    }

    @Test
    public void invalidDynamicURIs() throws Throwable {
        Path dynamic = new Path(fsUri.toString(), "${yyyy}/${MM}/${mmmmmmm}");
        fs.create(dynamic);
        Map<String, String> originals = taskConfig.originalsStrings();
        originals.put(FsSourceTaskConfig.FS_URIS, dynamic.toString());
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(IllegalArgumentException.class, () -> ReflectionUtils.makePolicy(
                (Class<? extends Policy>) taskConfig.getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
    }
}
