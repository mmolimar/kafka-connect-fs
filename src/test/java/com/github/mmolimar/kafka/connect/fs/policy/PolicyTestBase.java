package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

abstract class PolicyTestBase {

    protected static List<PolicyFsTestConfig> TEST_FILE_SYSTEMS = Arrays.asList(
            new LocalFsConfig(),
            new HdfsFsConfig()
    );

    @BeforeAll
    public static void initFs() throws IOException {
        for (PolicyFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.initFs();
        }
    }

    @AfterAll
    public static void finishFs() throws IOException {
        for (PolicyFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.getPolicy().close();
            fsConfig.close();
        }
    }

    @BeforeEach
    public void initPolicy() {
        for (PolicyFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            FsSourceTaskConfig sourceTaskConfig = buildSourceTaskConfig(fsConfig.getDirectories());
            Policy policy = ReflectionUtils.makePolicy((Class<? extends Policy>) sourceTaskConfig
                    .getClass(FsSourceTaskConfig.POLICY_CLASS), sourceTaskConfig);
            fsConfig.setSourceTaskConfig(sourceTaskConfig);
            fsConfig.setPolicy(policy);
        }
    }

    @AfterEach
    public void cleanDirsAndClose() throws IOException {
        for (PolicyFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            for (Path dir : fsConfig.getDirectories()) {
                fsConfig.getFs().delete(dir, true);
                fsConfig.getFs().mkdirs(dir);
            }
            fsConfig.getPolicy().close();
        }
    }

    private static Stream<Arguments> fileSystemConfigProvider() {
        return TEST_FILE_SYSTEMS.stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidArgs(PolicyFsTestConfig fsConfig) {
        assertThrows(IllegalArgumentException.class, () -> fsConfig.getSourceTaskConfig()
                .getClass(FsSourceTaskConfig.POLICY_CLASS)
                .getConstructor(fsConfig.getSourceTaskConfig().getClass()).newInstance(null));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidConfig(PolicyFsTestConfig fsConfig) {
        assertThrows(ConfigException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                                .getClass(FsSourceTaskConfig.POLICY_CLASS),
                        new FsSourceTaskConfig(new HashMap<>())));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void interruptPolicy(PolicyFsTestConfig fsConfig) throws IOException {
        fsConfig.getPolicy().execute();
        fsConfig.getPolicy().interrupt();
        assertTrue(fsConfig.getPolicy().hasEnded());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidDirectory(PolicyFsTestConfig fsConfig) throws IOException {
        FileSystem fs = fsConfig.getFs();
        for (Path dir : fsConfig.getDirectories()) {
            fs.delete(dir, true);
        }
        try {
            assertThrows(FileNotFoundException.class, () -> fsConfig.getPolicy().execute());
        } finally {
            for (Path dir : fsConfig.getDirectories()) {
                fs.mkdirs(dir);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void listEmptyDirectories(PolicyFsTestConfig fsConfig) throws IOException {
        Iterator<FileMetadata> it = fsConfig.getPolicy().execute();
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void oneFilePerFs(PolicyFsTestConfig fsConfig) throws IOException, InterruptedException {
        FileSystem fs = fsConfig.getFs();
        for (Path dir : fsConfig.getDirectories()) {
            fs.createNewFile(new Path(dir, System.nanoTime() + ".txt"));
            //this file does not match the regexp
            fs.createNewFile(new Path(dir, System.nanoTime() + ".invalid"));

            //we wait till FS has registered the files
            Thread.sleep(3000);
        }
        Iterator<FileMetadata> it = fsConfig.getPolicy().execute();
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void recursiveDirectory(PolicyFsTestConfig fsConfig) throws IOException, InterruptedException {
        FileSystem fs = fsConfig.getFs();
        for (Path dir : fsConfig.getDirectories()) {
            Path tmpDir = new Path(dir, String.valueOf(System.nanoTime()));
            fs.mkdirs(tmpDir);
            fs.createNewFile(new Path(tmpDir, System.nanoTime() + ".txt"));
            //this file does not match the regexp
            fs.createNewFile(new Path(tmpDir, System.nanoTime() + ".invalid"));

            //we wait till FS has registered the files
            Thread.sleep(3000);
        }
        Iterator<FileMetadata> it = fsConfig.getPolicy().execute();
        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.next();
        assertFalse(it.hasNext());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void execPolicyAlreadyEnded(PolicyFsTestConfig fsConfig) throws IOException {
        fsConfig.getPolicy().execute();
        assertTrue(fsConfig.getPolicy().hasEnded());
        assertThrows(IllegalWorkerStateException.class, () -> fsConfig.getPolicy().execute());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void dynamicURIs(PolicyFsTestConfig fsConfig) throws IOException {
        Path dynamic = new Path(fsConfig.getFsUri().toString(), "${G}/${yyyy}/${MM}/${W}");
        fsConfig.getFs().create(dynamic);
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(FsSourceTaskConfig.FS_URIS, dynamic.toString());
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        Policy policy = ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg);
        fsConfig.setPolicy(policy);
        assertEquals(1, fsConfig.getPolicy().getURIs().size());

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
        assertTrue(fsConfig.getPolicy().getURIs().get(0).endsWith(uri.toString()));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidDynamicURIs(PolicyFsTestConfig fsConfig) throws IOException {
        Path dynamic = new Path(fsConfig.getFsUri().toString(), "${yyyy}/${MM}/${mmmmmmm}");
        fsConfig.getFs().create(dynamic);
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(FsSourceTaskConfig.FS_URIS, dynamic.toString());
        FsSourceTaskConfig cfg = new FsSourceTaskConfig(originals);
        assertThrows(ConnectException.class, () ->
                ReflectionUtils.makePolicy((Class<? extends Policy>) fsConfig.getSourceTaskConfig()
                        .getClass(FsSourceTaskConfig.POLICY_CLASS), cfg));
        assertThrows(IllegalArgumentException.class, () -> {
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
    public void execPolicyBatchesFiles(PolicyFsTestConfig fsConfig) throws IOException, InterruptedException {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(FsSourceTaskConfig.POLICY_BATCH_SIZE, "1");
        FsSourceTaskConfig sourceTaskConfig = new FsSourceTaskConfig(originals);

        Policy policy = ReflectionUtils.makePolicy(
                (Class<? extends Policy>) fsConfig.getSourceTaskConfig().getClass(FsSourceTaskConfig.POLICY_CLASS),
                sourceTaskConfig);

        fsConfig.setPolicy(policy);

        FileSystem fs = fsConfig.getFs();
        for (Path dir : fsConfig.getDirectories()) {
            fs.createNewFile(new Path(dir, System.nanoTime() + ".txt"));
            //this file does not match the regexp
            fs.createNewFile(new Path(dir, System.nanoTime() + ".invalid"));

            //we wait till FS has registered the files
            Thread.sleep(3000);
        }
        

        Iterator<FileMetadata> it = fsConfig.getPolicy().execute();

        // First batch of files (1 file)
        assertTrue(it.hasNext());
        String firstPath = it.next().getPath();

        assertFalse(it.hasNext());

        // Second batch of files (1 file)
        it = fsConfig.getPolicy().execute();
        assertTrue(it.hasNext());

        assertNotEquals(firstPath, it.next().getPath());

        assertFalse(it.hasNext());
        
        // Second batch of files (1 file)
        it = fsConfig.getPolicy().execute();
        assertFalse(it.hasNext());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidBatchSize(PolicyFsTestConfig fsConfig) {
        Map<String, String> originals = fsConfig.getSourceTaskConfig().originalsStrings();
        originals.put(FsSourceTaskConfig.POLICY_BATCH_SIZE, "one");
        assertThrows(ConfigException.class, () -> {
            new FsSourceTaskConfig(originals);
        });

    }

    protected abstract FsSourceTaskConfig buildSourceTaskConfig(List<Path> directories);

}
