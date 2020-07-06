package com.github.mmolimar.kafka.connect.fs.task;

import com.github.mmolimar.kafka.connect.fs.FsSourceTask;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.AvroFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.CronPolicy;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.membermodification.MemberModifier;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class FsSourceTaskTest {

    private static final List<TaskFsTestConfig> TEST_FILE_SYSTEMS = Arrays.asList(
            new LocalFsConfig(),
            new HdfsFsConfig()
    );
    private static final int NUM_RECORDS = 10;
    private static final long NUM_BYTES_PER_FILE = 390;
    private static final String FILE_ALREADY_PROCESSED = "0101010101010101.txt";

    @BeforeAll
    public static void initFs() throws IOException {
        for (TaskFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.initFs();
        }
    }

    @AfterAll
    public static void finishFs() throws IOException {
        for (TaskFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.close();
        }
    }

    @BeforeEach
    public void initTask() {
        for (TaskFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            Map<String, String> taskConfig = new HashMap<String, String>() {{
                String[] uris = fsConfig.getDirectories().stream().map(Path::toString)
                        .toArray(String[]::new);
                put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
                put(FsSourceTaskConfig.TOPIC, "topic_test");
                put(FsSourceTaskConfig.POLICY_CLASS, SimplePolicy.class.getName());
                put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
                put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            }};

            // Mock initialization
            SourceTaskContext taskContext = PowerMock.createMock(SourceTaskContext.class);
            OffsetStorageReader offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);

            EasyMock.expect(taskContext.offsetStorageReader())
                    .andReturn(offsetStorageReader)
                    .times(2);

            // Every time the `offsetStorageReader.offsets(params)` method is called we want to capture the offsets params
            // and return a different result based on the offset params passed in
            // In this case, returning a different result based on the file path of the params
            Capture<Collection<Map<String, Object>>> captureOne = Capture.newInstance(CaptureType.ALL);
            AtomicInteger executionNumber = new AtomicInteger();
            EasyMock.expect(
                    offsetStorageReader.offsets(EasyMock.capture(captureOne))
            ).andAnswer(() -> {
                List<Collection<Map<String, Object>>> capturedValues = captureOne.getValues();
                Collection<Map<String, Object>> captured = capturedValues.get(executionNumber.get());
                executionNumber.addAndGet(1);

                Map<Map<String, Object>, Map<String, Object>> map = new HashMap<>();
                captured.forEach(part -> {
                    if (((String) (part.get("path"))).endsWith(FILE_ALREADY_PROCESSED)) {
                        map.put(part, new HashMap<String, Object>() {{
                            put("offset", (long) NUM_RECORDS);
                            put("eof", true);
                            put("file-size", NUM_BYTES_PER_FILE);
                        }});
                    } else {
                        map.put(part, new HashMap<String, Object>() {{
                            put("offset", (long) NUM_RECORDS / 2);
                        }});
                    }
                });
                return map;
            }).times(2);

            EasyMock.checkOrder(taskContext, false);
            EasyMock.replay(taskContext);

            EasyMock.checkOrder(offsetStorageReader, false);
            EasyMock.replay(offsetStorageReader);

            FsSourceTask task = new FsSourceTask();
            task.initialize(taskContext);

            fsConfig.setTaskConfig(taskConfig);
            fsConfig.setTask(task);
        }
    }

    @AfterEach
    public void cleanDirsAndStop() throws IOException {
        for (TaskFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            for (Path dir : fsConfig.getDirectories()) {
                fsConfig.getFs().delete(dir, true);
                fsConfig.getFs().mkdirs(dir);
            }
            fsConfig.getTask().stop();
        }
    }

    private static Stream<Arguments> fileSystemConfigProvider() {
        return TEST_FILE_SYSTEMS.stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void pollNoData(TaskFsTestConfig fsConfig) {
        fsConfig.getTask().start(fsConfig.getTaskConfig());
        assertEquals(0, fsConfig.getTask().poll().size());
        // policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFilesToProcess(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            fsConfig.getFs().createNewFile(new Path(dir, System.nanoTime() + ".txt"));
            // this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }
        fsConfig.getTask().start(fsConfig.getTaskConfig());
        assertEquals(0, fsConfig.getTask().poll().size());
        // policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void oneFilePerFs(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            Path dataFile = new Path(dir, System.nanoTime() + ".txt");
            createDataFile(fsConfig.getFs(), dataFile);
            // this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        fsConfig.getTask().start(fsConfig.getTaskConfig());
        List<SourceRecord> records = fsConfig.getTask().poll();
        assertEquals((NUM_RECORDS * fsConfig.getDirectories().size()) / 2, records.size());
        checkRecords(records);
        // policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void oneFilePerFsWithFileReaderInBatch(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            Path dataFile = new Path(dir, System.nanoTime() + ".txt");
            createDataFile(fsConfig.getFs(), dataFile);
            // this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        int readerBatchSize = 8;
        Map<String, String> taskConfig = fsConfig.getTaskConfig();
        taskConfig.put(FsSourceTaskConfig.POLICY_CLASS, CronPolicy.class.getName());
        taskConfig.put(CronPolicy.CRON_POLICY_EXPRESSION, "0/2 * * * * ?");
        taskConfig.put(CronPolicy.CRON_POLICY_END_DATE, LocalDateTime.now().plusDays(1).toString());
        taskConfig.put(FsSourceTaskConfig.FILE_READER_BATCH_SIZE, String.valueOf(readerBatchSize));
        taskConfig.put(FsSourceTaskConfig.POLL_INTERVAL_MS, "1000");
        fsConfig.getTask().start(taskConfig);

        List<SourceRecord> records = fsConfig.getTask().poll();
        assertEquals(((readerBatchSize % (NUM_RECORDS / 2)) * fsConfig.getDirectories().size()), records.size());
        checkRecords(records);

        records = fsConfig.getTask().poll();
        assertEquals(((readerBatchSize % (NUM_RECORDS / 2)) * fsConfig.getDirectories().size()), records.size());
        checkRecords(records);

        // policy has ended
        fsConfig.getTask().stop();
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void oneFilePerFsWithBatchAndFileReaderInBatch(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            Path dataFile = new Path(dir, System.nanoTime() + ".txt");
            createDataFile(fsConfig.getFs(), dataFile);
            // this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        int readerBatchSize = 8;
        Map<String, String> taskConfig = fsConfig.getTaskConfig();
        taskConfig.put(FsSourceTaskConfig.POLICY_CLASS, CronPolicy.class.getName());
        taskConfig.put(FsSourceTaskConfig.POLICY_BATCH_SIZE, "1");
        taskConfig.put(CronPolicy.CRON_POLICY_EXPRESSION, "0/2 * * * * ?");
        taskConfig.put(CronPolicy.CRON_POLICY_END_DATE, LocalDateTime.now().plusDays(1).toString());
        taskConfig.put(FsSourceTaskConfig.FILE_READER_BATCH_SIZE, String.valueOf(readerBatchSize));
        taskConfig.put(FsSourceTaskConfig.POLL_INTERVAL_MS, "1000");
        fsConfig.getTask().start(taskConfig);

        List<SourceRecord> firstBatch = fsConfig.getTask().poll();
        assertEquals(readerBatchSize % (NUM_RECORDS / 2), firstBatch.size());
        checkRecords(firstBatch);

        List<SourceRecord> secondBatch = fsConfig.getTask().poll();
        assertEquals(readerBatchSize % (NUM_RECORDS / 2), secondBatch.size());
        checkRecords(secondBatch);
        assertEquals(firstBatch.size(), secondBatch.size());
        IntStream.range(0, firstBatch.size())
                .forEach(index -> assertNotEquals(
                        firstBatch.get(index).sourcePartition().get("path"),
                        secondBatch.get(index).sourcePartition().get("path")
                ));

        // policy has ended
        fsConfig.getTask().stop();
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void skipsFetchingFileIfByteOffsetExistsAndMatchesFileLength(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            // this file will be skipped since the byte offset for the file is equal to the byte size of the file
            Path dataFile = new Path(dir, FILE_ALREADY_PROCESSED);
            createDataFile(fsConfig.getFs(), dataFile);
        }

        fsConfig.getTask().start(fsConfig.getTaskConfig());
        List<SourceRecord> records = fsConfig.getTask().poll();
        assertEquals(0, records.size());
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void nonExistentUri(TaskFsTestConfig fsConfig) {
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        props.put(FsSourceTaskConfig.FS_URIS,
                new Path(fsConfig.getFs().getWorkingDirectory(), UUID.randomUUID().toString()).toString());
        fsConfig.getTask().start(props);
        fsConfig.getTask().poll();
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void exceptionExecutingPolicy(TaskFsTestConfig fsConfig) throws IOException, IllegalAccessException {
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        fsConfig.getTask().start(props);

        Policy policy = EasyMock.createNiceMock(Policy.class);
        EasyMock.expect(policy.hasEnded()).andReturn(Boolean.FALSE);
        EasyMock.expect(policy.execute()).andThrow(new ConnectException("Exception from mock"));
        EasyMock.expect(policy.getURIs()).andReturn(null);
        EasyMock.checkOrder(policy, false);
        EasyMock.replay(policy);
        MemberModifier.field(FsSourceTask.class, "policy").set(fsConfig.getTask(), policy);

        assertEquals(0, fsConfig.getTask().poll().size());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void exceptionReadingFile(TaskFsTestConfig fsConfig) throws IOException {
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        File tmp = File.createTempFile("test-", ".txt");
        try (PrintWriter writer = new PrintWriter(tmp)) {
            writer.append("txt");
        }
        Path dest = new Path(fsConfig.getDirectories().get(0).toString(), System.nanoTime() + ".txt");
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), dest);
        props.put(FsSourceTaskConfig.FILE_READER_CLASS, AvroFileReader.class.getName());
        fsConfig.getTask().start(props);
        assertEquals(0, fsConfig.getTask().poll().size());
        fsConfig.getTask().stop();

        fsConfig.getFs().delete(dest, false);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void nullProperties(TaskFsTestConfig fsConfig) {
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(null));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void expectedFsUris(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.remove(FsSourceTaskConfig.FS_URIS);
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void expectedPolicyClass(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.remove(FsSourceTaskConfig.POLICY_CLASS);
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidPolicyClass(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.put(FsSourceTaskConfig.POLICY_CLASS, Object.class.getName());
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void expectedReaderClass(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.remove(FsSourceTaskConfig.FILE_READER_CLASS);
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidReaderClass(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.put(FsSourceTaskConfig.FILE_READER_CLASS, Object.class.getName());
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void minimumConfig(TaskFsTestConfig fsConfig) {
        fsConfig.getTask().start(fsConfig.getTaskConfig());
        fsConfig.getTask().stop();
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void pollWithoutStart(TaskFsTestConfig fsConfig) {
        assertNull(fsConfig.getTask().poll());
        fsConfig.getTask().stop();
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void checkVersion(TaskFsTestConfig fsConfig) {
        assertNotNull(fsConfig.getTask().version());
        assertFalse("unknown".equalsIgnoreCase(fsConfig.getTask().version()));
    }


    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void pollNoDataWithBatch(TaskFsTestConfig fsConfig) {
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        props.put(FsSourceTaskConfig.POLICY_BATCH_SIZE, "1");
        fsConfig.getTask().start(props);

        assertEquals(0, fsConfig.getTask().poll().size());
        // policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFilesToProcessWithBatch(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            fsConfig.getFs().createNewFile(new Path(dir, System.nanoTime() + ".txt"));
            // this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        props.put(FsSourceTaskConfig.POLICY_BATCH_SIZE, "1");
        fsConfig.getTask().start(props);

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> fresh = fsConfig.getTask().poll();
        while (fresh != null) {
            records.addAll(fresh);
            fresh = fsConfig.getTask().poll();
        }
        assertEquals(0, records.size());

        // policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void oneFilePerFsWithBatch(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            Path dataFile = new Path(dir, System.nanoTime() + ".txt");
            createDataFile(fsConfig.getFs(), dataFile);
            // this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        props.put(FsSourceTaskConfig.POLICY_BATCH_SIZE, "1");
        fsConfig.getTask().start(props);

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> fresh = fsConfig.getTask().poll();
        while (fresh != null) {
            records.addAll(fresh);
            fresh = fsConfig.getTask().poll();
        }

        assertEquals((NUM_RECORDS * fsConfig.getDirectories().size()) / 2, records.size());
        checkRecords(records);
        // policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void shouldNotSleepBetweenBatches(TaskFsTestConfig fsConfig) throws IOException {
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        props.put(FsSourceTaskConfig.POLL_INTERVAL_MS, "10000");
        props.put(FsSourceTaskConfig.POLICY_BATCH_SIZE, "1");

        for (Path dir : fsConfig.getDirectories()) {
            Path dataFile = new Path(dir, System.nanoTime() + ".txt");
            createDataFile(fsConfig.getFs(), dataFile);
            // this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        fsConfig.getTask().start(props);

        List<SourceRecord> records = new ArrayList<>();
        assertTimeoutPreemptively(Duration.ofSeconds(2), () -> {
            records.addAll(fsConfig.getTask().poll());
            records.addAll(fsConfig.getTask().poll());
        });

        assertEquals((NUM_RECORDS * fsConfig.getDirectories().size()) / 2, records.size());
        checkRecords(records);
        // policy has ended
        assertNull(fsConfig.getTask().poll());
    }


    protected void checkRecords(List<SourceRecord> records) {
        records.forEach(record -> {
            assertEquals("topic_test", record.topic());
            assertNotNull(record.sourcePartition());
            assertNotNull(record.sourceOffset());
            assertNotNull(record.value());

            assertNotNull(((Struct) record.value()).get(TextFileReader.FIELD_NAME_VALUE_DEFAULT));
        });
    }

    protected void createDataFile(FileSystem fs, Path path) throws IOException {
        File file = fillDataFile();
        fs.moveFromLocalFile(new Path(file.getAbsolutePath()), path);
    }

    private File fillDataFile() throws IOException {
        File txtFile = File.createTempFile("test-", ".txt");
        try (FileWriter writer = new FileWriter(txtFile)) {

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    writer.append(value + "\n");
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        return txtFile;
    }
}
