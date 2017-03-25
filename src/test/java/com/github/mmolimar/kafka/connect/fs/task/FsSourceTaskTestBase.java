package com.github.mmolimar.kafka.connect.fs.task;

import com.github.mmolimar.kafka.connect.fs.FsSourceTask;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.AvroFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.membermodification.MemberModifier;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class FsSourceTaskTestBase {

    protected static final int NUM_RECORDS = 10;

    protected static FileSystem fs;
    protected static List<Path> directories;
    protected static URI fsUri;

    protected FsSourceTask task;
    protected Map<String, String> taskConfig;
    protected SourceTaskContext taskContext;
    protected OffsetStorageReader offsetStorageReader;

    @AfterClass
    public static void tearDown() throws Exception {
        fs.close();
    }

    @Before
    public void initTask() {
        task = new FsSourceTask();
        taskConfig = new HashMap<String, String>() {{
            String uris[] = directories.stream().map(dir -> dir.toString())
                    .toArray(size -> new String[size]);
            put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
            put(FsSourceTaskConfig.TOPIC, "topic_test");
            put(FsSourceTaskConfig.POLICY_CLASS, SimplePolicy.class.getName());
            put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
            put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
        }};

        //Mock initialization
        taskContext = PowerMock.createMock(SourceTaskContext.class);
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);

        EasyMock.expect(taskContext.offsetStorageReader())
                .andReturn(offsetStorageReader);

        EasyMock.expect(taskContext.offsetStorageReader())
                .andReturn(offsetStorageReader);

        EasyMock.expect(offsetStorageReader.offset(EasyMock.anyObject()))
                .andReturn(new HashMap<String, Object>() {{
                    put("offset", 5L);
                }});
        EasyMock.expect(offsetStorageReader.offset(EasyMock.anyObject()))
                .andReturn(new HashMap<String, Object>() {{
                    put("offset", 5L);
                }});

        EasyMock.checkOrder(taskContext, false);
        EasyMock.replay(taskContext);

        EasyMock.checkOrder(offsetStorageReader, false);
        EasyMock.replay(offsetStorageReader);

        task.initialize(taskContext);

    }

    @After
    public void cleanDirsAndStop() throws IOException {
        for (Path dir : directories) {
            fs.delete(dir, true);
            fs.mkdirs(dir);
        }
        task.stop();
    }

    @Test
    public void pollNoData() throws InterruptedException {
        task.start(taskConfig);
        assertEquals(0, task.poll().size());
        //policy has ended
        assertNull(task.poll());
    }

    @Test
    public void emptyFilesToProcess() throws IOException, InterruptedException {
        for (Path dir : directories) {
            fs.createNewFile(new Path(dir, String.valueOf(System.nanoTime() + ".txt")));
            //this file does not match the regexp
            fs.createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }
        task.start(taskConfig);
        assertEquals(0, task.poll().size());
        //policy has ended
        assertNull(task.poll());
    }

    @Test
    public void oneFilePerFs() throws IOException, InterruptedException {
        for (Path dir : directories) {
            Path dataFile = new Path(dir, String.valueOf(System.nanoTime() + ".txt"));
            createDataFile(dataFile);
            //this file does not match the regexp
            fs.createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        task.start(taskConfig);
        List<SourceRecord> records = task.poll();
        assertEquals(10, records.size());
        checkRecords(records);
        //policy has ended
        assertNull(task.poll());
    }

    @Test
    public void nonExistentUri() throws InterruptedException {
        Map<String, String> props = new HashMap<>(taskConfig);
        props.put(FsSourceTaskConfig.FS_URIS, new Path(fs.getWorkingDirectory(), UUID.randomUUID().toString()).toString());
        task.start(props);
        task.poll();
    }

    @Test
    public void exceptionExecutingPolicy() throws InterruptedException, IOException, IllegalAccessException {
        Map<String, String> props = new HashMap<>(taskConfig);
        task.start(props);

        Policy policy = EasyMock.createNiceMock(Policy.class);
        EasyMock.expect(policy.hasEnded()).andReturn(Boolean.FALSE);
        EasyMock.expect(policy.execute()).andThrow(new ConnectException("Exception from mock"));
        EasyMock.expect(policy.getURIs()).andReturn(null);
        EasyMock.checkOrder(policy, false);
        EasyMock.replay(policy);
        MemberModifier.field(FsSourceTask.class, "policy").set(task, policy);

        assertEquals(0, task.poll().size());
    }

    @Test
    public void exceptionReadingFile() throws InterruptedException, IOException {
        Map<String, String> props = new HashMap<>(taskConfig);
        File tmp = File.createTempFile("test-", ".txt");
        try (PrintWriter writer = new PrintWriter(tmp)) {
            writer.append("txt");
        }
        Path dest = new Path(directories.get(0).toString(), System.nanoTime() + ".txt");
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), dest);
        props.put(FsSourceTaskConfig.FILE_READER_CLASS, AvroFileReader.class.getName());
        task.start(props);
        assertEquals(0, task.poll().size());
        task.stop();

        fs.delete(dest, false);
    }

    protected abstract void checkRecords(List<SourceRecord> records);

    protected abstract void createDataFile(Path path) throws IOException;

}