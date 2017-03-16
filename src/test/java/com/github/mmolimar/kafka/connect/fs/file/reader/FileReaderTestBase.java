package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.Assert.*;

public abstract class FileReaderTestBase {
    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected static final int NUM_RECORDS = 10;
    protected static final Map<Integer, Long> OFFSETS_BY_INDEX = new HashMap<>();

    protected static Class<? extends FileReader> readerClass;
    protected static FileSystem fs;
    protected static URI fsUri;
    protected static Path dataFile;
    protected static Map<String, Object> readerConfig;
    protected static FileReader reader;

    @AfterClass
    public static void tearDown() throws IOException {
        fs.delete(dataFile, true);
        fs.close();
    }

    @Before
    public void openReader() throws Throwable {
        reader = getReader(fs, dataFile, readerConfig);
        assertTrue(reader.getFilePath().equals(dataFile));
    }

    @After
    public void closeReader() {
        try {
            reader.close();
        } catch (Exception e) {
            //ignoring
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidArgs() throws Throwable {
        try {
            readerClass.getConstructor(FileSystem.class, Path.class, Map.class).newInstance(null, null, null);
        } catch (Exception e) {
            throw e.getCause();
        }
    }

    @Test(expected = FileNotFoundException.class)
    public void fileDoesNotExist() throws Throwable {
        Path path = new Path(new Path(fsUri), UUID.randomUUID().toString());
        getReader(fs, path, readerConfig);
    }

    @Test(expected = IOException.class)
    public void emptyFile() throws Throwable {
        File tmp = File.createTempFile("test-", "");
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fs, path, readerConfig);
    }

    @Test(expected = IOException.class)
    public void invalidFileFormat() throws Throwable {
        File tmp = File.createTempFile("test-", "");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
            writer.write("test");
        }
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fs, path, readerConfig);
    }

    @Test
    public void readAllData() {
        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(record, recordCount);
            recordCount++;
        }
        assertEquals("The number of records in the file does not match", NUM_RECORDS, recordCount);
    }

    @Test
    public void seekFile() {
        int recordIndex = NUM_RECORDS / 2;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue() + 1, reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = 0;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue() + 1, reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = NUM_RECORDS - 3;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue() + 1, reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        reader.seek(getOffset(OFFSETS_BY_INDEX.get(NUM_RECORDS - 1) + 1));
        assertFalse(reader.hasNext());

    }

    @Test(expected = RuntimeException.class)
    public void negativeSeek() {
        reader.seek(getOffset(-1));
    }

    @Test(expected = NoSuchElementException.class)
    public void exceededSeek() {
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(NUM_RECORDS - 1) + 1));
        assertFalse(reader.hasNext());
        reader.next();
    }

    @Test(expected = RuntimeException.class)
    public void readFileAlreadyClosed() throws IOException {
        reader.close();
        assertFalse(reader.hasNext());
        reader.seek(getOffset(0));
    }

    protected final FileReader getReader(FileSystem fs, Path path, Map<String, Object> config) throws Throwable {
        return ReflectionUtils.makeReader(readerClass, fs, path, config);
    }

    protected abstract Offset getOffset(long offset);

    protected abstract void checkData(Struct record, long index);


}
