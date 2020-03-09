package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public abstract class FileReaderTestBase {

    protected static final int NUM_RECORDS = 100;
    protected static final Map<Integer, Long> OFFSETS_BY_INDEX = new HashMap<>();

    protected static Class<? extends FileReader> readerClass;
    protected static FileSystem fs;
    protected static URI fsUri;
    protected static Path dataFile;
    protected static Map<String, Object> readerConfig;
    protected static FileReader reader;

    @AfterAll
    public static void tearDown() throws IOException {
        fs.close();
    }

    @BeforeEach
    public void openReader() throws Throwable {
        reader = getReader(fs, dataFile, readerConfig);
        assertEquals(reader.getFilePath(), dataFile);
    }

    @AfterEach
    public void closeReader() {
        try {
            reader.close();
        } catch (Exception e) {
            //ignoring
        }
    }

    @Test
    public void invalidArgs() {
        try {
            readerClass.getConstructor(FileSystem.class, Path.class, Map.class).newInstance(null, null, null);
        } catch (Exception e) {
            assertThrows(IllegalArgumentException.class, () -> {
                throw e.getCause();
            });
        }
    }

    @Test
    public void fileDoesNotExist() {
        Path path = new Path(new Path(fsUri), UUID.randomUUID().toString());
        assertThrows(FileNotFoundException.class, () -> getReader(fs, path, readerConfig));
    }

    @Test
    public void emptyFile() throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        assertThrows(IOException.class, () -> getReader(fs, path, readerConfig));
    }

    @Test
    public void invalidFileFormat() throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
            writer.write("test");
        }
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        assertThrows(IOException.class, () -> getReader(fs, path, readerConfig));
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
        assertEquals(NUM_RECORDS, recordCount, () -> "The number of records in the file does not match");
    }

    @Test
    public void seekFile() {
        int recordIndex = NUM_RECORDS / 2;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = 0;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = NUM_RECORDS - 3;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex)));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex), reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        reader.seek(getOffset(OFFSETS_BY_INDEX.get(NUM_RECORDS - 1) + 1));
        assertFalse(reader.hasNext());
    }

    @Test
    public void negativeSeek() {
        assertThrows(RuntimeException.class, () -> reader.seek(getOffset(-1)));
    }

    @Test
    public void exceededSeek() {
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(NUM_RECORDS - 1) + 1));
        assertFalse(reader.hasNext());
        assertThrows(NoSuchElementException.class, () -> reader.next());
    }

    @Test
    public void readFileAlreadyClosed() throws IOException {
        reader.close();
        assertThrows(IllegalStateException.class, () -> reader.hasNext());
        assertThrows(IllegalStateException.class, () -> reader.next());
    }

    protected final FileReader getReader(FileSystem fs, Path path, Map<String, Object> config) throws Throwable {
        return ReflectionUtils.makeReader(readerClass, fs, path, config);
    }

    protected abstract Offset getOffset(long offset);

    protected abstract void checkData(Struct record, long index);

    protected abstract String getFileExtension();

}
