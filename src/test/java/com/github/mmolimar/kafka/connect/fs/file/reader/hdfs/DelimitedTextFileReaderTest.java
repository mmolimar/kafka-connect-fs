package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.DelimitedTextFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class DelimitedTextFileReaderTest extends HdfsFileReaderTestBase {

    private static final String FIELD_COLUMN1 = "column_1";
    private static final String FIELD_COLUMN2 = "column_2";
    private static final String FIELD_COLUMN3 = "column_3";
    private static final String FIELD_COLUMN4 = "column_4";

    @BeforeClass
    public static void setUp() throws IOException {
        readerClass = DelimitedTextFileReader.class;
        dataFile = createDataFile(true);
        readerConfig = new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "true");
        }};
    }

    private static Path createDataFile(boolean header) throws IOException {
        File txtFile = File.createTempFile("test-", ".txt");
        try (FileWriter writer = new FileWriter(txtFile)) {

            if (header)
                writer.append(FIELD_COLUMN1 + "," + FIELD_COLUMN2 + "," + FIELD_COLUMN3 + "," + FIELD_COLUMN4 + "\n");
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    writer.append(value + "," + value + "," + value + "," + value + "\n");
                    if (header) OFFSETS_BY_INDEX.put(index, Long.valueOf(index++));
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsUri), txtFile.getName());
        fs.moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @Ignore(value = "This test does not apply for txt files")
    @Test(expected = IOException.class)
    public void emptyFile() throws Throwable {
        super.emptyFile();
    }

    @Ignore(value = "This test does not apply for txt files")
    @Test(expected = IOException.class)
    public void invalidFileFormat() throws Throwable {
        super.invalidFileFormat();
    }

    @Test(expected = IllegalArgumentException.class)
    public void invaliConfigArgs() throws Throwable {
        try {
            readerClass.getConstructor(FileSystem.class, Path.class, Map.class).newInstance(fs, dataFile, new HashMap<>());
        } catch (Exception e) {
            throw e.getCause();
        }
    }

    @Test
    public void readAllDataWithoutHeader() throws Throwable {
        Path file = createDataFile(false);
        FileReader reader = getReader(fs, file, new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "false");
        }});

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
    public void readAllDataWithMalformedRows() throws Throwable {
        File tmp = File.createTempFile("test-", "");
        try (FileWriter writer = new FileWriter(tmp)) {
            writer.append(FIELD_COLUMN1 + "," + FIELD_COLUMN2 + "," + FIELD_COLUMN3 + "," + FIELD_COLUMN4 + "\n");
            writer.append("dummy\n");
            writer.append("dummy\n");
        }
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "true");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_DEFAULT_VALUE, "custom_value");
        }};
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        reader = getReader(fs, path, cfg);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            assertTrue(record.get(FIELD_COLUMN1).equals("dummy"));
            assertTrue(record.get(FIELD_COLUMN2).equals("custom_value"));
            assertTrue(record.get(FIELD_COLUMN3).equals("custom_value"));
            assertTrue(record.get(FIELD_COLUMN4).equals("custom_value"));
            recordCount++;
        }
        assertEquals("The number of records in the file does not match", 2, recordCount);
    }

    @Test
    public void seekFileWithoutHeader() throws Throwable {
        Path file = createDataFile(false);
        FileReader reader = getReader(fs, file, new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "false");
        }});

        assertTrue(reader.hasNext());

        int recordIndex = NUM_RECORDS / 2;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex), false));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue() + 1, reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = 0;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex), false));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue() + 1, reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = NUM_RECORDS - 3;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex), false));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex).longValue() + 1, reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        reader.seek(getOffset(OFFSETS_BY_INDEX.get(NUM_RECORDS - 1) + 1, false));
        assertFalse(reader.hasNext());

    }

    @Test
    public void validFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "true");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_ENCODING, "Cp1252");
        }};
        getReader(fs, dataFile, cfg);
    }

    @Test(expected = UnsupportedCharsetException.class)
    public void invalidFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "true");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_ENCODING, "invalid_charset");
        }};
        getReader(fs, dataFile, cfg);
    }

    @Override
    protected Offset getOffset(long offset) {
        return getOffset(offset, true);
    }

    private Offset getOffset(long offset, boolean hasHeader) {
        return new DelimitedTextFileReader.DelimitedTextOffset(offset, hasHeader);
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertTrue(record.get(FIELD_COLUMN1).toString().startsWith(index + "_"));
        assertTrue(record.get(FIELD_COLUMN2).toString().startsWith(index + "_"));
        assertTrue(record.get(FIELD_COLUMN3).toString().startsWith(index + "_"));
        assertTrue(record.get(FIELD_COLUMN4).toString().startsWith(index + "_"));
    }
}
