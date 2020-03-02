package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.AgnosticFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.UnsupportedCharsetException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class TextFileReaderTest extends HdfsFileReaderTestBase {

    private static final String FIELD_NAME_VALUE = "custom_field_name";
    private static final String FILE_EXTENSION = "txt";

    @BeforeAll
    public static void setUp() throws IOException {
        readerClass = AgnosticFileReader.class;
        dataFile = createDataFile();
        readerConfig = new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
        }};
    }

    private static Path createDataFile() throws IOException {
        File txtFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (FileWriter writer = new FileWriter(txtFile)) {

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    writer.append(value + "\n");
                    OFFSETS_BY_INDEX.put(index, (long) index);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsUri), txtFile.getName());
        fs.moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @Test
    public void emptyFile() throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fs, path, readerConfig);
    }

    @Test
    public void invalidFileFormat() throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
            writer.write("test");
        }
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fs, path, readerConfig);
    }

    @Test
    public void validFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_ENCODING, "Cp1252");
        }};
        reader = getReader(fs, dataFile, cfg);
        readAllData();
    }

    @Test
    public void invalidFileEncoding() {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_ENCODING, "invalid_charset");
        }};
        assertThrows(UnsupportedCharsetException.class, () -> getReader(fs, dataFile, cfg));
    }

    @Test
    public void readDataWithRecordPerLineDisabled() throws Throwable {
        Path file = createDataFile();
        FileReader reader = getReader(fs, file, new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_RECORD_PER_LINE, "false");
        }});

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(record, recordCount);
            recordCount++;
        }
        reader.close();
        assertEquals(1, recordCount, () -> "The number of records in the file does not match");
    }

    @Override
    protected Offset getOffset(long offset) {
        return new TextFileReader.TextOffset(offset);
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertTrue(record.get(FIELD_NAME_VALUE).toString().startsWith(index + "_"));
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
