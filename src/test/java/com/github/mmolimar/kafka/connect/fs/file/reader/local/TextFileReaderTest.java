package com.github.mmolimar.kafka.connect.fs.file.reader.local;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.AgnosticFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
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

import static org.junit.Assert.assertTrue;

public class TextFileReaderTest extends LocalFileReaderTestBase {

    private static final String FIELD_NAME_VALUE = "custom_field_name";
    private static final String FILE_EXTENSION = "txt";

    @BeforeClass
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

    @Test
    public void validFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_ENCODING, "Cp1252");
        }};
        reader = getReader(fs, dataFile, cfg);
        readAllData();
    }

    @Test(expected = UnsupportedCharsetException.class)
    public void invalidFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_ENCODING, "invalid_charset");
        }};
        getReader(fs, dataFile, cfg);
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
