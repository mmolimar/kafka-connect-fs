package com.github.mmolimar.kafka.connect.fs.file.reader.local;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.AgnosticFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.DelimitedTextFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import org.apache.hadoop.fs.FileSystem;
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

public class DelimitedTextFileReaderTest extends LocalFileReaderTestBase {

    private static final String FIELD_COLUMN1 = "column_1";
    private static final String FIELD_COLUMN2 = "column_2";
    private static final String FIELD_COLUMN3 = "column_3";
    private static final String FIELD_COLUMN4 = "column_4";
    private static final String FILE_EXTENSION = "tcsv";

    @BeforeAll
    public static void setUp() throws IOException {
        readerClass = AgnosticFileReader.class;
        dataFile = createDataFile(true);
        readerConfig = new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "true");
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED, FILE_EXTENSION);
        }};
    }

    private static Path createDataFile(boolean header) throws IOException {
        File txtFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (FileWriter writer = new FileWriter(txtFile)) {

            if (header)
                writer.append(FIELD_COLUMN1 + "," + FIELD_COLUMN2 + "," + FIELD_COLUMN3 + "," + FIELD_COLUMN4 + "\n");
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    writer.append(value + "," + value + "," + value + "," + value + "\n");
                    if (header) OFFSETS_BY_INDEX.put(index, (long) index);
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
    public void invaliConfigArgs() {
        try {
            readerClass.getConstructor(FileSystem.class, Path.class, Map.class).newInstance(fs, dataFile,
                    new HashMap<String, Object>() {{
                        put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED, FILE_EXTENSION);
                    }});
        } catch (Exception e) {
            assertThrows(IllegalArgumentException.class, () -> {
                throw e.getCause();
            });
        }
    }

    @Test
    public void readAllDataWithoutHeader() throws Throwable {
        Path file = createDataFile(false);
        FileReader reader = getReader(fs, file, new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "false");
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED, getFileExtension());
        }});

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
    public void readAllDataWithMalformedRows() throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (FileWriter writer = new FileWriter(tmp)) {
            writer.append(FIELD_COLUMN1 + "," + FIELD_COLUMN2 + "," + FIELD_COLUMN3 + "," + FIELD_COLUMN4 + "\n");
            writer.append("dummy\n");
            writer.append("dummy\n");
        }
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "true");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_DEFAULT_VALUE, "custom_value");
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED, getFileExtension());
        }};
        Path path = new Path(new Path(fsUri), tmp.getName());
        fs.moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        reader = getReader(fs, path, cfg);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            assertAll(
                    () -> assertEquals("dummy", record.get(FIELD_COLUMN1)),
                    () -> assertEquals("custom_value", record.get(FIELD_COLUMN2)),
                    () -> assertEquals("custom_value", record.get(FIELD_COLUMN3)),
                    () -> assertEquals("custom_value", record.get(FIELD_COLUMN4))
            );
            recordCount++;
        }
        assertEquals(2, recordCount, () -> "The number of records in the file does not match");
    }

    @Test
    public void seekFileWithoutHeader() throws Throwable {
        Path file = createDataFile(false);
        FileReader reader = getReader(fs, file, new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "false");
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED, getFileExtension());
        }});

        assertTrue(reader.hasNext());

        int recordIndex = NUM_RECORDS / 2;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex), false));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex) + 1, reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = 0;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex), false));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex) + 1, reader.currentOffset().getRecordOffset());
        checkData(reader.next(), recordIndex);

        recordIndex = NUM_RECORDS - 3;
        reader.seek(getOffset(OFFSETS_BY_INDEX.get(recordIndex), false));
        assertTrue(reader.hasNext());
        assertEquals(OFFSETS_BY_INDEX.get(recordIndex) + 1, reader.currentOffset().getRecordOffset());
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
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED, getFileExtension());
        }};
        getReader(fs, dataFile, cfg);
    }

    @Test
    public void invalidFileEncoding() {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_TOKEN, ",");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_HEADER, "true");
            put(DelimitedTextFileReader.FILE_READER_DELIMITED_ENCODING, "invalid_charset");
            put(AgnosticFileReader.FILE_READER_AGNOSTIC_EXTENSIONS_DELIMITED, getFileExtension());
        }};
        assertThrows(UnsupportedCharsetException.class, () -> getReader(fs, dataFile, cfg));
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
        assertAll(
                () -> assertTrue(record.get(FIELD_COLUMN1).toString().startsWith(index + "_")),
                () -> assertTrue(record.get(FIELD_COLUMN2).toString().startsWith(index + "_")),
                () -> assertTrue(record.get(FIELD_COLUMN3).toString().startsWith(index + "_")),
                () -> assertTrue(record.get(FIELD_COLUMN4).toString().startsWith(index + "_"))
        );
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }
}
