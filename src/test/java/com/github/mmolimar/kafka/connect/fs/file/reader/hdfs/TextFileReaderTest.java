package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.AgnosticFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class TextFileReaderTest extends HdfsFileReaderTestBase {

    private static final String FIELD_NAME_VALUE = "custom_field_name";
    private static final String FILE_EXTENSION = "txt";
    private static final TextFileReader.CompressionType COMPRESSION_TYPE = TextFileReader.CompressionType.GZIP;

    @BeforeAll
    public static void setUp() throws IOException {
        readerClass = AgnosticFileReader.class;
        dataFile = createDataFile(COMPRESSION_TYPE);
        readerConfig = new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, COMPRESSION_TYPE);
            put(TextFileReader.FILE_READER_TEXT_COMPRESSION_CONCATENATED, "true");
        }};
    }

    private static OutputStream getOutputStream(File file, TextFileReader.CompressionType compression) throws IOException {
        final OutputStream os;
        switch (compression) {
            case BZIP2:
                os = new BZip2CompressorOutputStream(new FileOutputStream(file));
                break;
            case GZIP:
                os = new GzipCompressorOutputStream(new FileOutputStream(file));
                break;
            default:
                os = new FileOutputStream(file);
                break;
        }
        return os;
    }

    private static Path createDataFile(TextFileReader.CompressionType compression) throws IOException {
        File txtFile = File.createTempFile("test-", "." + FILE_EXTENSION);
        try (PrintWriter writer = new PrintWriter(getOutputStream(txtFile, compression))) {
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                writer.append(value + "\n");
                OFFSETS_BY_INDEX.put(index, (long) index);
            });
        }
        Path path = new Path(new Path(fsUri), txtFile.getName());
        fs.moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @Test
    public void validFileEncoding() throws Throwable {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_ENCODING, "Cp1252");
            put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, COMPRESSION_TYPE);
        }};
        reader = getReader(fs, dataFile, cfg);
        readAllData();
    }

    @Test
    public void invalidFileEncoding() {
        Map<String, Object> cfg = new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_ENCODING, "invalid_charset");
            put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, COMPRESSION_TYPE);
        }};
        assertThrows(UnsupportedCharsetException.class, () -> getReader(fs, dataFile, cfg));
    }

    @Test
    public void readDataWithRecordPerLineDisabled() throws Throwable {
        Path file = createDataFile(COMPRESSION_TYPE);
        FileReader reader = getReader(fs, file, new HashMap<String, Object>() {{
            put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
            put(TextFileReader.FILE_READER_TEXT_RECORD_PER_LINE, "false");
            put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, COMPRESSION_TYPE);
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

    @Test
    public void readDifferentCompressionTypes() {
        Arrays.stream(TextFileReader.CompressionType.values()).forEach(compressionType -> {
            try {
                Path file = createDataFile(compressionType);
                FileReader reader = getReader(fs, file, new HashMap<String, Object>() {{
                    put(TextFileReader.FILE_READER_TEXT_FIELD_NAME_VALUE, FIELD_NAME_VALUE);
                    put(TextFileReader.FILE_READER_TEXT_COMPRESSION_TYPE, compressionType);
                }});

                assertTrue(reader.hasNext());

                int recordCount = 0;
                while (reader.hasNext()) {
                    Struct record = reader.next();
                    checkData(record, recordCount);
                    recordCount++;
                }
                reader.close();
                assertEquals(NUM_RECORDS, recordCount, () -> "The number of records in the file does not match");
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
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
