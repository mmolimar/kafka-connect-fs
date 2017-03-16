package com.github.mmolimar.kafka.connect.fs.file.reader.local;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class TextFileReaderTest extends LocalFileReaderTestBase {

    private static final String FIELD_VALUE = "value";

    @BeforeClass
    public static void setUp() throws IOException {
        readerClass = TextFileReader.class;
        dataFile = createDataFile();
        readerConfig = new HashMap<>();
    }

    private static Path createDataFile() throws IOException {
        File txtFile = File.createTempFile("test-", ".txt");
        try (FileWriter writer = new FileWriter(txtFile)) {

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    writer.append(value + "\n");
                    OFFSETS_BY_INDEX.put(index, Long.valueOf(index++));
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        Path path = new Path(new Path(fsUri), txtFile.getName());
        fs.moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @Ignore(value = "This does not apply for txt files")
    @Test(expected = IOException.class)
    public void emptyFile() throws Throwable {
        super.emptyFile();
    }

    @Ignore(value = "This does not apply for txt files")
    @Test(expected = IOException.class)
    public void invalidFileFormat() throws Throwable {
        super.invalidFileFormat();
    }

    @Override
    protected Offset getOffset(long offset) {
        return new TextFileReader.TextOffset(offset);
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertTrue(record.get(FIELD_VALUE).toString().startsWith(index + "_"));
    }
}
