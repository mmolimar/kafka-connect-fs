package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class CsvFileReaderTest extends UnivocityFileReaderTest<CsvFileReader> {

    @Override
    protected Path createDataFile(FileSystemConfig fsConfig, Object... args) throws IOException {
        boolean header = args.length < 1 || (boolean) args[0];
        CompressionType compression = args.length < 2 ? COMPRESSION_TYPE_DEFAULT : (CompressionType) args[1];
        File txtFile = File.createTempFile("test-", "." + getFileExtension());
        try (PrintWriter writer = new PrintWriter(getOutputStream(txtFile, compression))) {
            if (header) {
                writer.append(FIELD_COLUMN1 + "#" + FIELD_COLUMN2 + "#" + FIELD_COLUMN3 + "#" + FIELD_COLUMN4 + "\n");
            }
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                writer.append(value + "#" + value + "#" + value + "#" + value + "\n");
                fsConfig.offsetsByIndex().put(index, (long) index);
            });
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), txtFile.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithMalformedRows(FileSystemConfig fsConfig) throws Throwable {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (FileWriter writer = new FileWriter(tmp)) {
            writer.append(FIELD_COLUMN1 + "," + FIELD_COLUMN2 + "," + FIELD_COLUMN3 + "," + FIELD_COLUMN4 + "\n");
            writer.append("dummy,\"\",,dummy\n");
            writer.append("#comment\n");
            writer.append("dummy,\"\",,dummy\n");
        }
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_FORMAT_DELIMITER, ",");
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_EMPTY_VALUE, "empty_value");
        readerConfig.put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_NULL_VALUE, "null_value");

        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        FileReader reader = getReader(fsConfig.getFs(), path, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            assertAll(
                    () -> assertEquals("dummy", record.get(FIELD_COLUMN1)),
                    () -> assertEquals("empty_value", record.get(FIELD_COLUMN2)),
                    () -> assertEquals("null_value", record.get(FIELD_COLUMN3)),
                    () -> assertEquals("dummy", record.get(FIELD_COLUMN4))
            );
            recordCount++;
        }
        assertEquals(2, recordCount, () -> "The number of records in the file does not match");
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<String, Object>() {{
            put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_FORMAT_DELIMITER, "#");
            put(CsvFileReader.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
        }};
    }
}
