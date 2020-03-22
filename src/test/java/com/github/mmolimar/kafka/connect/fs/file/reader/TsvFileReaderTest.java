package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

public class TsvFileReaderTest extends UnivocityFileReaderTest<TsvFileReader> {

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        boolean header = args.length < 1 || (boolean) args[0];
        CompressionType compression = args.length < 2 ? COMPRESSION_TYPE_DEFAULT : (CompressionType) args[1];
        File txtFile = File.createTempFile("test-", "." + getFileExtension());
        try (PrintWriter writer = new PrintWriter(getOutputStream(txtFile, compression))) {
            if (header) {
                writer.append(FIELD_COLUMN1 + "\t" + FIELD_COLUMN2 + "\t" + FIELD_COLUMN3 + "\t" + FIELD_COLUMN4 + "\n");
            }
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                writer.append(value + "\t" + value + "\t" + value + "\t" + value + "\n");
                fsConfig.offsetsByIndex().put(index, (long) index);
            });
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), txtFile.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(txtFile.getAbsolutePath()), path);
        return path;
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<String, Object>() {{
            put(TsvFileReader.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
        }};
    }
}
