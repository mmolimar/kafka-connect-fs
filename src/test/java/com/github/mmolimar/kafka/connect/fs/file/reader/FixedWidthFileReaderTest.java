package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FixedWidthFileReaderTest extends UnivocityFileReaderTest<FixedWidthFileReader> {

    private static final int[] fieldLengths = new int[]{45, 53, 71, 89};

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        boolean header = args.length < 1 || (boolean) args[0];
        CompressionType compression = args.length < 2 ? COMPRESSION_TYPE_DEFAULT : (CompressionType) args[1];
        File txtFile = File.createTempFile("test-", "." + getFileExtension());
        try (PrintWriter writer = new PrintWriter(getOutputStream(txtFile, compression))) {
            if (header) {
                writer.append(String.format("%-" + fieldLengths[0] + "s", FIELD_COLUMN1) +
                        String.format("%-" + fieldLengths[1] + "s", FIELD_COLUMN2) +
                        String.format("%-" + fieldLengths[2] + "s", FIELD_COLUMN3) +
                        String.format("%-" + fieldLengths[3] + "s", FIELD_COLUMN4) + "\n");
            }
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                writer.append(String.format("%-" + fieldLengths[0] + "s", value) +
                        String.format("%-" + fieldLengths[1] + "s", value) +
                        String.format("%-" + fieldLengths[2] + "s", value) +
                        String.format("%-" + fieldLengths[3] + "s", value) + "\n");
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
            put(FixedWidthFileReader.FILE_READER_DELIMITED_SETTINGS_HEADER, "true");
            put(FixedWidthFileReader.FILE_READER_DELIMITED_SETTINGS_FIELD_LENGTHS,
                    Arrays.stream(fieldLengths).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        }};
    }

}
