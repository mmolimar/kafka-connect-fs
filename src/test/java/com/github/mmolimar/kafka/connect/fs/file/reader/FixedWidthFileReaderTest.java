package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FixedWidthFileReaderTest extends UnivocityFileReaderTest<FixedWidthFileReader> {

    private static final int[] fieldLengths = new int[]{45, 53, 71, 89, 14, 44, 67, 46, 75};

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
                        String.format("%-" + fieldLengths[3] + "s", FIELD_COLUMN4) +
                        String.format("%-" + fieldLengths[4] + "s", FIELD_COLUMN5) +
                        String.format("%-" + fieldLengths[5] + "s", FIELD_COLUMN6) +
                        String.format("%-" + fieldLengths[6] + "s", FIELD_COLUMN7) +
                        String.format("%-" + fieldLengths[7] + "s", FIELD_COLUMN8) +
                        String.format("%-" + fieldLengths[8] + "s", FIELD_COLUMN9) + "\n");
            }
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                writer.append(String.format("%-" + fieldLengths[0] + "s", String.format("%d", (byte) 2)) +
                        String.format("%-" + fieldLengths[1] + "s", String.format("%d", (short) 4)) +
                        String.format("%-" + fieldLengths[2] + "s", String.format("%d", 8)) +
                        String.format("%-" + fieldLengths[3] + "s", String.format("%d", 16L)) +
                        String.format("%-" + fieldLengths[4] + "s", String.format("%f", 32.32f)) +
                        String.format("%-" + fieldLengths[5] + "s", String.format("%f", 64.64d)) +
                        String.format("%-" + fieldLengths[6] + "s", String.format("%s", true)) +
                        String.format("%-" + fieldLengths[7] + "s", String.format("%s", "test bytes")) +
                        String.format("%-" + fieldLengths[8] + "s", String.format("%s", "test string")) + "\n"
                );
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
            put(FixedWidthFileReader.FILE_READER_DELIMITED_SETTINGS_SCHEMA, "byte,short,int,long,float,double,boolean,bytes,string");
        }};
    }

}
