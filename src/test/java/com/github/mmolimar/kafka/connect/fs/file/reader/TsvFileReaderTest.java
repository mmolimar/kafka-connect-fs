package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class TsvFileReaderTest extends UnivocityFileReaderTest<TsvFileReader> {

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        boolean header = args.length < 1 || (boolean) args[0];
        CompressionType compression = args.length < 2 ? COMPRESSION_TYPE_DEFAULT : (CompressionType) args[1];
        File txtFile = File.createTempFile("test-", "." + getFileExtension());
        try (PrintWriter writer = new PrintWriter(getOutputStream(txtFile, compression))) {
            if (header) {
                String headerValue = String.join("\t", FIELD_COLUMN1, FIELD_COLUMN2, FIELD_COLUMN3, FIELD_COLUMN4,
                        FIELD_COLUMN5, FIELD_COLUMN6, FIELD_COLUMN7, FIELD_COLUMN8, FIELD_COLUMN9);
                writer.append(headerValue + "\n");
            }
            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d\t%d\t%d\t%d\t%f\t%f\t%s\t%s\t%s\n",
                        (byte) 2, (short) 4, 8, 16L, 32.32f, 64.64d,
                        true, "test bytes", "test string");
                writer.append(value);
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
            put(TsvFileReader.FILE_READER_DELIMITED_SETTINGS_SCHEMA, "byte,short,int,long,float,double,boolean,bytes,string");
        }};
    }
}
