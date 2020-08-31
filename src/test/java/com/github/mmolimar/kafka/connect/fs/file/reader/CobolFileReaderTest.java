package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class CobolFileReaderTest extends FileReaderTestBase {

    private static final String FILE_EXTENSION = "dt";

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return CobolFileReader.class;
    }

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        File cobolFile = File.createTempFile("test-", "." + getFileExtension());
        try (InputStream is = CobolFileReaderTest.class.getResourceAsStream("/file/reader/data/cobol/companies.dat")) {
            Files.copy(is, cobolFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        IntStream.range(0, NUM_RECORDS).forEach(index -> fsConfig.offsetsByIndex().put(index, (long) index));
        Path path = new Path(new Path(fsConfig.getFsUri()), cobolFile.getName());
        fsConfig.getFs().copyFromLocalFile(new Path(cobolFile.getAbsolutePath()), path);

        return path;
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidFileFormat(ReaderFsTestConfig fsConfig) throws IOException {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tmp))) {
            writer.write("test");
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fsConfig.getFs(), path, getReaderConfig());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFile(ReaderFsTestConfig fsConfig) throws IOException {
        File tmp = File.createTempFile("test-", "." + getFileExtension());
        Path path = new Path(new Path(fsConfig.getFsUri()), tmp.getName());
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), path);
        getReader(fsConfig.getFs(), path, getReaderConfig());
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<String, Object>() {{
            put(CobolFileReader.FILE_READER_COPYBOOK_CONTENT, copybookContent());
        }};
    }

    private String copybookContent() {
        return "          01  COMPANY-DETAILS.\n" +
                "              05  SEGMENT-ID        PIC X(5).\n" +
                "              05  COMPANY-ID        PIC X(10).\n" +
                "              05  STATIC-DETAILS.\n" +
                "                 10  COMPANY-NAME      PIC X(15).\n" +
                "                 10  ADDRESS           PIC X(25).\n" +
                "                 10  TAXPAYER.\n" +
                "                    15  TAXPAYER-TYPE  PIC X(1).\n" +
                "                    15  TAXPAYER-STR   PIC X(8).\n" +
                "                    15  TAXPAYER-NUM  REDEFINES TAXPAYER-STR\n" +
                "                                       PIC 9(8) COMP.";
    }

    @Override
    protected String getFileExtension() {
        return FILE_EXTENSION;
    }

    @Override
    protected void checkData(Struct record, long index) {
        Struct companyDetails = record.getStruct("COMPANY_DETAILS");
        Struct staticDetails = companyDetails.getStruct("STATIC_DETAILS");
        Struct taxpayer = staticDetails.getStruct("TAXPAYER");
        assertAll(
                () -> assertEquals("C", companyDetails.getString("SEGMENT_ID")),
                () -> assertEquals(String.format("%010d", index), companyDetails.getString("COMPANY_ID")),

                () -> assertEquals("Sample Q&A Ltd.", staticDetails.getString("COMPANY_NAME")),
                () -> assertEquals("223344 AK ave, Wonderland", staticDetails.getString("ADDRESS")),

                () -> assertEquals("A", taxpayer.getString("TAXPAYER_TYPE")),
                () -> assertEquals("88888888", taxpayer.getString("TAXPAYER_STR")),
                () -> assertNull(taxpayer.getInt32("TAXPAYER_NUM"))
        );
    }
}
