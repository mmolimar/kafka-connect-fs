package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class CobolFileReaderTest extends FileReaderTestBase {

    private static final String FILE_EXTENSION = "dt";
    private static final String DATA_FILENAME_1 = "companies";
    private static final String DATA_FILENAME_2 = "type-variety";
    private static final String DATA_FILENAME_3 = "code-pages";

    @Override
    protected Class<? extends FileReader> getReaderClass() {
        return CobolFileReader.class;
    }

    @Override
    protected Path createDataFile(ReaderFsTestConfig fsConfig, Object... args) throws IOException {
        String filename = args.length < 1 ? DATA_FILENAME_1 : args[0].toString();
        File cobolFile = File.createTempFile("test-", "." + getFileExtension());
        try (InputStream is = CobolFileReaderTest.class.getResourceAsStream("/file/reader/data/cobol/" + filename + "." + getFileExtension())) {
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

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyCopybook(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFile(fsConfig);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_COPYBOOK_CONTENT, "");
        assertThrows(ConnectException.class, () -> getReader(fsConfig.getFs(), file, readerConfig));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void nonExistentCopybook(ReaderFsTestConfig fsConfig) throws IOException {
        Path file = createDataFile(fsConfig);
        Map<String, Object> readerConfig = getReaderConfig();
        Path copybook = new Path(fsConfig.getFs().getWorkingDirectory(), UUID.randomUUID().toString());
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_COPYBOOK_PATH, copybook.toString());
        assertThrows(ConnectException.class, () -> getReader(fsConfig.getFs(), file, readerConfig));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithCopybookInPath(ReaderFsTestConfig fsConfig) throws IOException {
        String dataFilename = DATA_FILENAME_1;
        Path file = createDataFile(fsConfig, dataFilename);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_COPYBOOK_PATH, "");
        assertThrows(ConnectException.class, () -> getReader(fsConfig.getFs(), file, readerConfig));

        File cobolFile = File.createTempFile("copybook-", "." + getFileExtension());
        try (InputStream is = CobolFileReaderTest.class.getResourceAsStream("/file/reader/data/cobol/" + dataFilename + ".cpy")) {
            Files.copy(is, cobolFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
        Path path = new Path(new Path(fsConfig.getFsUri()), cobolFile.getName());
        fsConfig.getFs().copyFromLocalFile(new Path(cobolFile.getAbsolutePath()), path);
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_COPYBOOK_PATH, path.toString());
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            checkData(record, recordCount);
            recordCount++;
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithMultipleDataTypes(ReaderFsTestConfig fsConfig) throws IOException {
        String dataFilename = DATA_FILENAME_2;
        Path file = createDataFile(fsConfig, dataFilename);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_COPYBOOK_CONTENT, copybookContent(dataFilename));
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_READER_SCHEMA_POLICY, "collapse_root");
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_READER_FLOATING_POINT_FORMAT, "ieee754");
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_READER_IS_RECORD_SEQUENCE, "false");

        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            recordCount++;
            assertEquals(recordCount, record.get("ID"));
            assertEquals("Sample", record.get("STRING_VAL"));
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithBinaryData(ReaderFsTestConfig fsConfig) throws IOException {
        String dataFilename = DATA_FILENAME_3;
        Path file = createDataFile(fsConfig, dataFilename);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_COPYBOOK_CONTENT, copybookContent(dataFilename));
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_READER_SCHEMA_POLICY, "collapse_root");
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_READER_IS_RECORD_SEQUENCE, "false");
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            assertEquals(Schema.Type.STRING, record.schema().field("CURRENCY").schema().type());
            assertEquals(Schema.Type.STRING, record.schema().field("SIGNATURE").schema().type());
            assertEquals(Schema.Type.STRING, record.schema().field("COMPANY_NAME_NP").schema().type());
            assertEquals(Schema.Type.STRING, record.schema().field("COMPANY_ID").schema().type());
            assertEquals(Schema.Type.INT32, record.schema().field("WEALTH_QFY").schema().type());
            assertEquals(Schema.Type.FLOAT64, record.schema().field("AMOUNT").schema().type());
            assertNotNull(record.get("CURRENCY"));
            assertNotNull(record.get("SIGNATURE"));
            assertNotNull(record.get("COMPANY_NAME_NP"));
            assertNotNull(record.get("COMPANY_ID"));
            assertNotNull(record.get("WEALTH_QFY"));
            assertNotNull(record.get("AMOUNT"));
            assertEquals(6, record.schema().fields().size());
            recordCount++;
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void readAllDataWithBinaryRawData(ReaderFsTestConfig fsConfig) throws IOException {
        String dataFilename = DATA_FILENAME_3;
        Path file = createDataFile(fsConfig, dataFilename);
        Map<String, Object> readerConfig = getReaderConfig();
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_COPYBOOK_CONTENT, copybookContent(dataFilename));
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_READER_SCHEMA_POLICY, "collapse_root");
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_READER_DEBUG_FIELDS_POLICY, "raw");
        readerConfig.put(CobolFileReader.FILE_READER_COBOL_READER_IS_RECORD_SEQUENCE, "false");
        FileReader reader = getReader(fsConfig.getFs(), file, readerConfig);

        assertTrue(reader.hasNext());

        int recordCount = 0;
        while (reader.hasNext()) {
            Struct record = reader.next();
            assertEquals(Schema.Type.STRING, record.schema().field("CURRENCY").schema().type());
            assertEquals(Schema.Type.STRING, record.schema().field("SIGNATURE").schema().type());
            assertEquals(Schema.Type.STRING, record.schema().field("COMPANY_NAME_NP").schema().type());
            assertEquals(Schema.Type.STRING, record.schema().field("COMPANY_ID").schema().type());
            assertEquals(Schema.Type.INT32, record.schema().field("WEALTH_QFY").schema().type());
            assertEquals(Schema.Type.FLOAT64, record.schema().field("AMOUNT").schema().type());
            assertNotNull(record.get("CURRENCY"));
            assertNotNull(record.get("CURRENCY_debug"));
            assertNotNull(record.get("SIGNATURE"));
            assertNotNull(record.get("SIGNATURE_debug"));
            assertNotNull(record.get("COMPANY_NAME_NP"));
            assertNotNull(record.get("COMPANY_NAME_NP_debug"));
            assertNotNull(record.get("COMPANY_ID"));
            assertNotNull(record.get("COMPANY_ID_debug"));
            assertNotNull(record.get("WEALTH_QFY"));
            assertNotNull(record.get("WEALTH_QFY_debug"));
            assertNotNull(record.get("AMOUNT"));
            assertNotNull(record.get("AMOUNT_debug"));
            assertEquals(12, record.schema().fields().size());
            recordCount++;
        }
        assertEquals(NUM_RECORDS, recordCount, "The number of records in the file does not match");
    }

    @Override
    protected Map<String, Object> getReaderConfig() {
        return new HashMap<String, Object>() {{
            put(CobolFileReader.FILE_READER_COBOL_COPYBOOK_CONTENT, copybookContent(DATA_FILENAME_1));
            put(CobolFileReader.FILE_READER_COBOL_READER_IS_RECORD_SEQUENCE, "true");
        }};
    }

    private String copybookContent(String filename) {
        URL cpy = CobolFileReaderTest.class.getResource("/file/reader/data/cobol/" + filename + ".cpy");
        try {
            return String.join("\n", Files.readAllLines(Paths.get(cpy.toURI())));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
