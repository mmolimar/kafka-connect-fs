package com.github.mmolimar.kafka.connect.fs.task.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.BeforeClass;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HdfsFsSourceTaskTest extends HdfsFsSourceTaskTestBase {

    @BeforeClass
    public static void setUp() throws IOException {
        directories = new ArrayList<Path>() {{
            add(new Path(fsUri.toString(), UUID.randomUUID().toString()));
            add(new Path(fsUri.toString(), UUID.randomUUID().toString()));
        }};
        for (Path dir : directories) {
            fs.mkdirs(dir);
        }
    }

    @Override
    protected void checkRecords(List<SourceRecord> records) {
        records.forEach(record -> {
            assertTrue(record.topic().equals("topic_test"));
            assertNotNull(record.sourcePartition());
            assertNotNull(record.sourceOffset());
            assertNotNull(record.value());

            assertNotNull(((Struct) record.value()).get(TextFileReader.FIELD_VALUE));
        });
    }

    @Override
    protected void createDataFile(Path path) throws IOException {
        File file = fillDataFile();
        fs.moveFromLocalFile(new Path(file.getAbsolutePath()), path);
    }

    private File fillDataFile() throws IOException {
        File txtFile = File.createTempFile("test-", ".txt");
        try (FileWriter writer = new FileWriter(txtFile)) {

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    writer.append(value + "\n");
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        return txtFile;
    }
}
