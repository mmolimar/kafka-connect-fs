package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.SequenceFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kafka.connect.data.Struct;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

public class SequenceFileReaderTest extends HdfsFileReaderTestBase {

    private static final String FIELD_KEY = "key";
    private static final String FIELD_VALUE = "value";

    @BeforeClass
    public static void setUp() throws IOException {
        readerClass = SequenceFileReader.class;
        dataFile = createDataFile();
    }

    private static Path createDataFile() throws IOException {
        File seqFile = File.createTempFile("test-", ".seq");
        try (SequenceFile.Writer writer = SequenceFile.createWriter(fs.getConf(), SequenceFile.Writer.file(new Path(seqFile.getAbsolutePath())),
                SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class))) {

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                Writable key = new IntWritable(index);
                Writable value = new Text(String.format("%d_%s", index, UUID.randomUUID()));
                try {
                    writer.append(key, value);
                    writer.sync();
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        try (SequenceFile.Reader reader = new SequenceFile.Reader(fs.getConf(),
                SequenceFile.Reader.file(new Path(seqFile.getAbsolutePath())))) {
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());
            int index = 0;
            long pos = reader.getPosition() - 1;
            while (reader.next(key, value)) {
                OFFSETS_BY_INDEX.put(index++, pos);
                pos = reader.getPosition();
            }
        }
        Path path = new Path(new Path(fsUri), seqFile.getName());
        fs.moveFromLocalFile(new Path(seqFile.getAbsolutePath()), path);
        return path;
    }

    @Override
    protected Offset getOffset(long offset) {
        return new SequenceFileReader.SeqOffset(offset);
    }

    @Override
    protected void checkData(Struct record, long index) {
        assertTrue((Integer) record.get(FIELD_KEY) == index);
        assertTrue(record.get(FIELD_VALUE).toString().startsWith(index + "_"));
    }
}
