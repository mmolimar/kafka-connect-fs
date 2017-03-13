package com.github.mmolimar.kafka.connect.fs.file.reader.local;

import com.github.mmolimar.kafka.connect.fs.file.reader.FileReaderTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;

import java.io.IOException;

public abstract class LocalFileReaderTestBase extends FileReaderTestBase {

    @BeforeClass
    public static void initFs() throws IOException {
        fsUri = temporaryFolder.getRoot().toURI();
        fs = FileSystem.newInstance(fsUri, new Configuration());
    }

}
