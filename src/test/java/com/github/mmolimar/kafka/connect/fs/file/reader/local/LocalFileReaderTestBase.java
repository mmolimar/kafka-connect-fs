package com.github.mmolimar.kafka.connect.fs.file.reader.local;

import com.github.mmolimar.kafka.connect.fs.file.reader.FileReaderTestBase;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class LocalFileReaderTestBase extends FileReaderTestBase {

    private static Path localDir;

    @BeforeClass
    public static void initFs() throws IOException {
        localDir = Files.createTempDirectory("test-");
        fsUri = localDir.toUri();
        fs = FileSystem.newInstance(fsUri, new Configuration());
    }

    @AfterClass
    public static void finishFs() throws IOException {
        FileUtils.deleteDirectory(localDir.toFile());
    }

}
