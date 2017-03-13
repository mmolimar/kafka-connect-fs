package com.github.mmolimar.kafka.connect.fs.task.local;

import com.github.mmolimar.kafka.connect.fs.task.FsSourceTaskTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;

import java.io.IOException;

public abstract class LocalFsSourceTaskTestBase extends FsSourceTaskTestBase {

    @BeforeClass
    public static void initFs() throws IOException {
        fsUri = temporaryFolder.getRoot().toURI();
        fs = FileSystem.newInstance(fsUri, new Configuration());
    }
}
