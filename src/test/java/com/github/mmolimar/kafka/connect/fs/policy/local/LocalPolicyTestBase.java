package com.github.mmolimar.kafka.connect.fs.policy.local;

import com.github.mmolimar.kafka.connect.fs.policy.PolicyTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;

import java.io.IOException;

public abstract class LocalPolicyTestBase extends PolicyTestBase {

    @BeforeClass
    public static void initFs() throws IOException {
        fsUri = temporaryFolder.getRoot().toURI();
        fs = FileSystem.newInstance(fsUri, new Configuration());
    }
}
