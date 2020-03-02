package com.github.mmolimar.kafka.connect.fs.policy.local;

import com.github.mmolimar.kafka.connect.fs.policy.PolicyTestBase;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class LocalPolicyTestBase extends PolicyTestBase {

    private static Path localDir;

    @BeforeAll
    public static void initFs() throws IOException {
        localDir = Files.createTempDirectory("test-");
        fsUri = localDir.toUri();
        fs = FileSystem.newInstance(fsUri, new Configuration());
    }

    @AfterAll
    public static void finishFs() throws IOException {
        FileUtils.deleteDirectory(localDir.toFile());
    }
}
