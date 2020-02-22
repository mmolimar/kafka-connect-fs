package com.github.mmolimar.kafka.connect.fs.policy.hdfs;

import com.github.mmolimar.kafka.connect.fs.policy.PolicyTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class HdfsPolicyTestBase extends PolicyTestBase {

    private static MiniDFSCluster cluster;

    @BeforeClass
    public static void initFs() throws IOException {
        Configuration clusterConfig = new Configuration();
        Path hdfsDir = Files.createTempDirectory("test-");
        clusterConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.toAbsolutePath().toString());
        cluster = new MiniDFSCluster.Builder(clusterConfig).build();
        fsUri = URI.create("hdfs://localhost:" + cluster.getNameNodePort() + "/");
        fs = FileSystem.newInstance(fsUri, new Configuration());
    }

    @AfterClass
    public static void finishFs() throws Exception {
        cluster.shutdown(true);
    }
}
