package com.github.mmolimar.kafka.connect.fs.file.reader.hdfs;

import com.github.mmolimar.kafka.connect.fs.file.reader.FileReaderTestBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;

public abstract class HdfsFileReaderTestBase extends FileReaderTestBase {

    private static MiniDFSCluster cluster;
    private static Configuration clusterConfig;

    @BeforeClass
    public static void initFs() throws IOException {
        clusterConfig = new Configuration();
        clusterConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, temporaryFolder.getRoot().getAbsolutePath());
        cluster = new MiniDFSCluster.Builder(clusterConfig).build();
        fsUri = URI.create("hdfs://localhost:" + cluster.getNameNodePort() + "/");
        fs = FileSystem.newInstance(fsUri, new Configuration());
    }

    @AfterClass
    public static void finalizeCluster() throws Exception {
        cluster.finalizeCluster(clusterConfig);
    }
}
