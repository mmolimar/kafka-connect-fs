package com.github.mmolimar.kafka.connect.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

public abstract class AbstractHdfsFsConfig implements FsTestConfig {
    private MiniDFSCluster cluster;
    private FileSystem fs;
    private URI fsUri;

    @Override
    public final void initFs() throws IOException {
        Configuration clusterConfig = new Configuration();
        java.nio.file.Path hdfsDir = Files.createTempDirectory("test-");
        clusterConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.toAbsolutePath().toString());
        cluster = new MiniDFSCluster.Builder(clusterConfig).build();
        fsUri = URI.create("hdfs://localhost:" + cluster.getNameNodePort() + "/");
        fs = FileSystem.newInstance(fsUri, new Configuration());
        init();
    }

    protected abstract void init() throws IOException;

    @Override
    public FileSystem getFs() {
        return fs;
    }

    @Override
    public URI getFsUri() {
        return fsUri;
    }

    @Override
    public void close() throws IOException {
        fs.close();
        cluster.shutdown(true, true);
    }
}
