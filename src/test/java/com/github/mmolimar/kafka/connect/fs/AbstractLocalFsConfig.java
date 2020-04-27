package com.github.mmolimar.kafka.connect.fs;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

public abstract class AbstractLocalFsConfig implements FsTestConfig {
    private java.nio.file.Path localDir;
    private FileSystem fs;
    private URI fsUri;

    @Override
    public final void initFs() throws IOException {
        localDir = Files.createTempDirectory("test-");
        fsUri = localDir.toUri();
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
        FileUtils.deleteDirectory(localDir.toFile());
    }
}
