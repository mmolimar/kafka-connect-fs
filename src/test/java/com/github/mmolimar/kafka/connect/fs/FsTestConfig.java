package com.github.mmolimar.kafka.connect.fs;

import org.apache.hadoop.fs.FileSystem;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;

public interface FsTestConfig extends Closeable {

    void initFs() throws IOException;

    FileSystem getFs();

    URI getFsUri();

}
