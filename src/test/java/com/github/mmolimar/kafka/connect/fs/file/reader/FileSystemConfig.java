package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

interface FileSystemConfig extends Closeable {

    void initFs() throws IOException;

    FileSystem getFs();

    URI getFsUri();

    void setDataFile(Path dataFile);

    Path getDataFile();

    void setReader(FileReader reader);

    FileReader getReader();

    Map<Integer, Long> offsetsByIndex();

}

class LocalFsConfig implements FileSystemConfig {
    private java.nio.file.Path localDir;
    private FileSystem fs;
    private URI fsUri;
    private Path dataFile;
    private FileReader reader;
    private Map<Integer, Long> offsetsByIndex;

    @Override
    public void initFs() throws IOException {
        localDir = Files.createTempDirectory("test-");
        fsUri = localDir.toUri();
        fs = FileSystem.newInstance(fsUri, new Configuration());
        offsetsByIndex = new HashMap<>();
    }

    @Override
    public FileSystem getFs() {
        return fs;
    }

    @Override
    public URI getFsUri() {
        return fsUri;
    }

    @Override
    public void setDataFile(Path dataFile) {
        this.dataFile = dataFile;
    }

    @Override
    public Path getDataFile() {
        return dataFile;
    }

    @Override
    public void setReader(FileReader reader) {
        this.reader = reader;
    }

    @Override
    public FileReader getReader() {
        return reader;
    }

    @Override
    public Map<Integer, Long> offsetsByIndex() {
        return offsetsByIndex;
    }

    @Override
    public void close() throws IOException {
        fs.close();
        FileUtils.deleteDirectory(localDir.toFile());
    }
}

class HdfsFsConfig implements FileSystemConfig {
    private MiniDFSCluster cluster;
    private FileSystem fs;
    private URI fsUri;
    private Path dataFile;
    private FileReader reader;
    private Map<Integer, Long> offsetsByIndex;

    @Override
    public void initFs() throws IOException {
        Configuration clusterConfig = new Configuration();
        java.nio.file.Path hdfsDir = Files.createTempDirectory("test-");
        clusterConfig.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.toAbsolutePath().toString());
        cluster = new MiniDFSCluster.Builder(clusterConfig).build();
        fsUri = URI.create("hdfs://localhost:" + cluster.getNameNodePort() + "/");
        fs = FileSystem.newInstance(fsUri, new Configuration());
        offsetsByIndex = new HashMap<>();
    }

    @Override
    public FileSystem getFs() {
        return fs;
    }

    @Override
    public URI getFsUri() {
        return fsUri;
    }

    @Override
    public Path getDataFile() {
        return dataFile;
    }

    @Override
    public void setDataFile(Path dataFile) {
        this.dataFile = dataFile;
    }

    @Override
    public void setReader(FileReader reader) {
        this.reader = reader;
    }

    @Override
    public FileReader getReader() {
        return reader;
    }

    @Override
    public Map<Integer, Long> offsetsByIndex() {
        return offsetsByIndex;
    }

    @Override
    public void close() throws IOException {
        fs.close();
        cluster.shutdown(true);
    }
}
