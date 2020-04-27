package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.github.mmolimar.kafka.connect.fs.AbstractHdfsFsConfig;
import com.github.mmolimar.kafka.connect.fs.AbstractLocalFsConfig;
import com.github.mmolimar.kafka.connect.fs.FsTestConfig;
import org.apache.hadoop.fs.Path;

import java.util.HashMap;
import java.util.Map;

interface ReaderFsTestConfig extends FsTestConfig {

    void setDataFile(Path dataFile);

    Path getDataFile();

    void setReader(FileReader reader);

    FileReader getReader();

    Map<Integer, Long> offsetsByIndex();

}

class LocalFsConfig extends AbstractLocalFsConfig implements ReaderFsTestConfig {
    private Path dataFile;
    private FileReader reader;
    private Map<Integer, Long> offsetsByIndex;

    @Override
    public void init() {
        offsetsByIndex = new HashMap<>();
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

}

class HdfsFsConfig extends AbstractHdfsFsConfig implements ReaderFsTestConfig {
    private Path dataFile;
    private FileReader reader;
    private Map<Integer, Long> offsetsByIndex;

    @Override
    public void init() {
        offsetsByIndex = new HashMap<>();
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

}
