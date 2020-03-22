package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.AbstractHdfsFsConfig;
import com.github.mmolimar.kafka.connect.fs.AbstractLocalFsConfig;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.FsTestConfig;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

interface PolicyFsTestConfig extends FsTestConfig {

    Policy getPolicy();

    void setPolicy(Policy policy);

    FsSourceTaskConfig getSourceTaskConfig();

    void setSourceTaskConfig(FsSourceTaskConfig sourceTaskConfig);

    List<Path> getDirectories();

}

class LocalFsConfig extends AbstractLocalFsConfig implements PolicyFsTestConfig {
    private Policy policy;
    private FsSourceTaskConfig sourceTaskConfig;
    private List<Path> directories;

    @Override
    public void init() throws IOException {
        directories = new ArrayList<Path>() {{
            add(new Path(getFsUri().toString(), UUID.randomUUID().toString()));
            add(new Path(getFsUri().toString(), UUID.randomUUID().toString()));
        }};
        for (Path dir : directories) {
            getFs().mkdirs(dir);
        }
    }

    @Override
    public Policy getPolicy() {
        return policy;
    }

    @Override
    public void setPolicy(Policy policy) {
        this.policy = policy;
    }

    @Override
    public FsSourceTaskConfig getSourceTaskConfig() {
        return sourceTaskConfig;
    }

    @Override
    public void setSourceTaskConfig(FsSourceTaskConfig sourceTaskConfig) {
        this.sourceTaskConfig = sourceTaskConfig;
    }

    @Override
    public List<Path> getDirectories() {
        return directories;
    }

}

class HdfsFsConfig extends AbstractHdfsFsConfig implements PolicyFsTestConfig {
    private Policy policy;
    private FsSourceTaskConfig sourceTaskConfig;
    private List<Path> directories;

    @Override
    public void init() throws IOException {
        directories = new ArrayList<Path>() {{
            add(new Path(getFsUri().toString(), UUID.randomUUID().toString()));
            add(new Path(getFsUri().toString(), UUID.randomUUID().toString()));
        }};
        for (Path dir : directories) {
            getFs().mkdirs(dir);
        }
    }

    @Override
    public Policy getPolicy() {
        return policy;
    }

    @Override
    public void setPolicy(Policy policy) {
        this.policy = policy;
    }

    @Override
    public FsSourceTaskConfig getSourceTaskConfig() {
        return sourceTaskConfig;
    }

    @Override
    public void setSourceTaskConfig(FsSourceTaskConfig sourceTaskConfig) {
        this.sourceTaskConfig = sourceTaskConfig;
    }

    @Override
    public List<Path> getDirectories() {
        return directories;
    }

}
