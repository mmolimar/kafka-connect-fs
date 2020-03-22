package com.github.mmolimar.kafka.connect.fs.task;

import com.github.mmolimar.kafka.connect.fs.AbstractHdfsFsConfig;
import com.github.mmolimar.kafka.connect.fs.AbstractLocalFsConfig;
import com.github.mmolimar.kafka.connect.fs.FsSourceTask;
import com.github.mmolimar.kafka.connect.fs.FsTestConfig;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

interface TaskFsTestConfig extends FsTestConfig {

    FsSourceTask getTask();

    void setTask(FsSourceTask task);

    Map<String, String> getTaskConfig();

    void setTaskConfig(Map<String, String> taskConfig);

    List<Path> getDirectories();

}

class LocalFsConfig extends AbstractLocalFsConfig implements TaskFsTestConfig {
    private FsSourceTask task;
    private Map<String, String> taskConfig;
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
    public FsSourceTask getTask() {
        return task;
    }

    @Override
    public void setTask(FsSourceTask task) {
        this.task = task;
    }

    @Override
    public Map<String, String> getTaskConfig() {
        return taskConfig;
    }

    @Override
    public void setTaskConfig(Map<String, String> taskConfig) {
        this.taskConfig = taskConfig;
    }

    @Override
    public List<Path> getDirectories() {
        return directories;
    }

}

class HdfsFsConfig extends AbstractHdfsFsConfig implements TaskFsTestConfig {
    private FsSourceTask task;
    private Map<String, String> taskConfig;
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
    public FsSourceTask getTask() {
        return task;
    }

    @Override
    public void setTask(FsSourceTask task) {
        this.task = task;
    }

    @Override
    public Map<String, String> getTaskConfig() {
        return taskConfig;
    }

    @Override
    public void setTaskConfig(Map<String, String> taskConfig) {
        this.taskConfig = taskConfig;
    }

    @Override
    public List<Path> getDirectories() {
        return directories;
    }

}
