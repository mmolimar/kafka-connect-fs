package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import com.github.mmolimar.kafka.connect.fs.util.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FsSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FsSourceTask.class);

    private final AtomicBoolean stop;
    private FsSourceTaskConfig config;
    private Policy policy;

    public FsSourceTask() {
        this.stop = new AtomicBoolean(true);
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            config = new FsSourceTaskConfig(properties);

            if (config.getClass(FsSourceTaskConfig.POLICY_CLASS).isAssignableFrom(Policy.class)) {
                throw new ConfigException("Policy class " +
                        config.getClass(FsSourceTaskConfig.POLICY_CLASS) + "is not a sublass of " + Policy.class);
            }
            if (config.getClass(FsSourceTaskConfig.FILE_READER_CLASS).isAssignableFrom(FileReader.class)) {
                throw new ConfigException("FileReader class " +
                        config.getClass(FsSourceTaskConfig.FILE_READER_CLASS) + "is not a sublass of " + FileReader.class);
            }

            Class<Policy> policyClass = (Class<Policy>) Class.forName(properties.get(FsSourceTaskConfig.POLICY_CLASS));
            FsSourceTaskConfig taskConfig = new FsSourceTaskConfig(properties);
            policy = ReflectionUtils.makePolicy(policyClass, taskConfig);
        } catch (ConfigException ce) {
            log.error("Couldn't start FsSourceTask:", ce);
            throw new ConnectException("Couldn't start FsSourceTask due to configuration error", ce);
        } catch (Throwable t) {
            log.error("Couldn't start FsSourceConnector:", t);
            throw new ConnectException("A problem has ocurred reading configuration:" + t.getMessage());
        }

        stop.set(false);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.info("Polling for new data");

        while (!stop.get() && !policy.hasEnded()) {

            final List<SourceRecord> results = new ArrayList<>();
            List<FileMetadata> files = filesToProcess();
            files.forEach(metadata -> {
                try {
                    log.info("Processing records for file {}", metadata);
                    FileReader reader = policy.offer(metadata, context.offsetStorageReader());
                    while (reader.hasNext()) {
                        results.add(convert(metadata, reader.currentOffset(), reader.next()));
                    }
                } catch (IOException ioe) {
                    throw new ConnectException(ioe);
                }
            });
            return results;
        }

        return null;
    }

    private List<FileMetadata> filesToProcess() {
        try {
            return asStream(policy.execute())
                    .filter(metadata -> metadata.getLen() > 0)
                    .collect(Collectors.toList());
        } catch (IOException ioe) {
            throw new ConnectException(ioe);
        }
    }

    private <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private SourceRecord convert(FileMetadata metadata, Offset offset, Struct struct) {
        return new SourceRecord(
                new HashMap<String, Object>() {
                    {
                        put("path", metadata.getPath());
                        put("blocks", metadata.getBlocks());
                    }
                },
                Collections.singletonMap("offset", offset),
                struct.schema().name(),
                struct.schema(),
                struct
        );
    }

    @Override
    public void stop() {
        stop.set(true);
    }
}