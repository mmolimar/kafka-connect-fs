package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
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

    private final AtomicBoolean stop = new AtomicBoolean(false);
    private FsSourceTaskConfig config;
    private Policy policy;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting FS source task...");
        try {
            config = new FsSourceTaskConfig(properties);
            if (config.getClass(FsSourceTaskConfig.POLICY_CLASS).isAssignableFrom(Policy.class)) {
                throw new ConfigException("Policy class " +
                        config.getClass(FsSourceTaskConfig.POLICY_CLASS) + "is not a subclass of " + Policy.class);
            }
            if (config.getClass(FsSourceTaskConfig.FILE_READER_CLASS).isAssignableFrom(FileReader.class)) {
                throw new ConfigException("FileReader class " +
                        config.getClass(FsSourceTaskConfig.FILE_READER_CLASS) + "is not a subclass of " + FileReader.class);
            }

            Class<Policy> policyClass = (Class<Policy>) Class.forName(properties.get(FsSourceTaskConfig.POLICY_CLASS));
            FsSourceTaskConfig taskConfig = new FsSourceTaskConfig(properties);
            policy = ReflectionUtils.makePolicy(policyClass, taskConfig);
        } catch (ConfigException ce) {
            log.error("Couldn't start FsSourceTask:", ce);
            throw new ConnectException("Couldn't start FsSourceTask due to configuration error", ce);
        } catch (Exception e) {
            log.error("Couldn't start FsSourceConnector:", e);
            throw new ConnectException("A problem has occurred reading configuration: " + e.getMessage());
        }
        log.info("FS source task started with policy {}", policy.getClass().getName());
    }

    @Override
    public List<SourceRecord> poll() {
        while (!stop.get() && policy != null && !policy.hasEnded()) {
            log.trace("Polling for new data");

            return filesToProcess().map(metadata -> {
                List<SourceRecord> records = new ArrayList<>();
                try (FileReader reader = policy.offer(metadata, context.offsetStorageReader())) {
                    log.info("Processing records for file {}", metadata);
                    while (reader.hasNext()) {
                        records.add(convert(metadata, reader.currentOffset(), reader.next()));
                    }
                } catch (ConnectException | IOException e) {
                    //when an exception happens reading a file, the connector continues
                    log.error("Error reading file from FS: " + metadata.getPath() + ". Keep going...", e);
                }
                return records;
            }).flatMap(Collection::stream).collect(Collectors.toList());
        }
        return null;
    }

    private Stream<FileMetadata> filesToProcess() {
        try {
            return asStream(policy.execute())
                    .filter(metadata -> metadata.getLen() > 0)
                    .collect(Collectors.toList())
                    .stream();
        } catch (IOException | ConnectException e) {
            //when an exception happens executing the policy, the connector continues
            log.error("Cannot retrieve files to process from the FS: {}. " +
                    "There was an error executing the policy but the task tolerates this and continues. " +
                    e.getMessage(), policy.getURIs(), e);
            return Stream.empty();
        }
    }

    private <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private SourceRecord convert(FileMetadata metadata, long offset, Struct struct) {
        return new SourceRecord(
                Collections.singletonMap("path", metadata.getPath()),
                Collections.singletonMap("offset", offset),
                config.getTopic(),
                struct.schema(),
                struct
        );
    }

    @Override
    public void stop() {
        log.info("Stopping FS source task.");
        stop.set(true);
        if (policy != null) {
            policy.interrupt();
        }
    }
}
