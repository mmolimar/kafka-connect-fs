package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.AbstractFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import com.github.mmolimar.kafka.connect.fs.util.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FsSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(FsSourceTask.class);

    private final AtomicBoolean stop;
    private final Time time;

    private FsSourceTaskConfig config;
    private Policy policy;
    private int pollInterval;

    public FsSourceTask() {
        this.stop = new AtomicBoolean(false);
        this.time = new SystemTime();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("{} Starting FS source task...", this);
        try {
            config = new FsSourceTaskConfig(properties);
            if (config.getClass(FsSourceTaskConfig.POLICY_CLASS).isAssignableFrom(Policy.class)) {
                throw new ConfigException("Policy class " +
                        config.getClass(FsSourceTaskConfig.POLICY_CLASS) + " is not a subclass of " + Policy.class);
            }
            if (config.getClass(FsSourceTaskConfig.FILE_READER_CLASS).isAssignableFrom(FileReader.class)) {
                throw new ConfigException("FileReader class " +
                        config.getClass(FsSourceTaskConfig.FILE_READER_CLASS) + " is not a subclass of " + FileReader.class);
            }

            Class<Policy> policyClass = (Class<Policy>) Class.forName(properties.get(FsSourceTaskConfig.POLICY_CLASS));
            policy = ReflectionUtils.makePolicy(policyClass, config);
            pollInterval = config.getInt(FsSourceTaskConfig.POLL_INTERVAL_MS);
        } catch (ConfigException ce) {
            log.error("{} Couldn't start FS source task: {}", this, ce.getMessage(), ce);
            throw new ConnectException("Couldn't start FS source task due to configuration error: " + ce.getMessage(), ce);
        } catch (Exception e) {
            log.error("{} Couldn't start FS source task: {}", this, e.getMessage(), e);
            throw new ConnectException("A problem has occurred reading configuration: " + e.getMessage(), e);
        }
        log.info("{} FS source task started with policy [{}].", this, policy.getClass().getName());
    }

    @Override
    public List<SourceRecord> poll() {
        while (!stop.get() && policy != null && !policy.hasEnded()) {
            log.trace("{} Polling for new data...", this);
            Function<FileMetadata, Map<String, Object>> makePartitionKey = (FileMetadata metadata) ->
                    Collections.singletonMap("path", metadata.getPath());

            // Fetch all the offsets upfront to avoid fetching offsets once per file
            List<FileMetadata> filesToProcess = filesToProcess().collect(Collectors.toList());
            List<Map<String, Object>> partitions = filesToProcess.stream().map(makePartitionKey).collect(Collectors.toList());
            Map<Map<String, Object>, Map<String, Object>> offsets = context.offsetStorageReader().offsets(partitions);

            List<SourceRecord> totalRecords = filesToProcess.stream().map(metadata -> {
                List<SourceRecord> records = new ArrayList<>();
                Map<String, Object> partitionKey = makePartitionKey.apply(metadata);
                Map<String, Object> offset = Optional.ofNullable(offsets.get(partitionKey)).orElse(new HashMap<>());
                try (FileReader reader = policy.offer(metadata, offset)) {
                    if (reader.hasNext()) log.info("{} Processing records for file {}...", this, metadata);
                    while (reader.hasNext()) {
                        Struct record = reader.next();
                        // TODO change FileReader interface in the next major version
                        boolean hasNext = (reader instanceof AbstractFileReader) ?
                                ((AbstractFileReader) reader).hasNextBatch() || reader.hasNext() : reader.hasNext();
                        records.add(convert(metadata, reader.currentOffset(), !hasNext, record));
                    }
                } catch (IOException | ConnectException e) {
                    // when an exception happens reading a file, the connector continues
                    log.warn("{} Error reading file [{}]: {}. Keep going...",
                            this, metadata.getPath(), e.getMessage(), e);
                }
                log.debug("{} Read [{}] records from file [{}].", this, records.size(), metadata.getPath());

                return records;
            }).flatMap(Collection::stream).collect(Collectors.toList());

            log.debug("{} Returning [{}] records in execution number [{}] for policy [{}].",
                    this, totalRecords.size(), policy.getExecutions(), policy.getClass().getName());

            return totalRecords;
        }
        if (pollInterval > 0) {
            log.trace("{} Waiting [{}] ms for next poll.", this, pollInterval);
            time.sleep(pollInterval);
        }
        return null;
    }

    private Stream<FileMetadata> filesToProcess() {
        try {
            return asStream(policy.execute())
                    .filter(metadata -> metadata.getLen() > 0);
        } catch (IOException | ConnectException e) {
            // when an exception happens executing the policy, the connector continues
            log.error("{} Cannot retrieve files to process from the FS: [{}]. " +
                            "There was an error executing the policy but the task tolerates this and continues: {}",
                    this, policy.getURIs(), e.getMessage(), e);
            return Stream.empty();
        }
    }

    private <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private SourceRecord convert(FileMetadata metadata, long offset, boolean eof, Struct struct) {
        return new SourceRecord(
                Collections.singletonMap("path", metadata.getPath()),
                new HashMap<String, Object>() {{
                    put("offset", offset);
                    put("file-size", metadata.getLen());
                    put("eof", eof);
                }},
                config.getTopic(),
                struct.schema(),
                struct
        );
    }

    @Override
    public void stop() {
        log.info("{} Stopping FS source task...", this);
        stop.set(true);
        synchronized (this) {
            if (policy != null) {
                try {
                    policy.close();
                } catch (IOException ioe) {
                    log.warn("{} Error closing policy: {}", this, ioe.getMessage(), ioe);
                }
            }
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
