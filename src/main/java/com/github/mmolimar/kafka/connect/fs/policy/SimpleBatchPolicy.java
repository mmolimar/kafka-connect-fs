package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SimpleBatchPolicy extends AbstractPolicy {

    private static final Logger log = LoggerFactory.getLogger(SimpleBatchPolicy.class);

    private static final int DEFAULT_BATCH_SIZE = 200;

    private static final String BATCH_POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX + "simple_batch.";
    public static final String BATCH_POLICY_BATCH_SIZE = BATCH_POLICY_PREFIX + "batch_size";

    private int batchSize;
    private Map<FileSystem, Iterator<FileMetadata>> innerIterators = new HashMap<>();

    public SimpleBatchPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        try {
            this.batchSize = Integer.parseInt(
                    (String) customConfigs.getOrDefault(BATCH_POLICY_BATCH_SIZE, String.valueOf(DEFAULT_BATCH_SIZE)));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(BATCH_POLICY_BATCH_SIZE + " property is required and must be a "
                    + "number (int). Got: " + customConfigs.get(BATCH_POLICY_BATCH_SIZE));
        }
    }

    @Override
    public Iterator<FileMetadata> listFiles(final FileSystem fs) throws IOException {
        if (!innerIterators.containsKey(fs)) {
            innerIterators.put(fs, super.listFiles(fs));
        }

        return new Iterator<FileMetadata>() {
            private int currentFileIndex = 0;
            private Iterator<FileMetadata> iterator = innerIterators.get(fs);
            
            @Override
            public boolean hasNext() {
                log.debug("Current file index is {}. Batch size is {}.", currentFileIndex, batchSize);
                return (currentFileIndex < batchSize) && iterator.hasNext();
            }

            @Override
            public FileMetadata next() {
                FileMetadata metadata = iterator.next();
                currentFileIndex++;
                return metadata;
            }
        };
    }

    @Override
    protected boolean isPolicyCompleted() {
        if (innerIterators.size() == 0)
            return false;

        for (Iterator<FileMetadata> iterator : innerIterators.values()) {
            if(iterator.hasNext())
                return false;
        }
        
        return true;
    }
}
