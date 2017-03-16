package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.Offset;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

abstract class AbstractPolicy implements Policy {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final FsSourceTaskConfig conf;
    private final List<FileSystem> fileSystems;
    private final AtomicInteger executions;
    private final boolean recursive;
    private final Pattern fileRegexp;
    private boolean interrupted;

    public AbstractPolicy(FsSourceTaskConfig conf) throws IOException {
        this.fileSystems = new ArrayList<>();
        this.conf = conf;
        this.executions = new AtomicInteger(0);
        this.recursive = conf.getBoolean(FsSourceTaskConfig.POLICY_RECURSIVE);
        this.fileRegexp = Pattern.compile(conf.getString(FsSourceTaskConfig.FILE_REGEXP));
        this.interrupted = false;

        Map<String, Object> customConfigs = customConfigs();
        logAll(customConfigs);
        configFs(customConfigs);
        configPolicy(customConfigs);
    }

    private Map<String, Object> customConfigs() {
        return conf.originals().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.POLICY_PREFIX_CUSTOM))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));
    }

    private void configFs(Map<String, Object> customConfigs) throws IOException {
        for (String uri : this.conf.getFsUris()) {
            Configuration fsConfig = new Configuration();
            customConfigs.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.POLICY_PREFIX_FS))
                    .forEach(entry -> fsConfig.set(entry.getKey().replace(FsSourceTaskConfig.POLICY_PREFIX_FS, ""),
                            (String) entry.getValue()));

            FileSystem fs = FileSystem.get(URI.create(uri), fsConfig);
            fs.setWorkingDirectory(new Path(uri));
            this.fileSystems.add(fs);
        }
    }

    protected abstract void configPolicy(Map<String, Object> customConfigs);

    @Override
    public FsSourceTaskConfig getConf() {
        return conf;
    }

    @Override
    public final Iterator<FileMetadata> execute() throws IOException {
        if (hasEnded()) {
            throw new IllegalWorkerStateException("Policy has ended. Cannot be retried");
        }
        preCheck();

        Iterator<FileMetadata> files = Collections.emptyIterator();
        for (FileSystem fs : fileSystems) {
            files = concat(files, listFiles(fs));
        }
        executions.incrementAndGet();

        postCheck();

        return files;
    }

    @Override
    public void interrupt() {
        interrupted = true;
    }

    protected void preCheck() {
    }

    protected void postCheck() {
    }

    public Iterator<FileMetadata> listFiles(FileSystem fs) throws IOException {
        return new Iterator<FileMetadata>() {
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(fs.getWorkingDirectory(), recursive);
            LocatedFileStatus current = null;
            boolean previous = false;

            @Override
            public boolean hasNext() {
                try {
                    if (current == null) {
                        if (!it.hasNext()) return false;
                        current = it.next();
                        return hasNext();
                    }
                    if (current.isFile() &&
                            fileRegexp.matcher(current.getPath().getName()).find()) {
                        return true;
                    }
                    current = null;
                    return hasNext();
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            }

            @Override
            public FileMetadata next() {
                if (!hasNext() && current == null) {
                    throw new NoSuchElementException("There are no more items");
                }
                FileMetadata metadata = toMetadata(current);
                current = null;
                return metadata;
            }
        };
    }

    @Override
    public final boolean hasEnded() {
        return interrupted || isPolicyCompleted();
    }

    protected abstract boolean isPolicyCompleted();

    public final int getExecutions() {
        return executions.get();
    }

    protected FileMetadata toMetadata(LocatedFileStatus fileStatus) {
        List<FileMetadata.BlockInfo> blocks = new ArrayList<>();

        blocks.addAll(Arrays.stream(fileStatus.getBlockLocations())
                .map(block ->
                        new FileMetadata.BlockInfo(block.getOffset(), block.getLength(), block.isCorrupt()))
                .collect(Collectors.toList()));

        return new FileMetadata(fileStatus.getPath().toString(), fileStatus.getLen(), blocks);
    }

    @Override
    public FileReader offer(FileMetadata metadata, OffsetStorageReader offsetStorageReader) throws IOException {
        Map<String, Object> partition = new HashMap<>();
        partition.put("path", metadata.getPath());

        FileSystem current = fileSystems.stream()
                .filter(fs -> metadata.getPath().startsWith(fs.getWorkingDirectory().toString()))
                .findFirst().orElse(null);

        FileReader reader;
        try {
            reader = ReflectionUtils.makeReader((Class<? extends FileReader>) conf.getClass(FsSourceTaskConfig.FILE_READER_CLASS),
                    current, new Path(metadata.getPath()), conf.originals());
        } catch (Throwable t) {
            throw new ConnectException("An error has ocurred when creating reader for file: " + metadata.getPath(), t);
        }

        Map<String, Object> offset = offsetStorageReader.offset(partition);
        if (offset != null && offset.get("offset") != null) {
            reader.seek((Offset) offset.get("offset"));
        }
        return reader;
    }

    Iterator<FileMetadata> concat(final Iterator<FileMetadata> it1,
                                  final Iterator<FileMetadata> it2) {
        return new Iterator<FileMetadata>() {

            @Override
            public boolean hasNext() {
                return it1.hasNext() || it2.hasNext();
            }

            @Override
            public FileMetadata next() {
                return it1.hasNext() ? it1.next() : it2.next();
            }
        };
    }

    @Override
    public void close() throws IOException {
        for (FileSystem fs : fileSystems) {
            fs.close();
        }
    }

    private void logAll(Map<String, Object> conf) {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName());
        b.append(" values: ");
        b.append(Utils.NL);
        for (Map.Entry<String, Object> entry : conf.entrySet()) {
            b.append('\t');
            b.append(entry.getKey());
            b.append(" = ");
            b.append(entry.getValue());
            b.append(Utils.NL);
        }
        log.info(b.toString());
    }
}
