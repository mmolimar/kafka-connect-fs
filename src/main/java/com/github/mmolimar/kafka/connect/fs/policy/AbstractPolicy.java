package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.EmptyFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import com.github.mmolimar.kafka.connect.fs.util.TailCall;
import com.google.common.collect.Iterators;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

abstract class AbstractPolicy implements Policy {

    private final Logger log = LoggerFactory.getLogger(getClass());

    protected final List<FileSystem> fileSystems;
    protected final Pattern fileRegexp;

    private final FsSourceTaskConfig conf;
    private final AtomicLong executions;
    private final boolean recursive;
    private final int batchSize;
    private Iterator<List<FileMetadata>> previous;
    private boolean interrupted;

    public AbstractPolicy(FsSourceTaskConfig conf) throws IOException {
        this.fileSystems = new ArrayList<>();
        this.conf = conf;
        this.executions = new AtomicLong(0);
        this.recursive = conf.getBoolean(FsSourceTaskConfig.POLICY_RECURSIVE);
        this.fileRegexp = Pattern.compile(conf.getString(FsSourceTaskConfig.POLICY_REGEXP));
        this.batchSize = conf.getInt(FsSourceTaskConfig.POLICY_BATCH_SIZE);
        this.interrupted = false;
        this.previous = Collections.emptyIterator();

        Map<String, Object> customConfigs = customConfigs();
        logAll(customConfigs);
        configFs(customConfigs);
        configPolicy(customConfigs);
    }

    private Map<String, Object> customConfigs() {
        return conf.originals().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.POLICY_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void configFs(Map<String, Object> customConfigs) throws IOException {
        for (String uri : this.conf.getFsUris()) {
            Configuration fsConfig = new Configuration();
            customConfigs.entrySet().stream()
                    .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.POLICY_PREFIX_FS))
                    .forEach(entry -> fsConfig.set(entry.getKey().replace(FsSourceTaskConfig.POLICY_PREFIX_FS, ""),
                            (String) entry.getValue()));

            Path workingDir = new Path(convert(uri));
            FileSystem fs = FileSystem.newInstance(workingDir.toUri(), fsConfig);
            fs.setWorkingDirectory(workingDir);
            this.fileSystems.add(fs);
        }
    }

    private String convert(String uri) {
        String converted = uri;
        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter formatter;

        Pattern pattern = Pattern.compile("\\$\\{([a-zA-Z]+)}");
        Matcher matcher = pattern.matcher(uri);
        while (matcher.find()) {
            try {
                formatter = DateTimeFormatter.ofPattern(matcher.group(1));
                converted = converted.replaceAll("\\$\\{" + matcher.group(1) + "}", dateTime.format(formatter));
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot convert dynamic URI: " + matcher.group(1), e);
            }
        }
        return converted;
    }

    protected abstract void configPolicy(Map<String, Object> customConfigs);

    @Override
    public List<String> getURIs() {
        List<String> uris = new ArrayList<>();
        fileSystems.forEach(fs -> uris.add(fs.getWorkingDirectory().toString()));
        return uris;
    }

    @Override
    public final Iterator<FileMetadata> execute() throws IOException {
        if (hasEnded()) {
            throw new IllegalWorkerStateException("Policy has ended. Cannot be retried.");
        }

        if (batchSize > 0 && previous.hasNext()) {
            return previous.next().iterator();
        }

        preCheck();

        executions.incrementAndGet();
        Iterator<FileMetadata> files = Collections.emptyIterator();
        for (FileSystem fs : fileSystems) {
            files = concat(files, listFiles(fs));
        }
        postCheck();

        if (batchSize > 0) {
            previous = Iterators.partition(files, batchSize);
            if (!previous.hasNext())
                return Collections.emptyIterator();
            return previous.next().iterator();
        }

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

            private TailCall<Boolean> hasNextRec() {
                try {
                    if (current == null) {
                        if (!it.hasNext()) {
                            return TailCall.done(false);
                        }
                        current = it.next();
                        return this::hasNextRec;
                    }
                    if (current.isFile() && fileRegexp.matcher(current.getPath().getName()).find()) {
                        return TailCall.done(true);
                    }
                    current = null;
                    return this::hasNextRec;
                } catch (IOException ioe) {
                    throw new ConnectException(ioe);
                }
            }

            @Override
            public boolean hasNext() {
                return hasNextRec().invoke();
            }

            @Override
            public FileMetadata next() {
                if (!hasNext() && current == null) {
                    throw new NoSuchElementException("There are no more items.");
                }
                FileMetadata metadata = toMetadata(current);
                current = null;
                return metadata;
            }
        };
    }

    @Override
    public final boolean hasEnded() {
        if (interrupted) {
            return true;
        }

        return !previous.hasNext() && isPolicyCompleted();
    }

    protected abstract boolean isPolicyCompleted();

    public final long getExecutions() {
        return executions.get();
    }

    FileMetadata toMetadata(LocatedFileStatus fileStatus) {

        List<FileMetadata.BlockInfo> blocks = Arrays.stream(fileStatus.getBlockLocations())
                .map(block -> new FileMetadata.BlockInfo(block.getOffset(), block.getLength(), block.isCorrupt()))
                .collect(Collectors.toList());

        return new FileMetadata(fileStatus.getPath().toString(), fileStatus.getLen(), blocks);
    }

    @Override
    public FileReader offer(FileMetadata metadata, OffsetStorageReader offsetStorageReader) {
        FileSystem current = fileSystems.stream()
                .filter(fs -> metadata.getPath().startsWith(fs.getWorkingDirectory().toString()))
                .findFirst()
                .orElse(null);

        Supplier<FileReader> makeReader = () -> ReflectionUtils.makeReader(
                (Class<? extends FileReader>) conf.getClass(FsSourceTaskConfig.FILE_READER_CLASS),
                current, new Path(metadata.getPath()), conf.originals()
        );
        try {
            Map<String, Object> partition = Collections.singletonMap("path", metadata.getPath());
            Map<String, Object> offset = offsetStorageReader.offset(partition);
            if (offset != null && offset.get("offset") != null) {
                Object offsetObject = offset.get("offset");
                Object fileSizeBytesObject = offset.get("fileSizeBytes");

                // Only new versions of kafka-connect-fs store the file size bytes
                // If we have the byte offset, we can skip reading the entire file if the file size has not changed
                if (fileSizeBytesObject != null) {
                    Long byteOffset = (Long) fileSizeBytesObject;
                    if (metadata.getLen() == byteOffset) {
                        log.info("Skipping file [{}] due to it was already processed.", metadata.getPath());
                        return new EmptyFileReader(current, new Path(metadata.getPath()), conf.originals());
                    }
                }

                log.info("Seeking to offset [{}] for file [{}].", offset.get("offset"), metadata.getPath());
                FileReader reader = makeReader.get();
                reader.seek((Long) offsetObject);
                return reader;
            }
            return makeReader.get();
        } catch (Exception e) {
            throw new ConnectException("An error has occurred when creating reader for file: " + metadata.getPath(), e);
        }
    }

    private Iterator<FileMetadata> concat(final Iterator<FileMetadata> it1, final Iterator<FileMetadata> it2) {
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
