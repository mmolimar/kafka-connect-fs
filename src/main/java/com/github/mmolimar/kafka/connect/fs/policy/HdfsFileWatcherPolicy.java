package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HdfsFileWatcherPolicy extends AbstractPolicy {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileWatcherPolicy.class);
    private static final String URI_PREFIX = "hdfs://";

    private static final long DEFAULT_POLL = 5000L;
    private static final long DEFAULT_RETRY = 20000L;
    private static final String HDFS_FILE_WATCHER_POLICY_PREFIX = FsSourceTaskConfig.POLICY_PREFIX + "hdfs_file_watcher.";

    public static final String HDFS_FILE_WATCHER_POLICY_POLL_MS = HDFS_FILE_WATCHER_POLICY_PREFIX + "poll";
    public static final String HDFS_FILE_WATCHER_POLICY_RETRY_MS = HDFS_FILE_WATCHER_POLICY_PREFIX + "retry";

    private final Queue<FileMetadata> fileQueue;
    private final Time time;
    private Map<FileSystem, EventStreamThread> fsEvenStream;
    private long pollSleepMs;
    private long retrySleepMs;

    public HdfsFileWatcherPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
        this.fileQueue = new ConcurrentLinkedQueue<>();
        this.time = new SystemTime();
        startWatchers();
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        try {
            this.pollSleepMs = Long.parseLong((String) customConfigs
                    .getOrDefault(HDFS_FILE_WATCHER_POLICY_POLL_MS, String.valueOf(DEFAULT_POLL)));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(HDFS_FILE_WATCHER_POLICY_POLL_MS + " property is required and must be a " +
                    "number (long). Got: " + customConfigs.get(HDFS_FILE_WATCHER_POLICY_POLL_MS));
        }
        try {
            this.retrySleepMs = Long.parseLong((String) customConfigs
                    .getOrDefault(HDFS_FILE_WATCHER_POLICY_RETRY_MS, String.valueOf(DEFAULT_RETRY)));
        } catch (NumberFormatException nfe) {
            throw new ConfigException(HDFS_FILE_WATCHER_POLICY_RETRY_MS + " property is required and must be a " +
                    "number (long). Got: " + customConfigs.get(HDFS_FILE_WATCHER_POLICY_RETRY_MS));
        }
        this.fsEvenStream = new HashMap<>();
        this.fileSystems.stream()
                .filter(fs -> fs.getWorkingDirectory().toString().startsWith(URI_PREFIX))
                .forEach(fs -> {
                    try {
                        HdfsAdmin admin = new HdfsAdmin(fs.getWorkingDirectory().toUri(), fs.getConf());
                        fsEvenStream.put(fs, new EventStreamThread(fs, admin, retrySleepMs));
                    } catch (IOException ioe) {
                        throw new ConnectException("Error creating HDFS notifications.", ioe);
                    }
                });
    }

    private void startWatchers() {
        fsEvenStream.values().forEach(Thread::start);
    }

    private void stopWatchers() {
        fsEvenStream.values().forEach(Thread::interrupt);
    }

    @Override
    public Iterator<FileMetadata> listFiles(FileSystem fs) {
        Set<FileMetadata> files = new HashSet<>();
        FileMetadata metadata;
        while ((metadata = fileQueue.poll()) != null) {
            FileMetadata fm = metadata;
            files.removeIf(f -> f.getPath().equals(fm.getPath()));
            files.add(fm);
        }
        return files.iterator();
    }

    @Override
    protected boolean isPolicyCompleted() {
        return fsEvenStream.values().stream().noneMatch(Thread::isAlive);
    }

    @Override
    public void interrupt() {
        stopWatchers();
        super.interrupt();
    }

    @Override
    public void postCheck() {
        time.sleep(pollSleepMs);
    }

    @Override
    public void close() throws IOException {
        stopWatchers();
        super.close();
    }

    private class EventStreamThread extends Thread {
        private final FileSystem fs;
        private final HdfsAdmin admin;
        private final long retrySleepMs;
        private final Time time;

        EventStreamThread(FileSystem fs, HdfsAdmin admin, long retrySleepMs) {
            this.fs = fs;
            this.admin = admin;
            this.retrySleepMs = retrySleepMs;
            this.time = new SystemTime();
        }

        @Override
        public void run() {
            while (true) {
                try {
                    DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream();
                    if (fs.getFileStatus(fs.getWorkingDirectory()) != null &&
                            fs.exists(fs.getWorkingDirectory())) {
                        EventBatch batch = eventStream.poll();
                        if (batch == null) continue;

                        Set<String> files = new HashSet<>();
                        for (Event event : batch.getEvents()) {
                            switch (event.getEventType()) {
                                case CREATE:
                                    if (!((Event.CreateEvent) event).getPath().endsWith("._COPYING_")) {
                                        files.add(((Event.CreateEvent) event).getPath());
                                    }
                                    break;
                                case APPEND:
                                    if (!((Event.AppendEvent) event).getPath().endsWith("._COPYING_")) {
                                        files.add(((Event.AppendEvent) event).getPath());
                                    }
                                    break;
                                case RENAME:
                                    if (((Event.RenameEvent) event).getSrcPath().endsWith("._COPYING_")) {
                                        files.add(((Event.RenameEvent) event).getDstPath());
                                    }
                                    break;
                                case CLOSE:
                                    if (!((Event.CloseEvent) event).getPath().endsWith("._COPYING_")) {
                                        files.add(((Event.CloseEvent) event).getPath());
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                        enqueue(files);
                    }
                } catch (IOException ioe) {
                    if (retrySleepMs > 0) {
                        time.sleep(retrySleepMs);
                    } else {
                        log.warn("{} Error watching path [{}]: {}. Stopping it...",
                                this, fs.getWorkingDirectory(), ioe.getMessage(), ioe);
                        throw new IllegalWorkerStateException(ioe);
                    }
                } catch (Exception e) {
                    log.warn("{} Stopping watcher due to an unexpected exception when watching path [{}]: {}",
                            this, fs.getWorkingDirectory(), e.getMessage(), e);
                    throw new IllegalWorkerStateException(e);
                }
            }
        }

        private void enqueue(Set<String> paths) throws IOException {
            for (String path : paths) {
                Path filePath = new Path(path);
                if (!fs.exists(filePath) || fs.getFileStatus(filePath) == null) {
                    log.info("{} Cannot enqueue file [{}] because it does not exist but got an event from the FS",
                            this, filePath);
                    return;
                }

                RemoteIterator<LocatedFileStatus> it = fs.listFiles(filePath, false);
                while (it.hasNext()) {
                    LocatedFileStatus status = it.next();
                    if (status.isFile() && fileRegexp.matcher(status.getPath().getName()).find()) {
                        FileMetadata metadata = toMetadata(status);
                        log.debug("{} Enqueuing file to process [{}].", this, metadata.getPath());
                        fileQueue.offer(metadata);
                    }
                }
            }
        }

        @Override
        public String toString() {
            return this.getClass().getSimpleName();
        }
    }
}
