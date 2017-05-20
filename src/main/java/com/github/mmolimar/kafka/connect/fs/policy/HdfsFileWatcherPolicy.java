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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HdfsFileWatcherPolicy extends AbstractPolicy {

    private static final Logger log = LoggerFactory.getLogger(HdfsFileWatcherPolicy.class);
    private static final String URI_PREFIX = "hdfs://";

    private final Queue<FileMetadata> fileQueue;
    private Map<FileSystem, EventStreamThread> fsEvenStream;

    public HdfsFileWatcherPolicy(FsSourceTaskConfig conf) throws IOException {
        super(conf);
        this.fileQueue = new ConcurrentLinkedQueue();
        startWatchers();
    }

    @Override
    protected void configPolicy(Map<String, Object> customConfigs) {
        this.fsEvenStream = new HashMap<>();
        fileSystems.stream()
                .filter(fs -> fs.getWorkingDirectory().toString().startsWith(URI_PREFIX))
                .forEach(fs -> {
                    try {
                        HdfsAdmin admin = new HdfsAdmin(fs.getWorkingDirectory().toUri(), fs.getConf());
                        fsEvenStream.put(fs, new EventStreamThread(fs, admin));
                    } catch (IOException ioe) {
                        throw new ConnectException("Error creating admin for notifications", ioe);
                    }
                });
    }

    private void startWatchers() {
        fsEvenStream.values().forEach(stream -> stream.start());
    }

    private void stopWatchers() {
        fsEvenStream.values().forEach(stream -> stream.interrupt());
    }

    @Override
    public Iterator<FileMetadata> listFiles(FileSystem fs) throws IOException {
        Set<FileMetadata> files = new HashSet<>();
        FileMetadata metadata;
        while ((metadata = fileQueue.poll()) != null) {
            files.add(metadata);
        }
        return files.iterator();
    }

    @Override
    protected boolean isPolicyCompleted() {
        boolean hasRunningThreads = false;
        for (EventStreamThread thread : fsEvenStream.values()) {
            if (thread.isAlive()) {
                hasRunningThreads = true;
                break;
            }
        }
        return !hasRunningThreads;
    }

    @Override
    public void interrupt() {
        stopWatchers();
        super.interrupt();
    }

    @Override
    public void close() throws IOException {
        stopWatchers();
        super.close();
    }

    private class EventStreamThread extends Thread {
        private final FileSystem fs;
        private final HdfsAdmin admin;

        protected EventStreamThread(FileSystem fs, HdfsAdmin admin) {
            this.fs = fs;
            this.admin = admin;
        }

        @Override
        public void run() {
            try {
                DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream();
                while (fs.getFileStatus(fs.getWorkingDirectory()) != null &&
                        fs.exists(fs.getWorkingDirectory())) {
                    EventBatch batch = eventStream.poll();
                    if (batch == null) continue;

                    for (Event event : batch.getEvents()) {
                        switch (event.getEventType()) {
                            case CREATE:
                                enqueue(((Event.CreateEvent) event).getPath());
                                break;
                            case APPEND:
                                enqueue(((Event.AppendEvent) event).getPath());
                                break;
                            case CLOSE:
                                enqueue(((Event.CloseEvent) event).getPath());
                                break;
                            default:
                                break;
                        }
                    }
                }
            } catch (FileNotFoundException fnfe) {
                log.warn("Cannot find file in this FS {}. Stopping watcher...", fs.getWorkingDirectory(), fnfe);
            } catch (IOException ioe) {
                log.info("An interrupted exception has occurred. Path {} is not watched any more", fs.getWorkingDirectory());
            } catch (Exception ioe) {
                log.warn("Exception watching path {}", fs.getWorkingDirectory(), ioe);
                throw new IllegalWorkerStateException(ioe);
            }
        }

        private void enqueue(String path) throws IOException {
            Path filePath = new Path(path);
            if (!fs.exists(filePath) || fs.getFileStatus(filePath) == null) {
                log.info("Cannot enqueue file {} because it does not exist but got an event from the FS", filePath.toString());
                return;
            }

            RemoteIterator<LocatedFileStatus> it = fs.listFiles(filePath, false);
            while (it.hasNext()) {
                LocatedFileStatus status = it.next();
                if (!status.isFile() || !fileRegexp.matcher(status.getPath().getName()).find()) continue;
                fileQueue.offer(toMetadata(status));
            }
        }
    }
}

