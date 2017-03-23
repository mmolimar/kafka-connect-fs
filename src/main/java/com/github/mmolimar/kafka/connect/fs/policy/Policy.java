package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public interface Policy extends Closeable {

    Iterator<FileMetadata> execute() throws IOException;

    FileReader offer(FileMetadata metadata, OffsetStorageReader offsetStorageReader) throws IOException;

    boolean hasEnded();

    List<String> getURIs();

    FsSourceTaskConfig getConf();

    void interrupt();
}
