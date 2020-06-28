package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;

import java.io.Closeable;
import java.util.Iterator;
import java.util.function.Function;

public interface FileReader extends Iterator<Struct>, Closeable {

    Path getFilePath();

    Struct next();

    void seek(long offset);

    long currentOffset();
}

@FunctionalInterface
interface ReaderAdapter<T> extends Function<T, Struct> {

}
