package com.github.mmolimar.kafka.connect.fs.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class IteratorUtils {
    public static <T> Iterator<List<T>> chunkIterator(Iterator<T> iterator, int elementsPerChunk){
        if(elementsPerChunk <= 0){
            throw new IllegalArgumentException(String.format("elementsPerChunk must be greater than 0 but was set to %d", elementsPerChunk));
        }
        return new Iterator<List<T>>() {

            public boolean hasNext() {
                return iterator.hasNext();
            }

            public List<T> next() {
                List<T> result = new ArrayList<>(elementsPerChunk);
                for (int i = 0; i < elementsPerChunk && iterator.hasNext(); i++) {
                    result.add(iterator.next());
                }
                return result;
            }
        };
    }

    public static <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
