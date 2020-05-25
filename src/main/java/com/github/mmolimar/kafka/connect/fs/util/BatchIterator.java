package com.github.mmolimar.kafka.connect.fs.util;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchIterator {
    private static final Logger log = LoggerFactory.getLogger(BatchIterator.class);
    
    public static <T> Iterator<T> batchIterator(Iterator<T> iterator, long elementsPerBatch) {
        return new Iterator<T>() {
            private long count = 0;
            
            @Override
            public boolean hasNext() {
                log.debug("Current index is {}. Batch size is {}.", count, elementsPerBatch);
                return (count < elementsPerBatch) && iterator.hasNext();
            }

            @Override
            public T next() {
                T next = iterator.next();
                count++;
                return next;
            }
        };
    }
}
