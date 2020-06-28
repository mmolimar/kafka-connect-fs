package com.github.mmolimar.kafka.connect.fs.util;

import java.util.*;

public class Iterators {

    public static <T> Iterator<Iterator<T>> partition(Iterator<T> it, int size) {
        if (size <= 0) {
            return Collections.singletonList(it).iterator();
        }

        return new Iterator<Iterator<T>>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Iterator<T> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                List<T> elements = new ArrayList<>(size);
                while (it.hasNext() && elements.size() < size) {
                    elements.add(it.next());
                }
                return elements.iterator();
            }
        };
    }
}
