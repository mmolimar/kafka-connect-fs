package com.github.mmolimar.kafka.connect.fs.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.mmolimar.kafka.connect.fs.util.IteratorUtils.asStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IteratorUtilsTest {

    @Test
    public void testIteratorChunkingWorks() {
        Iterator<Integer> iterator = IntStream.rangeClosed(0, 9).boxed().iterator();
        Iterator<List<Integer>> chunkedIterator = IteratorUtils.chunkIterator(iterator, 2);

        List<List<Integer>> materializedChunkedIterator  = asStream(chunkedIterator).collect(Collectors.toList());
        ArrayList<ArrayList<Integer>> expected = new ArrayList<ArrayList<Integer>>(){{
            add(new ArrayList<Integer>(){{
                add(0); add(1);
            }});
            add(new ArrayList<Integer>(){{
                add(2); add(3);
            }});
            add(new ArrayList<Integer>(){{
                add(4); add(5);
            }});
            add(new ArrayList<Integer>(){{
                add(6); add(7);
            }});
            add(new ArrayList<Integer>(){{
                add(8); add(9);
            }});
        }};

        assertEquals(expected, materializedChunkedIterator);
    }

    @Test
    public void testIteratorChunkingWorksWithUnevenChunks() {
        Iterator<Integer> iterator = IntStream.rangeClosed(0, 4).boxed().iterator();
        Iterator<List<Integer>> chunkedIterator = IteratorUtils.chunkIterator(iterator, 2);

        List<List<Integer>> materializedChunkedIterator  = asStream(chunkedIterator).collect(Collectors.toList());
        ArrayList<ArrayList<Integer>> expected = new ArrayList<ArrayList<Integer>>(){{
            add(new ArrayList<Integer>(){{
                add(0); add(1);
            }});
            add(new ArrayList<Integer>(){{
                add(2); add(3);
            }});
            add(new ArrayList<Integer>(){{
                add(4);
            }});
        }};

        assertEquals(expected, materializedChunkedIterator);
    }

    @Test
    public void testIteratorChunkingWithChunkGreaterThanNumElementsWorks() {
        Iterator<Integer> iterator = IntStream.rangeClosed(0, 2).boxed().iterator();
        Iterator<List<Integer>> chunkedIterator = IteratorUtils.chunkIterator(iterator, 5);

        List<List<Integer>> materializedChunkedIterator  = asStream(chunkedIterator).collect(Collectors.toList());
        ArrayList<ArrayList<Integer>> expected = new ArrayList<ArrayList<Integer>>(){{
            add(new ArrayList<Integer>(){{
                add(0); add(1); add(2);
            }});
        }};

        assertEquals(expected, materializedChunkedIterator);
    }

    @Test
    public void testIteratorChunkingThrowsWithInvalidChunkSize() {
        Iterator<Integer> iterator = IntStream.rangeClosed(0, 2).boxed().iterator();

        assertThrows(IllegalArgumentException.class, () -> {
            IteratorUtils.chunkIterator(iterator, 0);
        });

    }
}
