package com.github.mmolimar.kafka.connect.fs.util;

import java.util.stream.Stream;

@FunctionalInterface
public interface TailCall<T> {

    TailCall<T> apply();

    default boolean completed() {
        return false;
    }

    default T result() {
        throw new IllegalStateException("Call does not have a value.");
    }

    default T invoke() {
        return Stream.iterate(this, TailCall::apply)
                .filter(TailCall::completed)
                .findFirst()
                .get()
                .result();
    }

    static <T> TailCall<T> done(final T value) {
        return new TailCall<T>() {
            @Override
            public boolean completed() {
                return true;
            }

            @Override
            public T result() {
                return value;
            }

            @Override
            public TailCall<T> apply() {
                throw new IllegalStateException("Done cannot be applied.");
            }
        };
    }
}
