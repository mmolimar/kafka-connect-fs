package com.github.mmolimar.kafka.connect.fs.util;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

public class ReflectionUtils {

    public static FileReader makeReader(Class<? extends FileReader> clazz, FileSystem fs,
                                        Path path, Map<String, Object> config) {
        return make(clazz, fs, path, config);
    }

    public static Policy makePolicy(Class<? extends Policy> clazz, FsSourceTaskConfig conf) {
        return make(clazz, conf);
    }

    private static <T> T make(Class<T> clazz, Object... args) {
        try {
            Class<?>[] constClasses = Arrays.stream(args).map(Object::getClass).toArray(Class[]::new);
            Constructor<T> constructor = ConstructorUtils.getMatchingAccessibleConstructor(clazz, constClasses);

            return constructor.newInstance(args);
        } catch (IllegalAccessException |
                InstantiationException |
                InvocationTargetException e) {
            throw new ConnectException(e.getCause());
        }
    }
}
