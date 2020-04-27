package com.github.mmolimar.kafka.connect.fs.file.reader;

public enum CompressionType {
    BZIP2,
    GZIP,
    NONE;

    private boolean concatenated;

    CompressionType() {
        this.concatenated = true;
    }

    public boolean isConcatenated() {
        return concatenated;
    }

    public static CompressionType fromName(String compression, boolean concatenated) {
        CompressionType ct = CompressionType.valueOf(compression.trim().toUpperCase());
        ct.concatenated = concatenated;
        return ct;
    }
}
