package com.github.mmolimar.kafka.connect.fs.file;

import java.util.List;
import java.util.Optional;

public class FileMetadata {
    private String path;
    private long length;
    private List<BlockInfo> blocks;

    public FileMetadata(String path, long length, List<BlockInfo> blocks) {
        this.path = Optional.ofNullable(path).orElse("");
        this.length = length;
        this.blocks = blocks;
    }

    public String getPath() {
        return path;
    }

    public long getLen() {
        return length;
    }

    public List<BlockInfo> getBlocks() {
        return blocks;
    }

    @Override
    public String toString() {
        return String.format("[path = %s, length = %s, blocks = %s]", path, length, blocks);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (!(object instanceof FileMetadata)) return false;

        FileMetadata metadata = (FileMetadata) object;
        return this.path.equals(metadata.getPath()) &&
                this.length == metadata.length &&
                this.blocks.equals(metadata.getBlocks());
    }

    public int hashCode() {
        return path.hashCode();
    }


    public static class BlockInfo {
        private long offset;
        private long length;
        private boolean corrupt;

        public BlockInfo(long offset, long length, boolean corrupt) {
            this.offset = offset;
            this.length = length;
            this.corrupt = corrupt;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (!(object instanceof BlockInfo)) return false;

            BlockInfo blockInfo = (BlockInfo) object;
            return this.offset == blockInfo.offset &&
                    this.length == blockInfo.length &&
                    this.corrupt == blockInfo.corrupt;
        }

        @Override
        public String toString() {
            return String.format("[offset = %s, length = %s, corrupt = %s]", offset, length, corrupt);
        }
    }
}
