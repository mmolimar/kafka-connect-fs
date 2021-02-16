package com.github.mmolimar.kafka.connect.fs.file.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.Map;

public class BinaryFileReader extends AbstractFileReader<BinaryFileReader.BinaryRecord> {

    private static final String FIELD_PATH = "path";
    private static final String FIELD_OWNER = "owner";
    private static final String FIELD_GROUP = "group";
    private static final String FIELD_LENGTH = "length";
    private static final String FIELD_ACCESS_TIME = "access_time";
    private static final String FIELD_MODIFICATION_TIME = "modification_time";
    private static final String FIELD_CONTENT = "content";

    protected static final int NUM_RECORDS = 1;

    private final FileStatus fileStatus;
    private final Schema schema;

    private FSDataInputStream is;
    private boolean closed;

    public BinaryFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, new BinaryToStruct(), config);

        this.is = getFs().open(getFilePath());
        this.fileStatus = getFs().getFileStatus(getFilePath());
        this.schema = buildSchema();
        this.closed = false;
    }

    @Override
    protected void configure(Map<String, String> config) {
    }

    @Override
    protected BinaryRecord nextRecord() throws IOException {
        return new BinaryRecord(schema, fileStatus, readFully(is));
    }

    @Override
    protected boolean hasNextRecord() throws IOException {
        return is.available() > 0;
    }

    @Override
    protected void seekFile(long offset) throws IOException {
        if (offset == 0 && !isClosed()) {
            is = getFs().open(getFilePath());
        } else if (!isClosed()){
            readFully(is);
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        is.close();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private Schema buildSchema() {
        return SchemaBuilder.struct()
                .field(FIELD_PATH, Schema.STRING_SCHEMA)
                .field(FIELD_OWNER, Schema.STRING_SCHEMA)
                .field(FIELD_GROUP, Schema.STRING_SCHEMA)
                .field(FIELD_LENGTH, Schema.INT64_SCHEMA)
                .field(FIELD_ACCESS_TIME, Schema.INT64_SCHEMA)
                .field(FIELD_MODIFICATION_TIME, Schema.INT64_SCHEMA)
                .field(FIELD_CONTENT, Schema.BYTES_SCHEMA)
                .build();
    }

    private byte[] readFully(FSDataInputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            while (true) {
                baos.write(in.readByte());
            }
        } catch (EOFException ignored) {
        }
        return baos.toByteArray();
    }

    static class BinaryToStruct implements ReaderAdapter<BinaryFileReader.BinaryRecord> {

        @Override
        public Struct apply(BinaryRecord record) {
            Struct struct = new Struct(record.schema);
            record.schema.fields().forEach(field -> {
                Object value = null;
                switch (field.name()) {
                    case FIELD_PATH:
                        value = record.fileStatus.getPath().toString();
                        break;
                    case FIELD_OWNER:
                        value = record.fileStatus.getOwner();
                        break;
                    case FIELD_GROUP:
                        value = record.fileStatus.getGroup();
                        break;
                    case FIELD_LENGTH:
                        value = record.fileStatus.getLen();
                        break;
                    case FIELD_ACCESS_TIME:
                        value = record.fileStatus.getAccessTime();
                        break;
                    case FIELD_MODIFICATION_TIME:
                        value = record.fileStatus.getModificationTime();
                        break;
                    case FIELD_CONTENT:
                        value = record.content;
                        break;
                }
                struct.put(field, value);
            });
            return struct;
        }
    }

    static class BinaryRecord {

        private final Schema schema;
        private final FileStatus fileStatus;
        private final byte[] content;

        BinaryRecord(Schema schema, FileStatus fileStatus, byte[] content) {
            this.schema = schema;
            this.fileStatus = fileStatus;
            this.content = content;
        }

    }
}
