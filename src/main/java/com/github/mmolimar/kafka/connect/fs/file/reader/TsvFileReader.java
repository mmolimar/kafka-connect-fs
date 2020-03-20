package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public class TsvFileReader extends UnivocityFileReader<TsvParserSettings> {

    public static final String FILE_READER_DELIMITED_SETTINGS_LINE_JOINING = FILE_READER_DELIMITED_SETTINGS + "line_joining";

    public static final String FILE_READER_DELIMITED_SETTINGS_FORMAT_ESCAPE = FILE_READER_DELIMITED_SETTINGS_FORMAT + "escape";
    public static final String FILE_READER_DELIMITED_SETTINGS_FORMAT_ESCAPED_CHAR = FILE_READER_DELIMITED_SETTINGS_FORMAT + "escaped_char";

    public TsvFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, config);
    }

    @Override
    protected TsvParserSettings parserSettings(Map<String, String> config) {
        TsvParserSettings settings = new TsvParserSettings();
        settings.setLineJoiningEnabled(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_LINE_JOINING, false));
        settings.getFormat().setEscapeChar(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_FORMAT_ESCAPE, "\"").charAt(0));
        settings.getFormat().setEscapedTabChar(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_FORMAT_ESCAPED_CHAR, "\"").charAt(0));

        return settings;
    }

    @Override
    protected AbstractParser<TsvParserSettings> createParser(TsvParserSettings settings) {
        return new TsvParser(settings);
    }
}
