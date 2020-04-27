package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthParser;
import com.univocity.parsers.fixed.FixedWidthParserSettings;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class FixedWidthFileReader extends UnivocityFileReader<FixedWidthParserSettings> {

    public static final String FILE_READER_DELIMITED_SETTINGS_FIELD_LENGTHS = FILE_READER_DELIMITED_SETTINGS + "field_lengths";
    public static final String FILE_READER_DELIMITED_SETTINGS_KEEP_PADDING = FILE_READER_DELIMITED_SETTINGS + "keep_padding";
    public static final String FILE_READER_DELIMITED_SETTINGS_PADDING_FOR_HEADERS = FILE_READER_DELIMITED_SETTINGS + "padding_for_headers";
    public static final String FILE_READER_DELIMITED_SETTINGS_ENDS_ON_NEW_LINE = FILE_READER_DELIMITED_SETTINGS + "ends_on_new_line";
    public static final String FILE_READER_DELIMITED_SETTINGS_SKIP_TRAILING_CHARS = FILE_READER_DELIMITED_SETTINGS + "skip_trailing_chars";

    public static final String FILE_READER_DELIMITED_SETTINGS_FORMAT_PADDING = FILE_READER_DELIMITED_SETTINGS_FORMAT + "padding";

    public FixedWidthFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, config);
    }

    @Override
    protected FixedWidthParserSettings parserSettings(Map<String, String> config) {
        FixedWidthFields fieldLengths = new FixedWidthFields();
        Optional.ofNullable(config.get(FILE_READER_DELIMITED_SETTINGS_FIELD_LENGTHS))
                .map(fl -> Arrays.stream(fl.split(",")))
                .ifPresent(fl -> fl.forEach(field -> fieldLengths.addField(Integer.parseInt(field))));

        FixedWidthParserSettings settings = new FixedWidthParserSettings(fieldLengths);
        settings.setKeepPadding(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_KEEP_PADDING, false));
        settings.setUseDefaultPaddingForHeaders(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_PADDING_FOR_HEADERS, true));
        settings.setRecordEndsOnNewline(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_ENDS_ON_NEW_LINE, true));
        settings.setSkipTrailingCharsUntilNewline(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_SKIP_TRAILING_CHARS, false));
        settings.getFormat().setPadding(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_FORMAT_PADDING, " ").charAt(0));

        return settings;
    }

    @Override
    protected AbstractParser<FixedWidthParserSettings> createParser(FixedWidthParserSettings settings) {
        return new FixedWidthParser(settings);
    }
}
