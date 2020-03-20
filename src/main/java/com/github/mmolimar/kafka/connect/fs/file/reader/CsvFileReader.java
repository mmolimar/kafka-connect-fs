package com.github.mmolimar.kafka.connect.fs.file.reader;

import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Map;

public class CsvFileReader extends UnivocityFileReader<CsvParserSettings> {

    public static final String FILE_READER_DELIMITED_SETTINGS_DELIMITER_DETECTION = FILE_READER_DELIMITED_SETTINGS + "delimiter_detection";
    public static final String FILE_READER_DELIMITED_SETTINGS_EMPTY_VALUE = FILE_READER_DELIMITED_SETTINGS + "empty_value";
    public static final String FILE_READER_DELIMITED_SETTINGS_ESCAPE_UNQUOTED = FILE_READER_DELIMITED_SETTINGS + "escape_unquoted";
    public static final String FILE_READER_DELIMITED_SETTINGS_FORMAT_DELIMITER = FILE_READER_DELIMITED_SETTINGS_FORMAT + "delimiter";

    public static final String FILE_READER_DELIMITED_SETTINGS_FORMAT_QUOTE = FILE_READER_DELIMITED_SETTINGS_FORMAT + "quote";
    public static final String FILE_READER_DELIMITED_SETTINGS_FORMAT_QUOTE_ESCAPE = FILE_READER_DELIMITED_SETTINGS_FORMAT + "quote_scape";

    public CsvFileReader(FileSystem fs, Path filePath, Map<String, Object> config) throws IOException {
        super(fs, filePath, config);
    }

    @Override
    protected CsvParserSettings parserSettings(Map<String, String> config) {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setDelimiterDetectionEnabled(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_DELIMITER_DETECTION, false));
        settings.setEmptyValue(config.get(FILE_READER_DELIMITED_SETTINGS_EMPTY_VALUE));
        settings.setEscapeUnquotedValues(getBoolean(config, FILE_READER_DELIMITED_SETTINGS_ESCAPE_UNQUOTED, false));
        settings.getFormat().setDelimiter(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_FORMAT_DELIMITER, ","));
        settings.getFormat().setQuote(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_FORMAT_QUOTE, "\"").charAt(0));
        settings.getFormat().setQuoteEscape(config.getOrDefault(FILE_READER_DELIMITED_SETTINGS_FORMAT_QUOTE_ESCAPE, "\"").charAt(0));

        return settings;
    }

    @Override
    protected AbstractParser<CsvParserSettings> createParser(CsvParserSettings settings) {
        return new CsvParser(settings);
    }
}
