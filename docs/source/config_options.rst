.. _config_options:

********************************************
Configuration Options
********************************************

.. _config_options-general:

General
============================================

General config properties for this connector.

``name``
  The connector name.

  * Type: string
  * Importance: high

``connector.class``
  Class indicating the connector.

  * Type: string
  * Importance: high

``tasks.max``
  Number of tasks the connector is allowed to start.

  * Type: int
  * Importance: high

.. tip::
  The number of URIs specified in the connector config will be grouped based on the
  number of tasks defined. So, if you have just one URI with one task is fine. Otherwise,
  if you want to improve the performance and process URIs in parallel you should adjust
  this number based on your requirements.

``fs.uris``
  Comma-separated URIs of the FS(s). They can be URIs pointing directly to a file in the FS and
  also can be dynamic using expressions for modifying the URIs in runtime. These expressions
  have the form ``${XXX}`` where XXX represents a pattern from ``java.time.format.DateTimeFormatter``
  Java class.

  * Type: string
  * Importance: high

.. tip::
  If you want to ingest data from dynamic directories, this is, directories created every day and
  avoiding to add new URIs or look for files from a parent directory, you can include expressions
  in the URIs to do that. For example, for this URI ``file:///data/${yyyy}``, it will be
  converted to ``file:///data/2020`` (when executing whe policy).

  You can use as many as you like in the URIs, for instance:
  ``file:///data/${yyyy}/${MM}/${dd}/${HH}${mm}``
  
.. tip:: 
  If you want to ingest data from S3, you can add credentials with:
  ``policy.fs.fs.s3a.access.key=<ACCESS_KEY>``
  and
  ``policy.fs.fs.s3a.secret.key=<SECRET_KEY>``
  
``topic``
  Topic in which copy data to.

  * Type: string
  * Importance: high

``poll.interval.ms``
  Frequency in milliseconds to poll for new data. This config just applies when the policies have ended.

  * Type: int
  * Default: ``10000``
  * Importance: medium

``policy.class``
  Policy class to apply (must implement ``com.github.mmolimar.kafka.connect.fs.policy.Policy`` interface).

  * Type: string
  * Importance: high

``policy.regexp``
  Regular expression to filter files from the FS.

  * Type: string
  * Importance: high

``policy.recursive``
  Flag to activate traversed recursion in subdirectories when listing files.

  * Type: boolean
  * Default: ``false``
  * Importance: medium

``policy.batch_size``
  Number of files that should be handled at a time. Non-positive values disable batching.

  * Type: int
  * Default: ``0``
  * Importance: medium

``policy.<policy_name>.<policy_property>``
  This represents custom properties you can include based on the policy class specified.

  * Type: based on the policy.
  * Importance: based on the policy.

``policy.fs.<fs_property>``
  Custom properties to use for the FS.

  * Type: based on the FS.
  * Importance: based on the FS.

``file_reader.class``
  File reader class to read files from the FS (must implement
  ``com.github.mmolimar.kafka.connect.fs.file.reader.FileReader`` interface).

  * Type: string
  * Importance: high

``file_reader.batch_size``
  Number of records to process at a time. Non-positive values disable batching.

  * Type: int
  * Default: ``0``
  * Importance: medium

``file_reader.<file_reader_name>.<file_reader_property>``
  This represents custom properties you can include based on the file reader class specified.

  * Type: based on the file reader.
  * Importance: based on the file reader.

.. _config_options-policies:

Policies
============================================

Some policies have custom properties to define and others don't.
So, depending on the configuration you'll have to take into account their properties.

.. _config_options-policies-simple:

Simple
--------------------------------------------

This policy does not have any additional configuration.

.. _config_options-policies-sleepy:

Sleepy
--------------------------------------------

In order to configure custom properties for this policy, the name you must use is ``sleepy``.

``policy.sleepy.sleep``
  Max sleep time (in ms) to wait to look for files in the FS. Once an execution has finished, the policy
  will sleep during this time to be executed again.

  * Type: long
  * Importance: high

``policy.sleepy.fraction``
  Sleep fraction to divide the sleep time to allow interrupting the policy faster.

  * Type: long
  * Default: ``10``
  * Importance: medium

``policy.sleepy.max_execs``
  Max executions allowed (negative to disable). After exceeding this number, the policy will end.
  An execution represents: listing files from the FS and its corresponding sleep time.

  * Type: long
  * Default: ``-1``
  * Importance: medium

.. _config_options-policies-cron:

Cron
--------------------------------------------

In order to configure custom properties for this policy, the name you must use is ``cron``.

``policy.cron.expression``
  Cron expression to schedule the policy.

  * Type: string
  * Importance: high

``policy.cron.end_date``
  End date to finish the policy with `ISO date-time <https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#ISO_LOCAL_DATE_TIME>`__
  format.

  * Type: date
  * Default: ``null``
  * Importance: medium

.. _config_options-policies-hdfs:

HDFS file watcher
--------------------------------------------

In order to configure custom properties for this policy, the name you must use is ``hdfs_file_watcher``.

``policy.hdfs_file_watcher.poll``
  Time to wait until the records retrieved from the file watcher will be sent to the source task.

  * Type: long
  * Default: ``5000``
  * Importance: medium

``policy.hdfs_file_watcher.retry``
  Sleep time to retry connections to HDFS in case of connection errors happened.

  * Type: long
  * Default: ``20000``
  * Importance: medium

.. _config_options-filereaders:

File readers
============================================

Some file readers have custom properties to define and others don't. So, depending on the configuration you'll have
to take into account their properties.

.. _config_options-filereaders-avro:

Avro
--------------------------------------------

In order to configure custom properties for this reader, the name you must use is ``avro``.

``file_reader.avro.schema``
  Avro schema in JSON format to use when reading a file.
  If not specified, the reader will use the schema defined in the file.

  * Type: string
  * Importance: medium

.. _config_options-filereaders-parquet:

Parquet
--------------------------------------------

In order to configure custom properties for this reader, the name you must use is ``parquet``.

``file_reader.parquet.schema``
  Avro schema in JSON format to use when reading a file.

  * Type: string
  * Importance: medium

``file_reader.parquet.projection``
  Avro schema in JSON format to use for projecting fields from records in a file.

  * Type: string
  * Importance: medium

.. _config_options-filereaders-orc:

ORC
--------------------------------------------

In order to configure custom properties for this reader, the name you must use is ``orc``.

``file_reader.orc.use_zerocopy``
  Use zero-copy when reading a ORC file.

  * Type: boolean
  * Default: ``false``
  * Importance: medium

``file_reader.orc.skip_corrupt_records``
  If reader will skip corrupt data or not. If disabled, an exception will be thrown when there is
  corrupted data in the file.

  * Type: boolean
  * Default: ``false``
  * Importance: medium

.. _config_options-filereaders-json:

SequenceFile
--------------------------------------------

In order to configure custom properties for this reader, the name you must use is ``sequence``.

``file_reader.sequence.field_name.key``
  Custom field name for the output key to include in the Kafka message.

  * Type: string
  * Default: ``key``
  * Importance: medium

``file_reader.sequence.field_name.value``
  Custom field name for the output value to include in the Kafka message.

  * Type: string
  * Default: ``value``
  * Importance: medium

``file_reader.sequence.buffer_size``
  Custom buffer size to read data from the Sequence file.

  * Type: int
  * Default: ``4096``
  * Importance: low

.. _config_options-filereaders-json:

JSON
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``json``.

``file_reader.json.record_per_line``
  If enabled, the reader will read each line as a record. Otherwise, the reader will read the full
  content of the file as a record.

  * Type: boolean
  * Default: ``true``
  * Importance: medium

``file_reader.json.deserialization.<deserialization_feature>``
  Deserialization feature to use when reading a JSON file. You can add as much as you like
  based on the ones defined `here. <https://fasterxml.github.io/jackson-databind/javadoc/2.10/com/fasterxml/jackson/databind/DeserializationFeature.html#enum.constant.summary>`__

  * Type: boolean
  * Importance: medium

``file_reader.json.encoding``
  Encoding to use for reading a file. If not specified, the reader will use the default encoding.

  * Type: string
  * Default: based on the locale and charset of the underlying operating system.
  * Importance: medium

``file_reader.json.compression.type``
  Compression type to use when reading a file.

  * Type: enum (available values ``bzip2``, ``gzip`` and ``none``)
  * Default: ``none``
  * Importance: medium

``file_reader.json.compression.concatenated``
  Flag to specify if the decompression of the reader will finish at the end of the file or after
  the first compressed stream.

  * Type: boolean
  * Default: ``true``
  * Importance: low

.. _config_options-filereaders-csv:

CSV
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``delimited`` (even though it's for CSV).

``file_reader.delimited.settings.format.delimiter``
  Field delimiter.

  * Type: string
  * Default: ``,``
  * Importance: high

``file_reader.delimited.settings.header``
  If the file contains header or not.

  * Type: boolean
  * Default: ``false``
  * Importance: high

``file_reader.delimited.settings.schema``
  A comma-separated list of ordered data types for each field in the file. Possible values: ``byte``, ``short``,
  ``int``, ``long``, ``float``, ``double``, ``boolean``, ``bytes`` and ``string``)

  * Type: string[]
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.data_type_mapping_error``
  Flag to enable/disable throwing errors when mapping data types based on the schema is not possible. If disabled,
  the returned value which could not be mapped will be ``null``.

  * Type: boolean
  * Default: ``true``
  * Importance: medium

``file_reader.delimited.settings.allow_nulls``
  If the schema supports nullable fields. If ``file_reader.delimited.settings.data_type_mapping_error`` config flag is
  disabled, the value set for this config will be ignored and set to ``true``.

  * Type: boolean
  * Default: ``false``
  * Importance: medium

``file_reader.delimited.settings.header_names``
  A comma-separated list of ordered field names to set when reading a file.

  * Type: string[]
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.null_value``
  Default value for ``null`` values.

  * Type: string
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.empty_value``
  Default value for empty values (empty values within quotes).

  * Type: string
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.format.line_separator``
  Line separator to be used.

  * Type: string
  * Default: ``\n``
  * Importance: medium

``file_reader.delimited.settings.max_columns``
  Default value for ``null`` values.

  * Type: int
  * Default: ``512``
  * Importance: low

``file_reader.delimited.settings.max_chars_per_column``
  Default value for ``null`` values.

  * Type: int
  * Default: ``4096``
  * Importance: low

``file_reader.delimited.settings.rows_to_skip``
  Number of rows to skip.

  * Type: long
  * Default: ``0``
  * Importance: low

``file_reader.delimited.settings.line_separator_detection``
  If the reader should detect the line separator automatically.

  * Type: boolean
  * Default: ``false``
  * Importance: low

``file_reader.delimited.settings.delimiter_detection``
  If the reader should detect the delimiter automatically.

  * Type: boolean
  * Default: ``false``
  * Importance: low

``file_reader.delimited.settings.ignore_leading_whitespaces``
  Flag to enable/disable skipping leading whitespaces from values.

  * Type: boolean
  * Default: ``true``
  * Importance: low

``file_reader.delimited.settings.ignore_trailing_whitespaces``
  Flag to enable/disable skipping trailing whitespaces from values.

  * Type: boolean
  * Default: ``true``
  * Importance: low

``file_reader.delimited.settings.format.comment``
  Character that represents a line comment at the beginning of a line.

  * Type: char
  * Default: ``#``
  * Importance: low

``file_reader.delimited.settings.escape_unquoted``
  Flag to enable/disable processing escape sequences in unquoted values.

  * Type: boolean
  * Default: ``false``
  * Importance: low

``file_reader.delimited.settings.format.quote``
  Character used for escaping values where the field delimiter is part of the value.

  * Type: char
  * Default: ``"``
  * Importance: low

``file_reader.delimited.settings.format.quote_escape``
  Character used for escaping quotes inside an already quoted value.

  * Type: char
  * Default: ``"``
  * Importance: low

``file_reader.delimited.encoding``
  Encoding to use for reading a file. If not specified, the reader will use the default encoding.

  * Type: string
  * Default: based on the locale and charset of the underlying operating system.
  * Importance: medium

``file_reader.delimited.compression.type``
  Compression type to use when reading a file.

  * Type: enum (available values ``bzip2``, ``gzip`` and ``none``)
  * Default: ``none``
  * Importance: medium

``file_reader.delimited.compression.concatenated``
  Flag to specify if the decompression of the reader will finish at the end of the file or after
  the first compressed stream.

  * Type: boolean
  * Default: ``true``
  * Importance: low

.. _config_options-filereaders-tsv:

TSV
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``delimited`` (even though it's for TSV).

``file_reader.delimited.settings.header``
  If the file contains header or not.

  * Type: boolean
  * Default: ``false``
  * Importance: high

``file_reader.delimited.settings.schema``
  A comma-separated list of ordered data types for each field in the file. Possible values: ``byte``, ``short``,
  ``int``, ``long``, ``float``, ``double``, ``boolean``, ``bytes`` and ``string``)

  * Type: string[]
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.data_type_mapping_error``
  Flag to enable/disable throwing errors when mapping data types based on the schema is not possible. If disabled,
  the returned value which could not be mapped will be ``null``.

  * Type: boolean
  * Default: ``true``
  * Importance: medium

``file_reader.delimited.settings.allow_nulls``
  If the schema supports nullable fields. If ``file_reader.delimited.settings.data_type_mapping_error`` config flag is
  disabled, the value set for this config will be ignored and set to ``true``.

  * Type: boolean
  * Default: ``false``
  * Importance: medium

``file_reader.delimited.settings.header_names``
  A comma-separated list of ordered field names to set when reading a file.

  * Type: string[]
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.null_value``
  Default value for ``null`` values.

  * Type: string
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.format.line_separator``
  Line separator to be used.

  * Type: string
  * Default: ``\n``
  * Importance: medium

``file_reader.delimited.settings.max_columns``
  Default value for ``null`` values.

  * Type: int
  * Default: ``512``
  * Importance: low

``file_reader.delimited.settings.max_chars_per_column``
  Default value for ``null`` values.

  * Type: int
  * Default: ``4096``
  * Importance: low

``file_reader.delimited.settings.rows_to_skip``
  Number of rows to skip.

  * Type: long
  * Default: ``0``
  * Importance: low

``file_reader.delimited.settings.line_separator_detection``
  If the reader should detect the line separator automatically.

  * Type: boolean
  * Default: ``false``
  * Importance: low

``file_reader.delimited.settings.line_joining``
  Identifies whether or lines ending with the escape character and followed by a line
  separator character should be joined with the following line.

  * Type: boolean
  * Default: ``true``
  * Importance: low

``file_reader.delimited.settings.ignore_leading_whitespaces``
  Flag to enable/disable skipping leading whitespaces from values.

  * Type: boolean
  * Default: ``true``
  * Importance: low

``file_reader.delimited.settings.ignore_trailing_whitespaces``
  Flag to enable/disable skipping trailing whitespaces from values.

  * Type: boolean
  * Default: ``true``
  * Importance: low

``file_reader.delimited.settings.format.comment``
  Character that represents a line comment at the beginning of a line.

  * Type: char
  * Default: ``#``
  * Importance: low

``file_reader.delimited.settings.format.escape``
  Character used for escaping special characters.

  * Type: char
  * Default: ``\``
  * Importance: low

``file_reader.delimited.settings.format.escaped_char``
  Character used to represent an escaped tab.

  * Type: char
  * Default: ``t``
  * Importance: low

``file_reader.delimited.encoding``
  Encoding to use for reading a file. If not specified, the reader will use the default encoding.

  * Type: string
  * Default: based on the locale and charset of the underlying operating system.
  * Importance: medium

``file_reader.delimited.compression.type``
  Compression type to use when reading a file.

  * Type: enum (available values ``bzip2``, ``gzip`` and ``none``)
  * Default: ``none``
  * Importance: medium

``file_reader.delimited.compression.concatenated``
  Flag to specify if the decompression of the reader will finish at the end of the file or after
  the first compressed stream.

  * Type: boolean
  * Default: ``true``
  * Importance: low

.. _config_options-filereaders-fixedwidth:

FixedWidth
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``delimited`` (even though it's for FixedWidth).

``file_reader.delimited.settings.field_lengths``
  A comma-separated ordered list of integers with the lengths of each field.

  * Type: int[]
  * Importance: high

``file_reader.delimited.settings.header``
  If the file contains header or not.

  * Type: boolean
  * Default: ``false``
  * Importance: high

``file_reader.delimited.settings.schema``
  A comma-separated list of ordered data types for each field in the file. Possible values: ``byte``, ``short``,
  ``int``, ``long``, ``float``, ``double``, ``boolean``, ``bytes`` and ``string``)

  * Type: string[]
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.data_type_mapping_error``
  Flag to enable/disable throwing errors when mapping data types based on the schema is not possible. If disabled,
  the returned value which could not be mapped will be ``null``.

  * Type: boolean
  * Default: ``true``
  * Importance: medium

``file_reader.delimited.settings.allow_nulls``
  If the schema supports nullable fields. If ``file_reader.delimited.settings.data_type_mapping_error`` config flag is
  disabled, the value set for this config will be ignored and set to ``true``.

  * Type: boolean
  * Default: ``false``
  * Importance: medium

``file_reader.delimited.settings.header_names``
  A comma-separated list of ordered field names to set when reading a file.

  * Type: string[]
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.keep_padding``
  If the padding character should be kept in each value.

  * Type: boolean
  * Default: ``false``
  * Importance: medium

``file_reader.delimited.settings.padding_for_headers``
  If headers have the default padding specified.

  * Type: boolean
  * Default: ``true``
  * Importance: medium

``file_reader.delimited.settings.null_value``
  Default value for ``null`` values.

  * Type: string
  * Default: ``null``
  * Importance: medium

``file_reader.delimited.settings.format.ends_on_new_line``
  Line separator to be used.

  * Type: boolean
  * Default: ``true``
  * Importance: medium

``file_reader.delimited.settings.format.line_separator``
  Line separator to be used.

  * Type: string
  * Default: ``\n``
  * Importance: medium

``file_reader.delimited.settings.format.padding``
  The padding character used to represent unwritten spaces.

  * Type: char
  * Default: `` ``
  * Importance: medium

``file_reader.delimited.settings.max_columns``
  Default value for ``null`` values.

  * Type: int
  * Default: ``512``
  * Importance: low

``file_reader.delimited.settings.max_chars_per_column``
  Default value for ``null`` values.

  * Type: int
  * Default: ``4096``
  * Importance: low

``file_reader.delimited.settings.skip_trailing_chars``
  If the trailing characters beyond the record's length should be skipped.

  * Type: boolean
  * Default: ``false``
  * Importance: low

``file_reader.delimited.settings.rows_to_skip``
  Number of rows to skip.

  * Type: long
  * Default: ``0``
  * Importance: low

``file_reader.delimited.settings.line_separator_detection``
  If the reader should detect the line separator automatically.

  * Type: boolean
  * Default: ``false``
  * Importance: low

``file_reader.delimited.settings.ignore_leading_whitespaces``
  Flag to enable/disable skipping leading whitespaces from values.

  * Type: boolean
  * Default: ``true``
  * Importance: low

``file_reader.delimited.settings.ignore_trailing_whitespaces``
  Flag to enable/disable skipping trailing whitespaces from values.

  * Type: boolean
  * Default: ``true``
  * Importance: low

``file_reader.delimited.settings.format.comment``
  Character that represents a line comment at the beginning of a line.

  * Type: char
  * Default: ``#``
  * Importance: low

``file_reader.delimited.encoding``
  Encoding to use for reading a file. If not specified, the reader will use the default encoding.

  * Type: string
  * Default: based on the locale and charset of the underlying operating system.
  * Importance: medium

``file_reader.delimited.compression.type``
  Compression type to use when reading a file.

  * Type: enum (available values ``bzip2``, ``gzip`` and ``none``)
  * Default: ``none``
  * Importance: medium

``file_reader.delimited.compression.concatenated``
  Flag to specify if the decompression of the reader will finish at the end of the file or after
  the first compressed stream.

  * Type: boolean
  * Default: ``true``
  * Importance: low

.. _config_options-filereaders-text:

Text
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``text``.

``file_reader.text.record_per_line``
  If enabled, the reader will read each line as a record. Otherwise, the reader will read the full
  content of the file as a record.

  * Type: boolean
  * Default: ``true``
  * Importance: medium

``file_reader.text.field_name.value``
  Custom field name for the output value to include in the Kafka message.

  * Type: string
  * Default: ``value``
  * Importance: medium

``file_reader.text.encoding``
  Encoding to use for reading a file. If not specified, the reader will use the default encoding.

  * Type: string
  * Default: based on the locale and charset of the underlying operating system.
  * Importance: medium

``file_reader.text.compression.type``
  Compression type to use when reading a file.

  * Type: enum (available values ``bzip2``, ``gzip`` and ``none``)
  * Default: ``none``
  * Importance: medium

``file_reader.text.compression.concatenated``
  Flag to specify if the decompression of the reader will finish at the end of the file or after
  the first compressed stream.

  * Type: boolean
  * Default: ``true``
  * Importance: low

.. _config_options-filereaders-agnostic:

Agnostic
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``agnostic``.

``file_reader.agnostic.extensions.parquet``
  A comma-separated string list with the accepted extensions for Parquet files.

  * Type: string
  * Default: ``parquet``
  * Importance: medium

``file_reader.agnostic.extensions.avro``
  A comma-separated string list with the accepted extensions for Avro files.

  * Type: string
  * Default: ``avro``
  * Importance: medium

``file_reader.agnostic.extensions.orc``
  A comma-separated string list with the accepted extensions for ORC files.

  * Type: string
  * Default: ``orc``
  * Importance: medium

``file_reader.agnostic.extensions.sequence``
  A comma-separated string list with the accepted extensions for Sequence files.

  * Type: string
  * Default: ``seq``
  * Importance: medium

``file_reader.agnostic.extensions.json``
  A comma-separated string list with the accepted extensions for JSON files.

  * Type: string
  * Default: ``json``
  * Importance: medium

``file_reader.agnostic.extensions.csv``
 A comma-separated string list with the accepted extensions for CSV files.

  * Type: string
  * Default: ``csv``
  * Importance: medium

``file_reader.agnostic.extensions.tsv``
 A comma-separated string list with the accepted extensions for TSV files.

  * Type: string
  * Default: ``tsv``
  * Importance: medium

``file_reader.agnostic.extensions.fixed``
 A comma-separated string list with the accepted extensions for fixed-width files.

  * Type: string
  * Default: ``fixed``
  * Importance: medium

.. note:: The Agnostic reader uses the previous ones as inner readers. So, in case of using this
          reader, you'll probably need to include also the specified properties for those
          readers in the connector configuration as well.
