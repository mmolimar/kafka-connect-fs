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
  converted to ``file:///data/2017`` (when executing whe policy).

  You can use as many as you like in the URIs, for instance:
  ``file:///data/${yyyy}/${MM}/${dd}/${HH}${mm}``
  
.. tip:: 
  If you want to ingest data from S3, you can add credentials with :
  ``policy.fs.fs.s3a.access.key=<ACCESS_KEY>``
  and
  ``policy.fs.fs.s3a.secret.key=<SECRET_KEY>``
  
``topic``
  Topic in which copy data.

  * Type: string
  * Importance: high

``policy.class``
  Policy class to apply (must implement ``com.github.mmolimar.kafka.connect.fs.policy.Policy`` interface).

  * Type: string
  * Importance: high

``policy.recursive``
  Flag to activate traversed recursion in subdirectories when listing files.

  * Type: boolean
  * Default: false
  * Importance: medium

``policy.regexp``
  Regular expression to filter files from the FS.

  * Type: string
  * Importance: high

``policy.<policy_name>.<policy_property>``
  This represents the custom properties you can include based on the policy class specified.

  * Type: depending on the policy.
  * Importance: depending on the policy.

``policy.fs.<fs_property>``
  Custom properties to use for the FS.

  * Type: depending on the FS.
  * Importance: depending on the FS.

``file_reader.class``
  File reader class to read files from the FS (must implement
  ``com.github.mmolimar.kafka.connect.fs.file.reader.FileReader`` interface).

  * Type: string
  * Importance: high

``file_reader.<file_reader_name>.<file_reader_property>``
  This represents the custom properties you can include based on the file reader class specified.

  * Type: depending on the file reader.
  * Importance: depending on the file reader.

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
  * Default: 10
  * Importance: medium

``policy.sleepy.max_execs``
  Max executions allowed (negative to disable). After exceeding this number, the policy will end.
  An execution represents: listing files from the FS and its corresponding sleep time.

  * Type: long
  * Default: -1
  * Importance: medium

.. _config_options-policies-hdfs:

HDFS file watcher
--------------------------------------------

This policy does not have any additional configuration.

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

.. _config_options-filereaders-sequencefile:

SequenceFile
--------------------------------------------

In order to configure custom properties for this reader, the name you must use is ``sequence``.

``file_reader.sequence.field_name.key``
  Custom field name for the output key to include in the Kafka message.

  * Type: string
  * Default: key
  * Importance: medium

``file_reader.sequence.field_name.value``
  Custom field name for the output value to include in the Kafka message.

  * Type: string
  * Default: value
  * Importance: medium

``file_reader.sequence.buffer_size``
  Custom buffer size to read data from the Sequence file.

  * Type: int
  * Default: 4096
  * Importance: low

.. _config_options-filereaders-json:

JSON
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``json``.

``file_reader.json.record_per_line``
  If enabled, the reader will read each line as a record. Otherwise, the reader will read the full
  content of the file as a record.

  * Type: boolean
  * Default: true
  * Importance: medium

``file_reader.json.deserialization.<deserialization_feature>``
  Deserialization feature to use when reading a JSON file. You can add as much as you like
  based on the ones defined `here. <https://fasterxml.github.io/jackson-databind/javadoc/2.10/com/fasterxml/jackson/databind/DeserializationFeature.html#enum.constant.summary>`__

  * Type: boolean
  * Importance: medium

``file_reader.json.compression.type``
  Compression type to use when reading a file.

  * Type: enum (available values ``bzip2``, ``gzip`` and ``none``)
  * Default: none
  * Importance: medium

``file_reader.json.compression.concatenated``
  Flag to specify if the decompression of the reader will finish at the end of the file or after
  the first compressed stream.

  * Type: boolean
  * Default: true
  * Importance: low

``file_reader.json.encoding``
  Encoding to use for reading a file. If not specified, the reader will use the default encoding.

  * Type: string
  * Importance: medium

.. _config_options-filereaders-text:

Text
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``text``.

``file_reader.json.record_per_line``
  If enabled, the reader will read each line as a record. Otherwise, the reader will read the full
  content of the file as a record.

  * Type: boolean
  * Default: true
  * Importance: medium

``file_reader.json.compression.type``
  Compression type to use when reading a file.

  * Type: enum (available values ``bzip2``, ``gzip`` and ``none``)
  * Default: none
  * Importance: medium

``file_reader.json.compression.concatenated``
  Flag to specify if the decompression of the reader will finish at the end of the file or after
  the first compressed stream.

  * Type: boolean
  * Default: true
  * Importance: low

``file_reader.text.field_name.value``
  Custom field name for the output value to include in the Kafka message.

  * Type: string
  * Default: value
  * Importance: low

``file_reader.text.encoding``
  Encoding to use for reading a file. If not specified, the reader will use the default encoding.

  * Type: string
  * Importance: medium

.. _config_options-filereaders-delimited:

Delimited text
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``delimited``.

``file_reader.delimited.token``
  The token delimiter for columns.

  * Type: string
  * Importance: high

``file_reader.delimited.header``
  If the file contains header or not.

  * Type: boolean
  * Default: false
  * Importance: medium

``file_reader.json.record_per_line``
  If enabled, the reader will read each line as a record. Otherwise, the reader will read the full
  content of the file as a record.

  * Type: boolean
  * Default: true
  * Importance: medium

``file_reader.delimited.default_value``
  Sets a default value in a column when its value is null. This is due to the record is malformed (it does not contain
  all expected columns).

  * Type: string
  * Default: ``null``
  * Importance: medium

``file_reader.json.compression.type``
  Compression type to use when reading a file.

  * Type: enum (available values ``bzip2``, ``gzip`` and ``none``)
  * Default: none
  * Importance: medium

``file_reader.json.compression.concatenated``
  Flag to specify if the decompression of the reader will finish at the end of the file or after
  the first compressed stream.

  * Type: boolean
  * Default: true
  * Importance: low

``file_reader.delimited.encoding``
  Encoding to use for reading a file. If not specified, the reader will use the default encoding.

  * Type: string
  * Importance: medium

Agnostic
--------------------------------------------

To configure custom properties for this reader, the name you must use is ``agnostic``.

``file_reader.agnostic.extensions.parquet``
  A comma-separated string list with the accepted extensions for Parquet files.

  * Type: string
  * Default: parquet
  * Importance: medium

``file_reader.agnostic.extensions.avro``
  A comma-separated string list with the accepted extensions for Avro files.

  * Type: string
  * Default: avro
  * Importance: medium

``file_reader.agnostic.extensions.sequence``
  A comma-separated string list with the accepted extensions for Sequence files.

  * Type: string
  * Default: seq
  * Importance: medium

``file_reader.agnostic.extensions.json``
  A comma-separated string list with the accepted extensions for JSON files.

  * Type: string
  * Default: json
  * Importance: medium

``file_reader.agnostic.extensions.delimited``
  A comma-separated string list with the accepted extensions for Delimited text files.

  * Type: string
  * Default: tsv,csv
  * Importance: medium

.. note:: The Agnostic reader uses the previous ones as inner readers. So, in case of using this
          reader, you'll probably need to include also the specified properties for those
          readers in the connector configuration as well.
