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
  Class containing the connector.

  * Type: string
  * Importance: high

``tasks.max``
  The number of tasks the connector is allowed to start.

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

Hdfs file watcher
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

This reader does not have any additional configuration.

.. _config_options-filereaders-parquet:

Parquet
--------------------------------------------

This reader does not have any additional configuration.

.. _config_options-filereaders-sequencefile:

SequenceFile
--------------------------------------------

In order to configure custom properties for this reader, the name you must use is ``sequence``.

``file_reader.sequence.buffer_size``
  Custom buffer size to read data from the Sequence file.

  * Type: int
  * Default: 4096
  * Importance: medium

.. _config_options-filereaders-text:

Text
--------------------------------------------

This reader does not have any additional configuration.

.. _config_options-filereaders-delimited:

Delimited text
--------------------------------------------

In order to configure custom properties for this reader, the name you must use is ``delimited``.

``file_reader.delimited.token``
  The token delimiter for columns.

  * Type: string
  * Importance: high

``file_reader.delimited.header``
  If the file contains header or not.

  * Type: boolean
  * Default: false
  * Importance: medium



