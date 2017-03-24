Configuration Options
============================================

Policies
--------------------------------------------

Simple
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This policy does not have any additional configuration.

Sleepy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``policy.custom.sleepy.sleep``
  Max sleep time (in ms) to wait to look for files in the FS.

  * Type: long
  * Importance: high

``policy.custom.sleepy.fraction``
  Sleep fraction to divide the sleep time to allow interrupt the policy.

  * Type: long
  * Default: 10
  * Importance: medium

``policy.custom.sleepy.max_execs``
  Max sleep times allowed (negative to disable).

  * Type: long
  * Default: -1
  * Importance: medium

Hdfs file watcher
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This policy does not have any additional configuration.


File readers
--------------------------------------------

Avro
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This reader does not have any additional configuration.

Parquet
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This reader does not have any additional configuration.

SequenceFile
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This reader does not have any additional configuration.

Delimited text
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``file.reader.delimited.token``
  The token delimiter for columns.

  * Type: string
  * Importance: high

``file.reader.delimited.header``
  If the file contains header or not (default false).

  * Type: boolean
  * Default: false
  * Importance: medium

Text
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This reader does not have any additional configuration.



