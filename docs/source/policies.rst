Policies
============================================

Policies

Simple
--------------------------------------------

Just list files included in the corresponding URI.

.. note:: This policy is more oriented for testing purposes.
          It never stops and Kafka Connect is continuosly trying to poll data from the file system.

Sleepy
--------------------------------------------

Simple policy with an custom sleep on each execution.

Hdfs file watcher
--------------------------------------------

It uses Hadoop notifications events (since Hadoop 2.6.0) and all
create/append/close events will be reported as new files to be ingested.
Just use it when your URIs start with ``hdfs://``

