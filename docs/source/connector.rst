Connector
============================================

Kafka Connect File System is a Source Connector for reading data from
any file system which has a implementation of the ``org.apache.hadoop.fs.FileSystem``
class from `Hadoop-Common <https://github.com/apache/hadoop-common>`__ and writing to Kafka.

Among others, these are some file systems it supports:

* HDFS.
* WebHDFS.
* S3.
* FTP.
* Local File System.
* Hadoop Archive File System.

You can download the source code `here. <https://github.com/mmolimar/kafka-connect-fs>`__

.. toctree::
   :maxdepth: 3

   getting_started
   features
