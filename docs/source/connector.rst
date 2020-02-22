.. _connector:

********************************************
Connector
********************************************

The connector takes advantage of the abstraction provided from `Hadoop Common <http://hadoop.apache.org/>`__
using the implementation of the ``org.apache.hadoop.fs.FileSystem`` class. So, it's possible to use a
wide variety of FS or if your FS is not included in the Hadoop Common API you can implement an extension
of this abstraction and using it in a transparent way.

Among others, these are some file systems it supports:

* HDFS.
* WebHDFS.
* S3.
* FTP and SFTP.
* Local File System.
* Hadoop Archive File System.

Getting started
============================================

Prerequisites
--------------------------------------------

-  Confluent Platform 5.4.0
-  Java 8

Building from source
--------------------------------------------

.. sourcecode:: bash

   mvn clean package

General config
--------------------------------------------

The ``kafka-connect-fs.properties`` file defines the following properties as required:

.. sourcecode:: bash

   name=FsSourceConnector
   connector.class=com.github.mmolimar.kafka.connect.fs.FsSourceConnector
   tasks.max=1
   fs.uris=file:///data,hdfs://localhost:9000/
   topic=mytopic
   policy.class=<Policy class>
   policy.recursive=true
   policy.regexp=.*
   file_reader.class=<File reader class>

#. The connector name.
#. Class indicating the connector.
#. Number of tasks the connector is allowed to start.
#. Comma-separated URIs of the FS(s). They can be URIs pointing out directly to a file
   or a directory in the FS. These URIs can also be dynamic by using expressions for
   modifying them in runtime.
#. Topic in which copy data from the FS.
#. Policy class to apply (must implement
   ``com.github.mmolimar.kafka.connect.fs.policy.Policy`` interface).
#. Flag to activate traversed recursion in subdirectories when listing files.
#. Regular expression to filter files from the FS.
#. File reader class to read files from the FS
   (must implement ``com.github.mmolimar.kafka.connect.fs.file.reader.FileReader`` interface).

A more detailed information about these properties can be found :ref:`here<config_options-general>`.

Running in development
--------------------------------------------

.. sourcecode:: bash

   export CONFLUENT_HOME=/path/to/confluent/install/dir

.. sourcecode:: bash

   mvn clean package
   export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
   $CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/kafka-connect-fs.properties

Components
============================================

There are two main concepts to decouple concerns within the connector.
They are **policies** and **file readers**, described below.

Policies
--------------------------------------------

In order to ingest data from the FS(s), the connector needs a **policy** to define the rules to do it.

Basically, the policy tries to connect to each FS included in ``fs.uris`` connector property, list files
(and filter them using the regular expression provided in the ``policy.regexp`` property) and enables
a file reader to read records from them.

The policy to be used by the connector is defined in ``policy.class`` connector property.

.. important:: When delivering records from the connector to Kafka, they contain their own file offset
               so, if in the next eventual policy execution this file is processed again,
               the policy will seek the file to this offset and process the next records
               if any (**if the offset was committed**).

.. note:: If the URIs included in the ``fs.uris`` connector property contain any expression of the
          form ``${XXX}``, this dynamic URI is built in the moment of the policy execution.

Currently, there are few policies to support some use cases but, for sure, you can develop your own one
if the existing policies don't fit your needs.
The only restriction is that you must implement the interface
``com.github.mmolimar.kafka.connect.fs.policy.Policy``.

.. include:: policies.rst

File readers
--------------------------------------------

They read files and process each record from the FS. The **file reader** is needed by the policy to enable
the connector to process each record and includes in the implementation how to seek and iterate over the
records in the file.

The file reader to be used when processing files is defined in ``file_reader.class`` connector property.

In the same way as the policies, the connector provides several sort of readers to parse and read records
for different file formats. If you don't have a file reader that fits your needs, just implement one
with the unique restriction that it must implement the interface
``com.github.mmolimar.kafka.connect.fs.file.reader.FileReader``.

.. include:: filereaders.rst
