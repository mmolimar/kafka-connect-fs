Getting started
============================================

Building source
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
   fs.uris=file:///data,hdfs://localhost:9000/data
   topic=mytopic
   policy.class=<Policy class>
   policy.recursive=true
   file.reader.class=<File reader class>
   file.regexps=*

1.  The connector name.
2.  The class containing the connector.
3.  The number of tasks the connector is allowed to start.
4.  Comma-separated URIs of the FS(s). They can be URIs pointing directly to a file in the FS.
5.  Topic in which copy data.
6.  Policy class to apply (must implement ``com.github.mmolimar.kafka.connect.fs.policy.Policy`` interface).
7.  Flag to activate traversed recursion in subdirectories when listing files.
8.  File reader class to read files from the FS (must implement ``com.github.mmolimar.kafka.connect.fs.file.reader.FileReader`` interface).
9.  Regular expression to filter files from the FS.

Running in development
--------------------------------------------

.. sourcecode:: bash

   export CONFLUENT_HOME=/path/to/confluent/install/dir

.. sourcecode:: bash

   mvn clean package
   export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
   $CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/kafka-connect-fs.properties
