# Kafka Connect FileSystem Source Connector [![Build Status](https://travis-ci.org/mmolimar/kafka-connect-fs.svg?branch=master)](https://travis-ci.org/mmolimar/kafka-connect-fs)[![Coverage Status](https://coveralls.io/repos/github/mmolimar/kafka-connect-fs/badge.svg?branch=master)](https://coveralls.io/github/mmolimar/kafka-connect-fs?branch=master)

Kafka Connect FileSystem is a Source Connector for reading data from any file system which implements 
``org.apache.hadoop.fs.FileSystem`` class from [Hadoop-Common](https://github.com/apache/hadoop-common) and writing to Kafka.

## Prerequisites

- Confluent 3.1.1
- Java 8

## Getting started

### Building source ###
    mvn clean package

### Config the connector ###
    name=FsSourceConnector
    connector.class=com.github.mmolimar.kafka.connect.fs.FsSourceConnector
    tasks.max=1
    fs.uris=file:///data,hdfs://localhost:9001/data
    topic=mytopic
    policy.class=com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy
    policy.recursive=true
    file.reader.class=com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader
    file.regexps=^[0-9]*\.txt$
The ``kafka-connect-fs.properties`` file defines:

1. The connector name.
2. The class containing the connector.
3. The number of tasks the connector is allowed to start.
4. Comma-separated URIs of the FS(s). They can be URIs pointing directly to a file in the FS.
5. Topic in which copy data to.
6. Policy class to apply.
7. Flag to activate traversed recursion in subdirectories when listing files.
8. File reader class to read files from the FS.
9. Regular expression to filter files from the FS.

#### Policies ####

##### SimplePolicy #####

Just list files included in the corresponding URI.

##### SleepyPolicy #####

Simple policy with an custom sleep on each execution.

```
    policy.custom.sleep=200000
    policy.custom.sleep.fraction=100
    policy.custom.max.executions=-1
```
1. Max sleep time (in ms) to wait to look for files in the FS.
2. Sleep fraction to divide the sleep time to allow interrupt the policy.
3. Max sleep times allowed (negative to disable).

##### HdfsFileWatcherPolicy #####

It uses Hadoop notifications events (since Hadoop 2.6.0) and all create/append/close events will be reported as new files to be ingested.
Just use it when your URIs start with ``hdfs://``

#### File readers ####

##### AvroFileReader #####

Read files with [Avro](http://avro.apache.org/) format.

##### ParquetFileReader #####

Read files with [Parquet](https://parquet.apache.org/) format.

##### SequenceFileReader #####

Read [Sequence files](https://wiki.apache.org/hadoop/SequenceFile).

##### DelimitedTextFileReader #####

Text file reader using custom tokens to distinguish different columns on each line. 

```
    file.reader.delimited.header=true
    file.reader.delimited.token=,
```
1. If the file contains header or not (default false).
2. The token delimiter for columns.

##### TextFileReader #####

Read plain text files. Each line represents one record.

### Running in development ###
```
mvn clean package
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
$CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/kafka-connect-fs.properties
```

## TODO's

- [ ] Add more file readers.
- [ ] Add more policies.
- [ ] Manages FS blocks.
- [ ] Improve documentation.
- [ ] Include a FS Sink Connector.

## Contributing

If you would like to add/fix something to this connector, you are welcome to do so!

## License

Released under the Apache License, version 2.0.

