# Kafka Connect FileSystem Source Connector [![Build Status](https://travis-ci.org/mmolimar/kafka-connect-fs.svg?branch=master)](https://travis-ci.org/mmolimar/kafka-connect-fs.svg?branch=master)[![Coverage Status](https://coveralls.io/repos/github/mmolimar/kafka-connect-fs/badge.svg?branch=master)](https://coveralls.io/github/mmolimar/kafka-connect-fs?branch=master)

Kafka Connect FileSystem is a Source Connector for reading data from any file system which implements 
``org.apache.hadoop.fs.FileSystem`` class from [Hadoop-Common](https://github.com/apache/hadoop-common) and writing to Kafka.

## Prerequisites

- Confluent 3.1.1
- Java 8

## Getting started

### Building source jar ###
    mvn clean package

### Config the connector ###
    name=FsSourceConnector
    connector.class=com.github.mmolimar.kafka.connect.fs.FsSourceConnector
    tasks.max=1
    fs.uris=file://data,hdfs://localhost:9001/dir
    policy.class=com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy
    policy.recursive=true
    file.reader.class=com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader
    file.regexps=^[0-9]*\.txt$
The ``kafka-connect-fs.properties`` file defines:
1. The connector name.
2. The class containing the connector.
3. The number of tasks the connector is allowed to start.
4. Comma-separated URIs of the FS(s).
5. Policy class to apply.
6. Flag to activate traversed recursion in subdirectories when listing files.
7. File reader class to read files from the FS.
8. Regular expression to filter files from the FS.

In case of using the SleepyPolicy you should configure also:
```
    policy.custom.sleep=200000
    policy.custom.sleep.fraction=100
    policy.custom.max.executions=-1
```
1. Max sleep time (in ms) to wait to look for files in the FS.
2. Sleep fraction to divide the sleep time to allow interrupt the policy
3. Max sleep times allowed (negative to disable)

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

