.. kafka-connect-fs documentation master file, created by Mario Molina

********************************************
FileSystem Connector
********************************************

Kafka Connect FileSystem Connector is a source connector for reading records from
files in the file systems specified and load them into Kafka.

The connector supports:

* Several sort of FS to use.
* Dynamic and static URIs to ingest data from.
* Policies to define rules about how to look for files.
* File readers to parse and read different kind of file formats.

To learn more about the connector you can read :ref:`this section<connector>` and for more detailed
configuration options you can read :ref:`this other one<config_options>`.

Also, you can download the source code from `here. <https://github.com/mmolimar/kafka-connect-fs>`__

Contents
============================================

.. toctree::
   :maxdepth: 2

   connector
   config_options
   faq
