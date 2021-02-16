Parquet
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Reads files with `Parquet <https://parquet.apache.org/>`__ format.

The reader takes advantage of the Parquet-Avro API and uses the Parquet file
as if it was an Avro file, so the message sent to Kafka is built in the same
way as the Avro file reader does.

More information about properties of this file reader :ref:`here<config_options-filereaders-parquet>`.

Avro
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Files with `Avro <https://avro.apache.org/>`__ format can be read with this reader.

The Avro schema is not needed due to is read from the file. The message sent
to Kafka is created by transforming the record by means of
`Confluent avro-converter <https://github.com/confluentinc/schema-registry/tree/master/avro-converter>`__
API.

More information about properties of this file reader :ref:`here<config_options-filereaders-avro>`.

ORC
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`ORC files <https://orc.apache.org>`__ are a self-describing type-aware
columnar file format designed for Hadoop workloads.

This reader can process this file format, translating its schema and building
a Kafka message with the content.

.. warning:: If you have ORC files with ``union`` data types, this sort of
             data types will be transformed in a ``map`` object in the Kafka message.
             The value of each key will be ``fieldN``, where ``N`` represents
             the index within the data type.

More information about properties of this file reader :ref:`here<config_options-filereaders-orc>`.

SequenceFile
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

`Sequence files <https://wiki.apache.org/hadoop/SequenceFile>`__ are one kind of
the Hadoop file formats which are serialized in key-value pairs.

This reader can process this file format and build a Kafka message with the
key-value pair. These two values are named ``key`` and ``value`` in the message
by default but you can customize these field names.

More information about properties of this file reader :ref:`here<config_options-filereaders-sequencefile>`.

Cobol
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Mainframe files (Cobol / EBCDIC binary files) can be processed with this reader which uses the
`Cobrix <https://github.com/AbsaOSS/cobrix/>`__ parser.

By means of the corresponding copybook -representing its schema-, it parses each record and
translate it into a Kafka message with the schema.

More information about properties of this file reader :ref:`here<config_options-filereaders-cobol>`.

Binary
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

All other kind of binary files can be ingested using this reader.

It just extracts the content plus some metadata such as: path, file owner, file group, length, access time,
and modification time.

Each message will contain the following schema:

  * ``path``: File path (string).
  * ``owner``: Owner of the file. (string).
  * ``group``: Group associated with the file. (string).
  * ``length``: Length of this file, in bytes. (long).
  * ``access_time``: Access time of the file. (long).
  * ``modification_time``: Modification time of the file (long).
  * ``content``: Content of the file (bytes).

More information about properties of this file reader :ref:`here<config_options-filereaders-binary>`.

CSV
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

CSV file reader using a custom token to distinguish different columns in each line.

It allows to distinguish a header in the files and set the name of their columns
in the message sent to Kafka. If there is no header, the value of each column will be in
the field named ``column_N`` (**N** represents the column index) in the message.
Also, the token delimiter for columns is configurable.

This reader is based on the `Univocity CSV parser <https://www.univocity.com/pages/univocity_parsers_csv.html#working-with-csv>`__.

More information about properties of this file reader :ref:`here<config_options-filereaders-csv>`.

TSV
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

TSV file reader using a tab ``\t`` to distinguish different columns in each line.

Its behaviour is the same one for the CSV file reader regarding the header and the column names.

This reader is based on the `Univocity TSV parser <https://www.univocity.com/pages/univocity_parsers_tsv.html#working-with-tsv>`__.

More information about properties of this file reader :ref:`here<config_options-filereaders-tsv>`.

FixedWidth
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

FixedWidth is a plain text file reader which distinguishes each column based on the length of each field.

Its behaviour is the same one for the CSV / TSV file readers regarding the header and the column names.

This reader is based on the `Univocity Fixed-Width parser <https://www.univocity.com/pages/univocity_parsers_fixed_width.html#working-with-fixed-width>`__.

More information about properties of this file reader :ref:`here<config_options-filereaders-fixedwidth>`.

JSON
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Reads JSON files which might contain multiple number of fields with their specified
data types. The schema for this sort of records is inferred reading the first record
and marked as optional in the schema all the fields contained.

More information about properties of this file reader :ref:`here<config_options-filereaders-json>`.

XML
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Reads XML files which might contain multiple number of fields with their specified
data types. The schema for this sort of records is inferred reading the first record
and marked as optional in the schema all the fields contained.

.. warning:: Take into account the current
             `limitations <https://github.com/FasterXML/jackson-dataformat-xml#known-limitations>`__.

More information about properties of this file reader :ref:`here<config_options-filereaders-xml>`.

YAML
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Reads YAML files which might contain multiple number of fields with their specified
data types. The schema for this sort of records is inferred reading the first record
and marked as optional in the schema all the fields contained.

More information about properties of this file reader :ref:`here<config_options-filereaders-yaml>`.

Text
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Reads plain text files.

Each line represents one record (by default) which will be in a field
named ``value`` in the message sent to Kafka by default but you can
customize these field names.

More information about properties of this file reader :ref:`here<config_options-filereaders-text>`.

Agnostic
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Actually, this reader is a wrapper of the readers listing above.

It tries to read any kind of file format using an internal reader based on the file extension,
applying the proper one (Parquet, Avro, ORC, SequenceFile, Cobol / EBCDIC, CSV, TSV, FixedWidth, JSON, XML,
YAML, or Text). In case of no extension has been matched, the Text file reader will be applied.

Default extensions for each format (configurable):

* Parquet: ``.parquet``
* Avro: ``.avro``
* ORC: ``.orc``
* SequenceFile: ``.seq``
* Cobol / EBCDIC: ``.dat``
* Other binary files: ``.bin``
* CSV: ``.csv``
* TSV: ``.tsv``
* FixedWidth: ``.fixed``
* JSON: ``.json``
* XML: ``.xml``
* YAML: ``.yaml``
* Text: any other sort of file extension.

More information about properties of this file reader :ref:`here<config_options-filereaders-agnostic>`.
