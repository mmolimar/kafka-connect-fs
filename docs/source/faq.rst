.. faq:

********************************************
FAQs
********************************************

**My file was already processed and the connector, when it's executed again,
processes the same records again.**

If during the previous executions the records were sent successfully to Kafka,
their offsets were sent too. Then, when executing the policy again, it
retrieves the offset and seeks the file. If this didn't happen, it's possible
that the offset was not committed yet and, consequently, the offset retrieved
is non-existent or too old.

Have a look when the offsets are committed in Kafka and/or try to execute the
policy when you are sure the offsets have been committed.

**The connector started but does not process any kind of file.**

This can be for several reasons:

* Check if the files contained in the FS match the regexp provided.
* Check if there is any kind of problem with the FS. The connector tolerates
  FS connection exceptions to process them later but in log files you'll find
  these possible errors.
* The file reader is reading files with an invalid format so it cannot
  process the file and continues with the next one. You can see
  this as an error in the log.

**I have directories in the FS created day by day and I have to modify
the connector everyday.**

Don't do this! Take advantage of the dynamic URIs using expressions.

For instance, if you have this URI ``hdfs://host:9000/data/2020``, you can
use this URI ``hdfs://host:9000/data/${yyyy}`` instead.

**The connector is too slow to process all URIs I have.**

Obviously, this depends of the files in the FS(s) but having several URIs in
the connector might be a good idea to adjust the number of tasks
to process those URIs in parallel ( ``tasks.max`` connector property).

**I removed a file from the FS but the connector is still sending messages
with the contents of that file.**

This is a tricky issue. The file reader is an iterator and processes
record by record but part of the file is buffered and, even though the
file was removed from the FS, the file reader continues producing records
until throws an exception. It's a matter of time.

But the main thing is that you don't have to worry about removing files
from the FS when they are being processed. The connector tolerates errors
when reading files and continues with the next file.
