Simple
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It's a policy which just filters and processes files included in the corresponding URIs one time.

.. attention:: This policy is more oriented for testing purposes.

Sleepy
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The behaviour of this policy is similar to Simple policy but on each execution it sleeps
and wait for the next one. Additionally, its custom properties allow to end it.

You can learn more about the properties of this policy :ref:`here<config_options-policies-sleepy>`.

Cron
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This policy is scheduled based on cron expressions and their format to put in the configuration
are based on the library `Quartz Scheduler <https://www.quartz-scheduler.org>`__

After finishing each execution, the policy gets slept until the next one is scheduled, if applicable.

You can learn more about the properties of this policy :ref:`here<config_options-policies-cron>`.

HDFS file watcher
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It uses Hadoop notifications events and all create/append/rename/close events will be reported
as files to be ingested.

Just use it when you have HDFS URIs.

You can learn more about the properties of this policy :ref:`here<config_options-policies-hdfs>`.

.. attention:: The URIs included in the general property ``fs.uris`` will be filtered and only those
               ones which start with the prefix ``hdfs://`` will be watched. Also, this policy
               will only work for Hadoop versions 2.6.0 or higher.
