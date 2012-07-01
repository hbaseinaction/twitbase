# HBase In Action

[http://www.manning.com/dimidukkhurana][0]

## Usage

Code is managed by maven. Be sure to install maven on your platform
before running these commands. Also be aware that HBase is not yet
supported on the OpenJDK platform, the default JVM installed on
Ubuntu. You'll want to install the Oracle (Sun) Java 6 runtime and
make sure it's configured on your `$PATH` before you continue. Again,
on Ubuntu, you may find the [`oab-java6`][1] utility to be of use.

To build a self-contained jar:

    $ mvn assembly:assembly

TwitBase applications can be run using:

    $ java -cp target/HBaseIA-1.0.0-SNAPSHOT-jar-with-dependencies.jar <app> [options...]

Utilities for interacting with TwitBase include:

 - `HBaseIA.TwitBase.cli.InitTables` :: create TwitBase tables
 - `HBaseIA.TwitBase.cli.TwitsTool` :: tool for managing Twits
 - `HBaseIA.TwitBase.cli.UsersTool` :: tool for managing Users

The following MapReduce jobs can be launched the same way:

 - `HBaseIA.TwitBase.mapreduce.TimeSpent` :: run TimeSpent log
   processing MR job
 - `HBaseIA.TwitBase.mapreduce.CountShakespeare` :: run
   Shakespearean counter MR job
 - `HBaseIA.TwitBase.mapreduce.HamletTagger` :: run
   hamlet-tagging MR job

## License

Copyright (C) 2012 Nick Dimiduk, Amandeep Khurana

Distributed under the [Apache License, version 2.0][2], the same as HBase.

[0]: http://www.manning.com/dimidukkhurana
[1]: https://github.com/flexiondotorg/oab-java6
[2]: http://www.apache.org/licenses/LICENSE-2.0.html
