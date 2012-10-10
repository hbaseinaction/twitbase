# HBase In Action: TwitBase

[http://www.manning.com/dimidukkhurana][0]

## Compiling the project

Code is managed by maven. Be sure to install maven on your platform
before running these commands. Also be aware that HBase is not yet
supported on the OpenJDK platform, the default JVM installed on most
modern Linux distributions. You'll want to install the Oracle (Sun)
Java 6 runtime and make sure it's configured on your `$PATH` before
you continue. Again, on Ubuntu, you may find the [`oab-java6`][1]
utility to be of use.

To build a self-contained jar:

    $ mvn package

The jar created using this by default will allow you to interact with
HBase running in standalone mode on your local machine. If you want
to interact with a remote (possibly fully distributed) HBase
deployment, you can put your `hbase-site.xml` file in the
`src/main/resources` directory before compiling the jar.

## Using TwitBase

We have provided a launcher script to run TwitBase and the utilities
that the HBaseIA project comes with.

    $ bin/launcher

Just run the launcher without any arguments and it'll print out the
usage information.

TwitBase applications can also be run using java directly:

    $ java -cp target/twitbase-1.0.0.jar <app> [options...]

Utilities for interacting with TwitBase include:

 - `HBaseIA.TwitBase.InitTables` : create TwitBase tables
 - `HBaseIA.TwitBase.TwitsTool` : tool for managing Twits
 - `HBaseIA.TwitBase.UsersTool` : tool for managing Users
 - `HBaseIA.TwitBase.LoadUsers` : tool for loading random Users
 - `HBaseIA.TwitBase.LoadTwits` : tool for loading random Twits

The following MapReduce jobs can be launched the same way:

 - `HBaseIA.TwitBase.mapreduce.TimeSpent` : run TimeSpent log
   processing MR job
 - `HBaseIA.TwitBase.mapreduce.CountShakespeare` : run
   Shakespearean counter MR job
 - `HBaseIA.TwitBase.mapreduce.HamletTagger` : run
   hamlet-tagging MR job

## Other utilities and scripts

The following utilities are available for you to play with:

 - `utils.TablePreSplitter` : create pre-split table

## License

Copyright (C) 2012 Nick Dimiduk, Amandeep Khurana

Distributed under the [Apache License, version 2.0][2], the same as HBase.

[0]: http://www.manning.com/dimidukkhurana
[1]: https://github.com/flexiondotorg/oab-java6
[2]: http://www.apache.org/licenses/LICENSE-2.0.html
