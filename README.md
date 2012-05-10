# HBase In Action

[http://www.manning.com/dimidukkhurana][0]

## Usage

Code is managed by maven. Be sure to install maven2 on your platform.

To build a self-contained jar:

    $ mvn assembly:assembly

TwitBase applications can be run using:

    $ java -cp target/HBaseIA-1.0.0-SNAPSHOT-jar-with-dependencies.jar <app> [options...]

Utilities for interacting with TwitBase include:

 - <tt>HBaseIA.TwitBase.cli.InitTables</tt> :: create TwitBase tables
 - <tt>HBaseIA.TwitBase.cli.TwitsTool</tt> :: tool for managing Twits
 - <tt>HBaseIA.TwitBase.cli.UsersTool</tt> :: tool for managing Users
 - <tt>HBaseIA.TwitBase.cli.LoadUsers</tt> :: bulk-load random Users
 - <tt>HBaseIA.TwitBase.cli.LoadTwits</tt> :: bulk-load random Twits

The following MapReduce jobs can be launched the same way:

 - <tt>HBaseIA.TwitBase.mapreduce.TimeSpent</tt> :: run TimeSpent log
   processing MR job
 - <tt>HBaseIA.TwitBase.mapreduce.CountShakespeare</tt> :: run
   Shakespearean counter MR job
 - <tt>HBaseIA.TwitBase.mapreduce.HamletTagger</tt> :: run
   hamlet-tagging MR job

## License

Copyright (C) 2012 Nick Dimiduk, Amandeep Khurana

Distributed under the [Apache License, version 2.0][1], the same as HBase.

[0]: http://www.manning.com/dimidukkhurana
[1]: http://www.apache.org/licenses/LICENSE-2.0.html
