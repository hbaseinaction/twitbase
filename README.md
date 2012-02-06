# HBase In Action

FIXME: write description

## Usage

Build managed via [leiningen][1]. Install it and then checkout this
project.

To build a jar suitable for running the examples:

    $ lein deps, jar

Run the example code like so:

    $ hadoop jar HBaseIA-*.jar HBaseIA.Ch03.TimeSpent src/test/resource/listing\ 3.3.txt ./out

[1]: https://github.com/technomancy/leiningen/tree/1.6.2

TwitBase utilities can be run using:

    $ java -cp `hbase classpath`:HBaseIA-<version>.jar HBaseIA.TwitBase.cli.<tool> [options...] 

Utilities include:

 - <tt>InitTables</tt>
 - <tt>TwitsTool</tt>
 - <tt>UserTool</tt>

## License

Copyright (C) 2012 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
