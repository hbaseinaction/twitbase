# HBase In Action

FIXME: write description

## Usage

Build managed via [leiningen][1]. A version of lein is included with
the project source. It will retrieve its necessary dependencies the
first time it is run.

To build a jar suitable for running the examples:

    $ ./lein deps, jar

Run the example code like so:

    $ hadoop jar HBaseIA-*.jar HBaseIA.Ch03.TimeSpent src/test/resource/listing\ 3.3.txt ./out

[1]: https://github.com/technomancy/leiningen/tree/1.7.0

TwitBase utilities can be run using:

    $ ./lein run <alias> [options...]

Utility aliases include:

 - <tt>:init-tables</tt>
 - <tt>:twits-tool</tt>
 - <tt>:users-tool</tt>

## License

Copyright (C) 2012 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
