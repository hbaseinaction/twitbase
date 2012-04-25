# HBase In Action (http://www.manning.com/dimidukkhurana)

FIXME: write description

## Usage

Build managed via [leiningen][1]. A version of lein is included with
the project source. It will retrieve its necessary dependencies the
first time it is run.

To build the source:

    $ ./lein deps, compile

[1]: https://github.com/technomancy/leiningen/tree/1.7.0

TwitBase utilities can be run using:

    $ ./lein run <alias> [options...]

Utility aliases include:

 - <tt>:init-tables</tt> :: create TwitBase tables
 - <tt>:twits-tool</tt>  :: tool for managing Twits
 - <tt>:users-tool</tt>  :: tool for managing Users
 - <tt>:load-users</tt>  :: bulk-load random Users
 - <tt>:load-twits</tt>  :: bulk-load random Twits
 - <tt>:timespent</tt>   :: run TimeSpent log processing MR job
 - <tt>:shakespeare</tt> :: run Shakespearean counter MR job
 - <tt>:tag-hamlet</tt>  :: run hamlet-tagging MR job

## License

Copyright (C) 2012 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
