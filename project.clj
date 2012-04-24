(defproject HBaseIA "1.0.0-SNAPSHOT"
  :description "Code and examples pertaining to HBase In Action"

  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.apache.hadoop/hadoop-core "1.0.0"]
                 [org.apache.hbase/hbase "0.92.0"
                  ;; avoid brining in lots of maven cruft
                  :exclusions [org.apache.maven.plugins/maven-release-plugin]]]
  :repositories {"apache release"
                 "https://repository.apache.org/content/repositories/releases/"}

  :omit-source true
  :javac-options {:debug "true"
                  :target "1.5"}
  :source-path "src/main/clj"
  :java-source-path "src/main/java"
  :run-aliases {;; twitbase cli tools
                :users-tool  HBaseIA.TwitBase.cli.UsersTool
                :twits-tool  HBaseIA.TwitBase.cli.TwitsTool
                :init-tables HBaseIA.TwitBase.cli.InitTables
                ;; convienence utils
                :load-users  HBaseIA.TwitBase.repl/load-users
                :load-twits  HBaseIA.TwitBase.repl/load-twits
                ;; short-hand to launch mapreduce jobs locally
                :timespent   HBaseIA.TwitBase.mapreduce.TimeSpent
                :shakespeare HBaseIA.TwitBase.mapreduce.CountShakespeare
                :tag-hamlet  HBaseIA.TwitBase.mapreduce.HamletTagger})
