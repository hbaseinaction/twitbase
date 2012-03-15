(defproject HBaseIA "1.0.0-SNAPSHOT"
  :description "Code and examples pertaining to HBase In Action"

  :dev-dependencies [[org.apache.hbase/hbase "0.92.0"]
                     [org.apache.hadoop/hadoop-core "1.0.0"]]
  :repositories {"apache release"
                 "https://repository.apache.org/content/repositories/releases/"}

  :omit-source true
  :javac-options {:debug "true"
                  :target "1.5"}
  :source-path "src/main/clj"
  :java-source-path "src/main/java"
  :run-aliases {:users-tool  HBaseIA.TwitBase.cli.UsersTool
                :twits-tool  HBaseIA.TwitBase.cli.TwitsTool
                :init-tables HBaseIA.TwitBase.cli.InitTables})
