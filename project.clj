(defproject HBaseIA "1.0.0-SNAPSHOT"
  :description "Code and examples pertaining to HBase In Action"
  :dependencies [[org.apache.hbase/hbase "0.90.5"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "1.0.0"]]
  :omit-source true
  :javac-options {:debug "true"
                  :target "1.5"}
  :source-path "src/main/clj"
  :java-source-path "src/main/java"
  :run-aliases {:users-tool  HBaseIA.TwitBase.cli.UsersTool
                :twits-tool  HBaseIA.TwitBase.cli.TwitsTool
                :init-tables HBaseIA.TwitBase.cli.InitTables})
