(ns HBaseIA.core
  "Convienence library."
  (:require [clojure.java.io :as io])
  (:import
   [java.util.concurrent Executors]
   [org.apache.hadoop.hbase.client HTablePool]
   [HBaseIA.TwitBase.hbase TwitsDAO UsersDAO]))

(def *names*
  (with-open [f (io/reader "/usr/share/dict/propernames")]
    (.split (slurp f) "\n")))

(def *words*
  (with-open [f (io/reader "/usr/share/dict/words")]
    (.split (slurp f) "\n")))

(let [*pool* (HTablePool.)]
  (def *users* (UsersDAO. *pool*))
  (def *twits* (TwitsDAO. *pool*)))

(defn get-user [user] (.getUser *users* user))

(defn list-users []
  (seq (.getUsers *users*)))

(defn add-user [user name email passwd]
  (.addUser *users* user name email passwd))

(defn add-user-random []
  (let [name (str (rand-nth *names*) " " (rand-nth *names*))
        user (format "%s%2d" (.substring name 0 5) (rand-int 100))
        email (format "%s@%s.com" user (rand-nth *words*))]
    (add-user user name email "abc123")
    (get-user user)))
