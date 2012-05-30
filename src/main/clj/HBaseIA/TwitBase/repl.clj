(ns HBaseIA.TwitBase.repl
  "Convienence library."
  (:require [clojure.java.io :as io])
  (:import
   [java.util.concurrent Executors]
   [org.apache.hadoop.hbase.client HTablePool]
   [HBaseIA.TwitBase.hbase FollowersDAO TwitsDAO UsersDAO]
   [org.joda.time DateTime]))

(def names
  (with-open [f (io/reader "/usr/share/dict/propernames")]
    (.split (slurp f) "\n")))

(def words
  (with-open [f (io/reader "/usr/share/dict/words")]
    (.split (slurp f) "\n")))

(defn rand-date []
  (let [y (+ 2010 (rand-int 5))
        m (inc (rand-int 12))
        d (inc (rand-int 28))]
    (DateTime. y m d 0 0 0 0)))

(let [pool (HTablePool.)]
  (def users (UsersDAO. pool))
  (def twits (TwitsDAO. pool))
  (def followers (FollowersDAO. pool)))

(defn get-user [user] (.getUser users user))

(defn list-users []
  (seq (.getUsers users)))

(defn add-user [user name email passwd]
  (.addUser users user name email passwd))

(defn add-user-random []
  (let [name (str (rand-nth names) " " (rand-nth names))
        user (format "%s%2d" (.substring name 0 5) (rand-int 100))
        email (format "%s@%s.com" user (rand-nth words))]
    (add-user user name email "abc123")
    (get-user user)))

(defn twit
  ([user msg]
     (twit user (DateTime.) msg))
  ([user dt msg]
     (.postTwit twits user dt msg)))

(defn twit-random
  [user]
  (->> (repeatedly 12 #(rand-nth words))
       (reduce #(format "%s %s" %1 %2))
       (twit user (rand-date))))

(defn list-twits [user]
  (seq (.list twits user)))

(defn add-follows [userA userB]
  (do ;; DERP! repalce with Observer!
    (.addFollows followers userA userB)
    (.addFollowing followers userA userB)))

(defn list-relations []
  (seq (.listRelations followers)))

(defn followers-count-scan [user]
  (.followersCountScan followers user))

(defn followers-count-coproc [user]
  (.followersCount followers user))

;;
;; cli mains
;;
;; Use these fns to populate TwitBase after table initialization.
;;

(defn load-users-main
  "Load n random users."
  [n]
  (let [n (Integer. n)]
    (-> (repeatedly n #'add-user-random)
        (doall))))

(defn load-twits-main
  "Load n random twits per user."
  [n]
  (let [n (Integer. n)
        _ (->> (list-users)
               (map #(-> (repeatedly n (fn [] (twit-random (.user %))))
                         (doall)))
               (doall))]))

(gen-class
 :name HBaseIA.TwitBase.cli.LoadUsers
 :prefix "load-users-"
 :main true)

(gen-class
 :name HBaseIA.TwitBase.cli.LoadTwits
 :prefix "load-twits-"
 :main true)
