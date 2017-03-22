(ns jepsen.mongodb.runner
  "Runs the full Mongo test suite, including a config file. Provides exit
  status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen.mongodb [core :as m]
                            [mongo :as client]
                            [set :as set]
                            [document-cas :as dc]
                            [dbhash :as dbh]]
            [jepsen [cli :as jc]
                    [core :as jepsen]]))

(def test-names
  {"set" set/test
   "register" dc/test
   "dbhash" dbh/test})

(def opt-spec
  "Command line option specification for tools.cli."
  [[nil "--key-time-limit SECONDS"
    "How long should we test an individual key for, in seconds?"
    :default  100
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-w" "--write-concern LEVEL" "Write concern level"
    :default  :majority
    :parse-fn keyword
    :validate [client/write-concerns (jc/one-of client/write-concerns)]]

   ["-r" "--read-concern LEVEL" "Read concern level"
    :default  :linearizable
    :parse-fn keyword
    :validate [client/read-concerns (jc/one-of client/read-concerns)]]

   [nil "--no-reads" "Disable reads, to test write safety only"]

   [nil "--read-with-find-and-modify" "Use findAndModify to ensure read safety"]

   ["-s" "--storage-engine ENGINE" "Mongod storage engine"
    :default  "wiredTiger"
    :validate [(partial re-find #"\A[a-zA-Z0-9]+\z") "Must be a single word"]]

   ["-p" "--protocol-version INT" "Replication protocol version number"
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [(complement neg?) "Must be non-negative"]]

   ["-t" "--test TEST-NAME" "Test to run"
    :default  set/test
    :parse-fn test-names
    :validate [identity (jc/one-of test-names)]]])

(defn -main
  [& args]
  (jc/run!
    (merge (jc/serve-cmd)
           (jc/single-test-cmd
             {:opt-spec opt-spec
              :tarball "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian81-3.4.0-rc3.tgz"
              :test-fn (fn [opts] ((:test opts) (dissoc opts :test)))}))
    args))
