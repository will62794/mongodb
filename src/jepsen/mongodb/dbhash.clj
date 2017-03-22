(ns jepsen.mongodb.dbhash
  "Lots of ops, followed by a final db hash check on each replica."
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.tools.logging :refer [info debug warn]]
            [jepsen
             [util :as util :refer [meh timeout]]
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.mongodb.core :refer :all]
            [jepsen.mongodb.mongo :as m]))

; Define a Client record type that extends client/Client from Jepsen core.
(defrecord Client [db-name coll-name read-concern write-concern client coll]
  client/Client
  (setup! [this test node]
    (let [client (m/client node)
          coll  (-> client
                    (m/db db-name)
                    (m/collection coll-name)
                    (m/with-read-concern read-concern)
                    (m/with-write-concern write-concern))]
      (assoc this :client client, :coll coll)))

  (invoke! [this test op]
    (with-errors op #{:read}
      (case (:f op)
        :add (let [res (m/insert! coll {:value (:value op)})]
               (assoc op :type :ok))
        :read (assoc op
                     :type :ok
                     :value (->> coll
                                 m/find-all
                                 (map :value)
                                 (into (sorted-set)))))))

  (teardown! [this test]
    (.close ^java.io.Closeable client)))

; Define the test client with a database, collection name, and read/write concern options
(defn client
  "A set test client"
  [opts]
  (Client. "jepsen" "randomcool"
           (:read-concern opts)
          (:write-concern opts)
          nil nil))

(defn test
  "A set test, which inserts a sequence of integers into a collection, and
  performs a final read back."
  [opts]
  (test-
    "set"
    (merge
      {:client (client opts)
       :concurrency (count (:nodes opts))
       :generator (->> (range)
                       (map (fn [x] {:type :invoke, :f :add, :value x}))
                       gen/seq
                       (gen/stagger 1/2))
       :final-generator (gen/each
                          ; First one wakes up the mongo client, second one
                          ; reads
                          (gen/limit 2 {:type :invoke, :f :read, :value nil}))
       :checker (checker/compose
                  {:set      checker/set
                   :timeline (timeline/html)
                   :perf     (checker/perf)})}
      opts)))
