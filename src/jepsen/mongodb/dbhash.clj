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


; Define a 'Client' record type that implements client/Client from Jepsen core.

; Protocols in Clojure, defined with (defprotocol name ...), are roughly like
; interfaces in Java. They are just a collection of named functions & their
; signatures. You can "implement" a protocol by defining a record using (defrecord
; name [fields]) and implementing concrete versions of  the functions declared in
; the protocol.
(defrecord Client [db-name coll-name read-concern write-concern client coll]
  client/Client
  (setup! [this test node]
    (let [client (m/client node)
          coll  (-> client ;Note that "->" is like a pipe operator, allowing you to thread a value through multiple function calls linearly.
                    (m/db db-name)
                    (m/collection coll-name)
                    (m/with-read-concern read-concern)
                    (m/with-write-concern write-concern))]
      (assoc this :client client, :coll coll, :dbase (m/db client db-name))))

  (invoke! [this test op]
    (with-errors op #{:read}
      (case (:f op)
        :add (let [res (m/insert! coll {:value (:value op)})]
               (assoc op :type :ok))
        :read-dbhash (assoc op
                        :type :ok
                        :value ((info "wilbur schultz.")
                            (try (info (mongo! "var rs = new ReplSetTest('192.168.33.100:27017').status();"))
                              (catch Exception e))))
                            ; (let [command '("dbHash" "dbHash")]
                            ; (m/run-command! (:dbase this) command))))
        :read (assoc op
                     :type :ok
                     :value (->> coll
                                 m/find-all
                                 (map :value)
                                 (into (sorted-set)))))))

  (teardown! [this test]
    (.close ^java.io.Closeable client)))

; Define the test client with a database, collection name, and read/write concern options
; "Client."" is one way of constructing a "Client" record
(defn client
  "A set test client"
  [opts]
  (Client. "jepsen" "randomcollection"
           (:read-concern opts)
          (:write-concern opts)
          nil nil))


; (def set
;   "Given a set of :add operations followed by a final :read, verifies that... (db hashes of collections match).
;   every successfully added element is present in the read, and that the read
;   contains only elements for which an add was attempted."
;   (reify Checker
;     (check [this test model history opts]
;       (let [attempts (->> history
;                           (r/filter op/invoke?)
;                           (r/filter #(= :add (:f %)))
;                           (r/map :value)
;                           (into #{}))
;             adds (->> history
;                       (r/filter op/ok?)
;                       (r/filter #(= :add (:f %)))
;                       (r/map :value)
;                       (into #{}))
;             final-read (->> history
;                           (r/filter op/ok?)
;                           (r/filter #(= :read (:f %)))
;                           (r/map :value)
;                           (reduce (fn [_ x] x) nil))]
;         (if-not final-read
;           {:valid? :unknown
;            :error  "Set was never read"}

;           (let [; The OK set is every read value which we tried to add
;                 ok          (set/intersection final-read attempts)

;                 ; Unexpected records are those we *never* attempted.
;                 unexpected  (set/difference final-read attempts)

;                 ; Lost records are those we definitely added but weren't read
;                 lost        (set/difference adds final-read)

;                 ; Recovered records are those where we didn't know if the add
;                 ; succeeded or not, but we found them in the final set.
;                 recovered   (set/difference ok adds)]

;             {:valid?          (and (empty? lost) (empty? unexpected))
;              :ok              (util/integer-interval-set-str ok)
;              :lost            (util/integer-interval-set-str lost)
;              :unexpected      (util/integer-interval-set-str unexpected)
;              :recovered       (util/integer-interval-set-str recovered)
;              :ok-frac         (util/fraction (count ok) (count attempts))
;              :unexpected-frac (util/fraction (count unexpected) (count attempts))
;              :lost-frac       (util/fraction (count lost) (count attempts))
;              :recovered-frac  (util/fraction (count recovered) (count attempts))}))))))


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
                       (map (fn [x] {:type :invoke, :f :read, :value nil}))
                       ; (map (fn [x] {:type :invoke, :f :add, :value x}))
                       gen/seq
                       (gen/stagger 1/2))
       :final-generator (gen/each
                          ; First one wakes up the mongo client, second one reads
                          (gen/limit 1 {:type :invoke, :f :read-dbhash, :value nil}))
       :checker (checker/compose
                  {:set      checker/set
                   :timeline (timeline/html)
                   :perf     (checker/perf)})}
      opts)))
