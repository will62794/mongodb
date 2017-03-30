(ns jepsen.mongodb.rcmaj
  "Read concern majority test."
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.tools.logging :refer [info debug warn]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [jepsen
             [util :as util :refer [meh timeout]]
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.checker :as checker]
            [knossos [model :as model]
                     [op :as op]]
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
                    (m/with-read-concern read-concern) ;could be an option passed in
                    (m/with-write-concern write-concern))]
      (assoc this :client client, :coll coll, :dbase (m/db client db-name))))

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
; "Client."" is one way of constructing a "Client" record
(defn client
  "A set test client"
  [opts]
  (Client. "jepsen" "rcmaj"
           (:read-concern opts)
          (:write-concern opts)
          nil nil))

 
(defn subset-chain-acc 
  "Reducer for determining if a list of sets form an ordered chain of subsets."
  [acc x] 
  [x (and (second acc) (clojure.set/subset? (first acc) x))])

(defn subset-chain?
  "Determine if a list of sets form an ordered chain of subsets.

       (subset-chain? [#{1 2 3} #{1 2 3 4} #{1 2 3 4 5 6}]) == true
       (subset-chain? [#{4 5} #{4 5 6} #{4 5 8}]) == false
  " 
  [sets]
  (second (reduce subset-chain-acc [#{} true] sets)))

; Define our own custom checker to test read concern majority.
(def read-committed
  "Given a set of :add operations followed by intermittent :read ops, verifies that
  for every read that reads a set of elements D, any later read will read elements D*, where D 
  is a subset of D*."
  (reify checker/Checker
    (check [this test model history opts]
      (info (with-out-str (clojure.pprint/pprint (->> history
                                                      (filter op/ok?)
                                                      (filter #(= :read (:f %)))))))
      (let [reads (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :read (:f %)))
                          ; (sort-by :time)
                          (r/map :value))
            final-read (->> history
                          (r/filter op/ok?)
                          (r/filter #(= :read (:f %)))
                          (r/map :value)
                          (reduce (fn [_ x] x) nil))]
        (if-not final-read
          {:valid? :unknown
           :error  "Set was never read"}

          (let [; The OK set is every read value which we tried to add
                ok          #{}
                valid       (reduce subset-chain-acc [#{} true] reads)]

                ; Unexpected records are those we *never* attempted.
                ; unexpected  (set/difference final-read attempts)

                ; Lost records are those we definitely added but weren't read
                ; lost        (set/difference adds final-read)

                ; Recovered records are those where we didn't know if the add
                ; succeeded or not, but we found them in the final set.
                ; recovered   (set/difference ok adds)]
            {:valid?            (second valid)
             :read-commit       valid
             ; :ok              (util/integer-interval-set-str ok)
             ; :lost            (util/integer-interval-set-str lost)
             ; :unexpected      (util/integer-interval-set-str unexpected)
             ; :recovered       (util/integer-interval-set-str recovered)
             ; :ok-frac         (util/fraction (count ok) (count attempts))
             ; :unexpected-frac (util/fraction (count unexpected) (count attempts))
             ; :lost-frac       (util/fraction (count lost) (count attempts))
             :recovered-frac    nil}))))))


; Note: a normal Clojure object can act as a generator i.e. the value it generates is just itself.

; Write workload generator
(defn write-gen [avg-delay] (->> (range)
                                (map (fn [x] {:type :invoke, :f :add, :value x}))
                                gen/seq
                                (gen/stagger avg-delay)))

; Read workload generator
(defn read-gen [avg-delay] (->> (range)
                                (map (fn [x] {:type :invoke, :f :read, :value nil}))
                                gen/seq
                                (gen/stagger avg-delay)))

(defn test
  "A read concern majority test. We insert many documents while periodically reading the state of the collection."
  [opts]
  (test-
    "rcmaj"
    (merge
      {:client (client opts)
       :concurrency (count (:nodes opts))
       ;Execute reads on a single thread.
       :generator  (gen/reserve 29 (write-gen 1/2) 1 (read-gen 10))
       :final-generator nil
       :checker (checker/compose
                  {:set      read-committed ;checker/set
                   :timeline (timeline/html)
                   :perf     (checker/perf)})}
      opts)))
