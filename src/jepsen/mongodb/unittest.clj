(ns unittests
  (:require [jepsen.mongodb.rcmaj :as rcm])
  (:use clojure.test))

(deftest subset-chain-test
  (let [sets ])
  (is (= (rcm/subset-chain? [#{1 2 3} #{1 2 3 4} #{1 2 3 4 5 6}]) true))  
  (is (= (rcm/subset-chain? [#{1 2} #{1 2 3} #{1 2 5 6}]) false))