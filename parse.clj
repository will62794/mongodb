(require '[clojure.data.fressian :as fress])
(use '[clojure.java.io :as io])
(use 'clojure.string)

; Parse data from a Jepsen test run, which is stored in Fressian binary format

(def test_runs_dir "store/mongodb set s:wiredTiger p:1/")
(def f (io/file test_runs_dir))
(def filedirs (file-seq f))

(defn isFressian [filepath]
  (ends-with? filepath "test.fressian")
)

(def fressian_test_files (filter isFressian filedirs))

;Read a fressian data file into a native record
(defn read_fressian_test [test_file]
  (def testObj (fress/read (io/input-stream test_file)))
  {:time-limit (:time-limit testObj), :valid? (:valid? (:results testObj))}
)

(def all_results (map read_fressian_test fressian_test_files))

;(prn all_results)

;Count percentage of failures at a certain time limit 
(defn pct_errs [results timelimit] 
  (defn timelimit-filter [el] (= (:time-limit el) timelimit))
  (defn valid-filter [el] (false? (:valid? el)))
  (def errs (count (filter (fn [el] (and (timelimit-filter el) (valid-filter el))) results)))
  (def total (count (filter timelimit-filter results)))
  (def pct_errs (float (/ errs (max 1 total))))
  {:timelimit timelimit :errs errs :total total :pct_errs pct_errs}
)

(def time-limits [200 400 600 800 1000 1200 1400])
(def results (map (partial pct_errs all_results) time-limits))

(pprint results)

;(def test_file "store/mongodb set s:wiredTiger p:1/20170323T161048.000-0400/test.fressian")
;(def test_data (fress/read (io/input-stream test_file)))
;(prn (keys test_data))
;(prn "Time limit: " (:time-limit test_data))
;(prn "Valid?: "(:valid? (:results test_data)))
