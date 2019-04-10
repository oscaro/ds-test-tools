(ns ds-test-tools.core-test
  (:require [clojure.test :refer :all]
            [ds-test-tools.core :as dtt]
            [datasplash.api :as ds]
            [tools.io :as tio])
  (:gen-class))


(deftest simple-pipeline-inc
  (let [job (fn [conf p]
              (->> p
                (ds/read-edn-file (:numbers conf))
                (ds/map inc)
                (ds/write-edn-file (str (:output conf) "/higher.edn"))))

        {:keys [result]} (dtt/run-pipeline
                           {:numbers [1 2 3 4]}
                           {:result "higher"}
                           job)]

    (is (= [2 3 4 5] (sort result)))))


(deftest simple-pipeline-inc-subdir-in
  (let [job (fn [conf p]
              (->> p
                (ds/read-edn-file (tio/join-path (:numbers conf) "subdir/numbers.edn*"))
                (ds/map inc)
                (ds/write-edn-file (tio/join-path (:output conf) "higher.edn"))))

        {:keys [result]} (dtt/run-pipeline
                           {:numbers {"subdir/numbers.edn" [1 2 3 4]}}
                           {:result "higher"}
                           job)]

    (is (= [2 3 4 5] (sort result)))))

(deftest simple-pipeline-inc-subdir-out
  (let [job (fn [conf p]
              (->> p
                (ds/read-edn-file (:numbers conf))
                (ds/map inc)
                (ds/write-edn-file (tio/join-path (:output conf) "subdir/higher.edn"))))

        {:keys [result]} (dtt/run-pipeline
                           {:numbers [1 2 3 4]}
                           {:result "subdir/higher"}
                           job)]

    (is (= [2 3 4 5] (sort result)))))

(deftest simple-pipeline-inc-subdir-in-out
  (let [job (fn test-job-inc-subdir-in [conf p]

              (let [odd (ds/read-edn-file (tio/join-path (:numbers conf) "odd/*") p)
                    even (ds/read-edn-file (tio/join-path (:numbers conf) "even/*") p)]

                (->> (ds/concat odd even)
                     (ds/write-edn-file (tio/join-path (:output conf) "subdir/all.edn")))))

        {:keys [result]} (dtt/run-pipeline
                           {:numbers {"odd/odd.edn" [1 3 5]
                                      "even/even.edn" [0 2 4]}}
                           {:result "subdir/all"}
                           job)]

    (is (= [0 1 2 3 4 5] (sort result)))))

(deftest simple-pipeline-multioutputs
  (let [job (fn [conf p]
              (let [numbers (ds/read-edn-file (:numbers conf) p)
                    incs (ds/map inc numbers)
                    decs (ds/map dec numbers)]
                (ds/write-edn-file (tio/join-path (:output-incs conf)) incs)
                (ds/write-edn-file (tio/join-path (:output-decs conf)) decs)))
        {:keys [output-incs output-decs]} (dtt/run-pipeline
                                           {:use-outputs-map true}
                                           {:numbers [1 2 3 4]}
                                           {:output-incs "subdir/incs.edn"
                                            :output-decs "subdir/decs.edn"}
                                           job)]

    (is (= [2 3 4 5] (sort output-incs)))
    (is (= [0 1 2 3] (sort output-decs)))))
