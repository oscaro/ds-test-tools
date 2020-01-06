(ns ds-test-tools.core
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [datasplash.api :as ds]
            [tools.io :as tio :refer [with-tempdir]])
  (:gen-class))

(defn- ->conf-path
  [x]
  (if (vector? x) x [x]))

(defn- jsons-reader [k_ filename] (tio/read-jsons-file filename))
(defn- edns-reader [k_ filename] (tio/read-edns-file filename))

(defn- jsons-writer [k_ filename data] (tio/write-jsons-file filename data))
(defn- edns-writer [k_ filename data] (tio/write-edns-file filename data))

(def ^:private readers
  {:jsons jsons-reader
   :edns edns-reader})

(def ^:private writers
  {:jsons jsons-writer
   :edns edns-writer})

(defn- mk-default-options
  []
  {:p (ds/make-pipeline {})
   :output-key :output
   :writer :edns
   :reader :edns})

(defn- make-multi-outputs
  [outputs output-dirname]
  (reduce
   (fn [m [output-name relative-path]]
     (let [path (tio/join-path output-dirname relative-path)]
       (do (io/make-parents path)
           (assoc m output-name path))))
   {}
   outputs))

(defn- make-output-keys
  [{:keys [output-key use-outputs-map] :as options_} outputs output-dirname]
  (if use-outputs-map
    (make-multi-outputs outputs output-dirname)
    {output-key output-dirname}))

(defn run-pipeline
  "Run a pipeline. It dumps inputs in temporary files as well as create a
   temporary directory for the output. It inject them in the config then run
   a pipeline and collect its outputs.


   Simple example:

      (defn my-job [conf p]
        (->> p
          (ds/read-edn-file (:numbers conf))
          (ds/map inc)
          (ds/write-edn-file (str (:output conf) \"/higher.edn\"))))


      (let [{:keys [result]} (run-pipeline
                               {:numbers [1 2 3 4]}
                               {:result \"higher\"}
                               my-job)]
        (println (sort result))) ; '(2 3 4 5)
"
  {:added "0.0.1"}
  ([inputs outputs body-fn]
   (run-pipeline (mk-default-options) inputs outputs body-fn))

  ([options inputs outputs body-fn]
   (let [{:keys [p writer reader base-conf] :as options} (merge (mk-default-options) options)

         writer (get writers writer writer)
         reader (get readers reader reader)]

     (with-tempdir [output-dirname]
                   (with-tempdir [input-dirname]
                                 ;; Build the conf
                                 (let [base-conf (merge base-conf (make-output-keys options outputs output-dirname))

                                       conf (reduce-kv (fn [acc k data]
                                                         (let [path (->conf-path k)
                                                               filename (tio/join-path input-dirname
                                                                                       (str/join "-" (map name path)))]

                                                           (if (map? data)
                                                             ;; directories
                                                             (doseq [[subpath data] data]
                                                               (let [full-path (tio/join-path filename subpath)]
                                                                 (io/make-parents full-path)
                                                                 (writer k full-path data)))

                                                             ;; single file
                                                             (writer k filename data))

                                                           (assoc-in acc path filename)))
                                                       base-conf inputs)]

                                   ;; Run the pipeline
                                   (body-fn conf p)
                                   (ds/wait-pipeline-result (ds/run-pipeline p))))

       ;; Collect outputs
       (reduce-kv (fn [acc k filename]
                    (let [extension? "(?:\\.\\w+)?"
                          shards "\\d+-of-\\d+"
                          file-pattern (re-pattern (str ".*/" filename extension? "-" shards ".*"))
                          content (doall (mapcat (fn [filename]
                                                   (reader k filename))
                                                 (->> (tio/join-path output-dirname filename)
                                                      (tio/list-files)
                                                      (filter #(re-matches file-pattern %1)))))]
                      (assoc acc k content)))
                  {} outputs)))))
