(ns ds-test-tools.local-save-intermediate
  (:require
   [clojure.java.io :as io]
   [clojure.pprint :as pp]
   [clojure.string :as string]
   [datasplash.api :as ds]
   [datasplash.core :as ds-core]
   [tools.io :as tio])
  (:import
   (org.apache.beam.sdk.transforms PTransform)
   (org.apache.beam.sdk.values PCollection PCollectionView)))

(def ^:dynamic *dir* nil)
(def ^:dynamic *names* #{})
(def ^:dynamic *prevent-loop* nil)
(def ^:dynamic *all-names-encountered* nil)

;; Obviosuly, this has drawbacks. But datasplash.core/apply-transform is hardly ever modified.

(defn apply-transform
  "A modified version of `datasplash.core/apply-transform` that will save intermediate input"
  [pcoll ^PTransform transform schema
   {:keys [coder coll-name side-outputs checkpoint] :as options}]
  (let [nam (some-> options (:name) (name))
        clean-opts (dissoc options :name :coder :coll-name)
        configured-transform (ds-core/with-opts schema clean-opts transform)
        bound (ds-core/tapply pcoll nam configured-transform)
        rcoll (if-not side-outputs
                (-> bound
                    (cond-> coder (.setCoder coder))
                    (cond-> coll-name (.setName coll-name)))
                (let [pct (ds-core/pcolltuple->map bound)]
                  (if coder
                    (do
                      (doseq [^PCollection pcoll (vals pct)]
                        (.setCoder pcoll coder))
                      pct)
                    pct)))]
    ;; this `checkpoint` feature that is already in datasplash, is similar but different
    ;; you specify the transform that need to be checkpointed inline
    ;; it is meant for live data in production, and only a few checkpoints
    (when checkpoint
      (ds-core/write-edn-file checkpoint rcoll))
    (when *all-names-encountered*
      (swap! *all-names-encountered* conj (:name options)))
    (when (or (and (= *names* :all)
                   (-> options :name keyword?)
                   (not *prevent-loop*))
              (-> options :name *names*))
      (binding [*prevent-loop* true]
        (let [coerce-to-pcoll (fn [pcoll]
                                (cond
                                  (instance? PCollection pcoll)
                                  pcoll
                                  ;; documented as internal use only...
                                  ;; seem to work in the direct runner at least
                                  ;; which is where we want to use it
                                  ;; Another way would be to create an empty input, and
                                  ;; use map or other transform were we can get side-inputs in.
                                  ;; (we would need to use .getPipeline to get to the start)
                                  (instance? PCollectionView pcoll)
                                  (.getPCollection pcoll)

                                  :else nil))
              in-pcoll (coerce-to-pcoll pcoll)
              out-pcoll (coerce-to-pcoll rcoll)]
          (when in-pcoll
            (ds-core/write-edn-file (str *dir* "/" nam ".in")
                                    {:num-shards 1 :prevent-loop true}
                                    in-pcoll))
          (when out-pcoll
            (ds-core/write-edn-file (str *dir* "/" nam ".out")
                                    {:num-shards 1 :prevent-loop true}
                                    out-pcoll)))))
    rcoll))

(defn- format-and-rename [dir]
  (->> (tio/list-files dir)
       (filter #(re-find #"-00000-of-00001" %))
       (reduce (fn [_ filename]
                 (let [new-name (string/replace filename "-00000-of-00001" ".edn")
                       edns (tio/read-edns-file filename)]
                   (when (seq edns)
                     (with-open [w (io/writer new-name)]
                       (reduce (fn [_ edn]
                                 (pp/pprint edn w))
                               nil
                               edns))))
                 (io/delete-file filename))
               nil))
  (io/delete-file (tio/join-path dir ".temp-beam") :silently))


(defn save!
  "Calls `f`, saving in/out of every transform in `names` to `dir`."
  [f names dir]
  (let [tilde-expanded (tio/expand-home dir)]
    (binding [*names* names
              *dir* tilde-expanded]
      (with-redefs-fn {#'ds-core/apply-transform apply-transform} f))
    (format-and-rename tilde-expanded))
  :done)

(defn list-transforms
  [f]
  (binding [*all-names-encountered* (atom [])]
    (with-redefs-fn {#'ds-core/apply-transform apply-transform} f)
    @*all-names-encountered*))


