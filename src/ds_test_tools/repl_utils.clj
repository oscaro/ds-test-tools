(ns ds-test-tools.repl-utils
  (:require
   [clojure.walk :as walk]
   [datasplash.api :as ds]
   [superstring.core :as str]
   [tools.io :as tio]))

(defn remove-0s [s]
  (str/chop-suffix s "-00000-of-00001"))

(defprotocol DW
  "Used internally by `direct`."
  (dwrite [this dir]))

(extend-protocol DW
  org.apache.beam.sdk.values.PCollection
  (dwrite [this dir]
    ;; name "pcoll" vs "view" is irrelevant for now,
    ;; but we might want to read them differently
    ;; in the future.
    (ds/write-edn-file (tio/join-path dir "pcoll") {:num-shards 1} this))
  org.apache.beam.sdk.values.PCollectionViews$SimplePCollectionView
  (dwrite [this dir]
    (->> (.getPCollection this)
         (ds/write-edn-file (tio/join-path dir "view") {:num-shards 1})))
  clojure.lang.ILookup
  (dwrite [this dir]
    (doseq [[k thing] this]
      (dwrite thing (tio/join-path dir (name k)))))
  clojure.lang.IPersistentVector
  (dwrite [this dir]
    (doseq [i (range (count this))]
      (dwrite (this i) (tio/join-path dir (str i))))))

(defn dread [dir]
  (let [ensure-java-io-file (if (instance? java.io.File dir)
                            dir
                            (java.io.File. dir))
        files (remove #(-> (tio/basename %) (.startsWith "."))
                      (.listFiles ensure-java-io-file))]
    (println dir)
    (println files)
    (cond
      ;; value
      (= 1 (count files))
      (tio/read-edns-file (first files))

      ;; vec
      (seq (filter #(-> (tio/basename %) (.startsWith "0"))
                   files))
      (mapv dread (sort files))

      ;; map
      :else
      (into {}
            (map (fn [filename]
                   [(-> (tio/basename filename) remove-0s keyword)
                    (dread filename)]))
            files))))

(defmacro direct
  "Inserts pipeline boilerplate.
  This is meant to be used at the repl, or in test namespaces.
  In the main code it makes more sense to write the boilerplate
  explicitely.

  Examples at the end of the docstring.

  The first argument is a bindings vector like in `let`.
  A `p (ds/make-pipeline [])` has been already been inserted.
  You could shadow it if you want (probably not), but you can't
  access a `p` from outer scope.
  The `side-inputs` symbol has a special meaning.
  If present, the corresponding map will be added
  as a `:side-inputs` key to the options argument
  of every `ds/map` (and similar) call in the main body.
  Furthermore, inside these calls, any symbol that has
  the same name as a key in the map gets expanded to
  the appropriate form that includes a call to
  `ds/side-inputs`. For example, if you write the
  bindings `side-inputs {:a a-view}`, then `a`
  inside a `ds/map` gets expanded to `(:a (ds/side-inputs))`
  See the full example below.

  The body forms then get wrapped into a (ds/->> p),
  (so in most cases you don't have `p` appear explicitely,
  but you stil have access to it if you need several input nodes)
  written to a temp file, then the pipeline is ran (blockingly),
  then the file is read and the contents returned.

  The last form can yield a pcoll, a PCollectionView, a map or
  a vec of such, even recursively.

  We do not care to remove the temp file afterwards,
  since it is the point of os-supplied temp files that
  they need not be explicitely deleted.

  Examples :

  (direct []
  (ds/generate-input [1 2 3])
  (ds/map inc))

  ;; => [4 2 3]
  ;; (of course, order of elements is not kept by BEAM)

  (direct []
  (ds/generate-input [1])
  (ds/view)
  :is-view)

  ;; => [1]
  ;; as you see, it would be better if we did it differenty
  ;; according to what type of view it is. Here in case
  ;; of a singleton view, we could unwrap it for instance.

  (direct [a 2]
  (ds/generate-input [1 2 3])
  (ds/map (partial + a)))

  ;; => [3 5 4]

  (direct [a-view (->> (ds/generate-input [2] p)
                     ds/view)
         side-inputs {:a a-view}]
  (ds/generate-input [1 2 3])
  ;; for some reason you can't use partial...
  (ds/map (fn [x] (+ x a))))

  ;; => [3 4 5]

  (direct [a-view (->> (ds/generate-input [2] p)
                     ds/view)
         side-inputs {:a a-view}]
  (ds/generate-input [1 2 3])
  (ds/map (fn [x] (+ a x))
          {:name :opt-map-already-present}))

  ;; => [5 3 4]


  (letfn [(ab [pcoll] {:a (ds/map inc pcoll)
                       :b (ds/map dec pcoll)})]
    (sut/direct []
      (ds/generate-input [1 2])
       ab)

  ;; => {:a [2 3] :b [0 1]}
  "
  {:style/indent 1}
  [& forms]
  (let [p-gen (gensym "p")
        forms (walk/postwalk (fn [form] (if (= 'p form) p-gen form))
                             forms)
        [binding-vec & forms] forms
        side-inputs (->> (partition 2 binding-vec)
                         (filter #(= 'side-inputs (first %)))
                         first
                         second)
        forms (cond->> forms
                side-inputs
                (walk/postwalk
                 (fn [form]
                   (cond
                     (and (symbol? form)
                          (side-inputs (keyword form)))
                     `(~(keyword form) (datasplash.api/side-inputs))
                     (and (list? form)
                          (#{'ds/map
                             'ds/map-kv
                             'ds/mapcat
                             'ds/filter
                             'ds/keep}
                           (first form)))
                     ;; if the ds/map is not in a ->>, user needs put
                     ;; a (possibly empty) options map. Because
                     ;; we'll insert one if not present, but we don't
                     ;; know where because we don't know if inside ->>
                     ;; or not.
                     (let [ensured-opts-map (cond-> form
                                              (not (some map? form))
                                              (concat '({})))]
                       (map (fn [subform]
                              (cond-> subform
                                (map? subform)
                                (update :side-inputs
                                        (fn [old-si-form]
                                          ;; can't use backtick
                                          ;; because it would namespace side-inputs
                                          (list 'merge 'side-inputs old-si-form)))))
                            ensured-opts-map))
                     :else form))))]


    `(tio/with-tempdir [tmp-dir#]
       (let [~p-gen (ds/make-pipeline [])
             ~@binding-vec]
         (dwrite (->> ~p-gen ~@forms) tmp-dir#)
         (clojure.test/is (= :done
                             (ds/wait-pipeline-result
                              (ds/run-pipeline ~p-gen))))
         (dread tmp-dir#)))))
