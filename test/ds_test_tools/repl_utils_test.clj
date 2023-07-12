(ns ds-test-tools.repl-utils-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [datasplash.api :as ds]
   [ds-test-tools.repl-utils :as sut]))

(deftest direct-test
  (testing "most basic usage"
    (is (= #{2 3 4}
           (set (sut/direct []
                  (ds/generate-input [1 2 3])
                  (ds/map inc))))))

  (testing "view"
    (testing "singleton"
      (is (= #{1}
             (set (sut/direct []
                    (ds/generate-input [1])
                    (ds/view))))))
    (testing "map"
      (is (= {1 "1" 2 "2"}
             (into {} (sut/direct []
                        (ds/generate-input [1 2])
                        (ds/map-kv (fn [x] [x (str x)]))
                        (ds/view {:type :map})))))))

  (testing "normal let bindings"
    (is (= #{3 4 5}
           (set (sut/direct [a 2]
                  (ds/generate-input [1 2 3])
                  (ds/map (partial + a)))))))

  (testing "side-inputs"
    (testing "basic usage"
      (is (= #{3 4 5}
             (set (sut/direct [a-view (->> (ds/generate-input [2] p)
                                           ds/view)
                               side-inputs {:a a-view}]
                    (ds/generate-input [1 2 3])
                    ;; for some reason you can't use partial...
                    (ds/map (fn [x] (+ x a))))))))

    (testing "opt map already present"
      (is (= #{3 4 5}
             (set (sut/direct [a-view (->> (ds/generate-input [2] p)
                                           ds/view)
                               side-inputs {:a a-view}]
                    (ds/generate-input [1 2 3])
                    (ds/map (fn [x] (+ a x))
                            {:name :opt-map-already-present}))))))

    (testing "opt map already present, with side-inputs bindings"
      (is (= #{3 4 5}
             (set (sut/direct [a-view (->> (ds/generate-input [2] p)
                                           ds/view)
                               b-view (->> (ds/generate-input [4] p)
                                           ds/view)
                               side-inputs {:a a-view}]
                    (ds/generate-input [1 2 3])
                    (ds/map (fn [x] (+ a x))
                            {:name :opt-map-already-present
                             :side-inputs {:b b-view}})))))))

  (testing "last form yields a map"
    (letfn [(ab [pcoll] {:a (ds/map inc pcoll)
                         :b (ds/map dec pcoll)})]
      (is (= {:a #{2 3} :b #{0 1}}
             (update-vals (sut/direct []
                            (ds/generate-input [1 2])
                            ab)
                          set)))
      (sut/direct []
        (ds/generate-input [1 2])
        ab)))

  (testing "last form yields a vec"
    (letfn [(ab [pcoll] [(ds/map inc pcoll)
                         (ds/map dec pcoll)])]
      (is (= [#{2 3} #{0 1}]
             (mapv set
                   (sut/direct []
                     (ds/generate-input [1 2])
                     ab)))))))
