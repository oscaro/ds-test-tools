(defproject com.oscaro/ds-test-tools "0.1.1-SNAPSHOT"
  :description "Tools to test Datasplash pipelines"
  :url "https://github.com/oscaro/ds-test-tools"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [datasplash          "0.6.4"]
                 [com.oscaro/tools-io "0.3.17"]]
  :deploy-repositories [["releases" {:url "https://clojars.org/repo"}]]
  :aot :all
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :plugins [[lein-codox "0.10.2"]]
                   :dependencies [[org.clojure/tools.namespace "0.2.11"]]
                   :source-paths ["dev"]}})
