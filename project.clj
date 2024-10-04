(defproject com.oscaro/ds-test-tools "0.2.2-SNAPSHOT"
  :description "Tools to test Datasplash pipelines"
  :url "https://github.com/oscaro/ds-test-tools"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.0"]
                 [datasplash          "0.7.23"]
                 [com.oscaro/tools-io "0.3.26"]]
  :deploy-repositories [["snapshots" {:url "https://repo.clojars.org"
                                      :username :env/clojars_username
                                      :password :env/clojars_password
                                      :sign-releases false}]
                        ["releases"  {:url "https://repo.clojars.org"
                                      :username :env/clojars_username
                                      :password :env/clojars_password
                                      :sign-releases false}]]
  :aot :all
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :plugins [[lein-codox "0.10.2"]]
                   :dependencies [[org.clojure/tools.namespace "1.5.0"]]
                   :source-paths ["dev"]}})
