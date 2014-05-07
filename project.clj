(defn get-version
  []
  (let [version-fn (try
                     (load-file "src/frost/version.clj")
                     (catch java.io.FileNotFoundException e
                       ; Fix for CCW working directory bug.
                       (load-file "workspace/frost/src/frost/version.clj")))]
    (version-fn)))


(defproject frost (get-version)
  :min-lein-version "2.0.0"
  :description "frost is a library for binary serialization of Clojure data structures."
  :url "https://github.com/guv/frost"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clojure.options "0.2.9"]
                 ; keep kryo 2.17 since there is a bug in the "reference instead of copy" implementation in later kryo (TODO: check most recent version)
                 [com.esotericsoftware.kryo/kryo "2.17"]
                 ; alternative fast compression
                 [org.xerial.snappy/snappy-java "1.0.5"]
                 ; gui for analysis namespace
                 [clj-gui "0.3.3"]]
  :profiles
  {:dev {:dependencies [[clj-debug "0.7.3"]
                        [midje "1.6.3"]
                        [org.clojure/test.check "0.5.7"]]}
   :reflection {:warn-on-reflection true}}
  
  :source-paths ["src"]
)
