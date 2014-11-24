(defn get-version
  []
  (let [version-file "src/frost/version.clj"
        version-fn (try
                     ; works in leiningen
                     (load-file version-file)
                     (catch java.io.FileNotFoundException e
                       ; workaround for CCW (version number is not needed anyway)
                       (constantly "0.8.15-UNDEFINED")))]
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
                 [clj-gui "0.3.4"]]
  :profiles
  {:dev {:dependencies [[clj-debug "0.7.4"]
                        [midje "1.6.3"]
                        [org.clojure/test.check "0.5.7"]]}
   :reflection {:warn-on-reflection true}}
  
  :source-paths ["src"]
)
