(defproject jepsen.bigchain "0.1.0-SNAPSHOT"
  :description "A Jepsen test for plutodb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :main jepsen.bigchain
  :jvm-opts ["-Dcom.sun.management.jmxremote"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.6-SNAPSHOT"]
                 [org.clojure/data.codec "0.1.0"]
                 [clj-http "3.7.0"]
                 [cheshire "5.8.0"]
                 [debugger "0.2.0"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 ])
