(ns jepsen.bigchain
  (:gen-class)
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clj-http.client :as cl]
            [clojure.data.codec.base64 :as b64]
            [slingshot.slingshot :refer [try+]]
            [knossos.model :as model]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [timeout meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian])
  (:use [clojure.tools.nrepl.server :only (start-server stop-server)])
  )

(def dir     "/opt/bigchain")
(def binary  "sqldb")
(def logfile (str dir "/plutodb.log"))
(def pidfile (str dir "/plutodb.pid"))

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (str node ":38388"))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (str (node-url node 38388) "/v1/"))

(defn cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (peer-url node)))
       (str/join ",")))

(defn start!
  "Starts DB"
  [test node]
  (cu/start-daemon!
    {:logfile logfile
     :pidfile pidfile
     :chdir   dir}
    binary
    "--listen" ":38388"
    "--dbaddr" "mysql:3306"
    "--dbauth" "root:my-secret-pw"
    "--dbname" "plutodb"
    "--drop"
    )
  :started)

(defn stop!
  "Stops DB"
  [test node]
  (info node "tearing down plutodb")
  (cu/stop-daemon! binary pidfile)
    :stopped
  )

(defn kill!
  "Kills DB"
  [test node]
  (meh (c/su (c/exec :killall :-9 binary)))
  :killed)

(defn db
  "PlutoDB for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing plutodb" version "to" dir)
        (info  (let [url "http://control/plutos.tar"]
          (cu/install-archive! url dir)))

        (start! test node)

        (Thread/sleep 5000)))

    (teardown! [_ test node]
      (stop! test node))

    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(defn enc
  "encodes string to base64 bytes slice for json"
  [v]
  ; (String. (b64/encode (.getBytes (str v))) "UTF-8")
    (-> v
      str
      .getBytes
      b64/encode
      (String. "UTF-8"))
  )

(defn dec_
  "decodes base64 string"
  [v]
  (cond
    (nil? v) nil
    (clojure.string/blank? v) ""
    :else (-> v
      .getBytes
      b64/decode
      String.)))

(defn update-x
  "update if exists"
  [v k f]
  (if (get v k)
    (update v k f)
    v))

(defn enckv
  "encodes {:key, :value} item"
  [v]
  (-> v
    (update-x :key enc)
    (update-x :value enc)
    ))

(defn enclist
  "encodes list of {:key, :value} items"
  [l] 
    (map enckv l)
  )

(defn deckv
  "decodes item"
  [v]
  (-> v
    (update-x :key dec_)
    (update-x :value dec_)
    ))

(defn declist
  "decodes list of items"
  [l]
    (map deckv l)
  )

(defn req
  "makes request to graceful api"
  [cm addr k v]
  ;(info "REQUEST" (str addr k) v)
  (let [resp (cl/post (str addr k)
                      {:connection-manager cm
                       :content-type :json
                       :conn-timeout 1000
                       :form-params v
                       :as :json
                       })
        ans {:resp (-> resp
                     :body)
             :status (:status resp)}]
    ;(info "REQUEST" (str addr k) v (:status resp) ans)
    ans
    ))

(defn push
  "make push request"
  [cm addr sender & ids]
  (let [v {:txns (into [] (map (fn [id] {:sender sender :id id :receiver (+ 1000000 sender id) :amount id}) ids))}
        resp (req cm addr "Push" v)]
    resp))

(defn fetch
  "make fetch request"
  [cm addr account]
  (req cm addr "Fetch" {:account account :limit 1}))

(defn k5
  [id]
  (map (fn [x] (str (char (* x (/ 256 5))) id)) (range 5)))

(defn dbpush
  "push to executor endpoint"
  [cm addr acc id]
  (let [encid (enc id)
        n 5
        ks (k5 acc)
        v {:ops (into [] (map (fn [x] {:op 1 :key (enc x) :value encid}) ks))}
        resp (req cm addr "Exec" v)]
    resp))

(defn dbfetch
  "fetch from executor endpoint"
  [cm addr acc]
  (let [n 5
        ks (k5 acc)
        v {:ops (into [] (map (fn [x] {:op 0 :key (enc x)}) ks))}
        resp (req cm addr "Query" v)
        ids (map (fn [x] (-> x :value dec_ parse-long)) (-> resp :resp :results))
        eq (when (pos? (count ids)) (apply = ids))]
    [resp eq ids]))

(defn client
  "A client for a single compare-and-set register"
  [addr cm]
  ;(info "CLIENT CONSTRUCTOR" addr cm)
  (reify client/Client
    (setup! [_ test node]
      (client (client-url node) (clj-http.conn-mgr/make-reusable-conn-manager {:timeout 2 :threads 5})))

    (invoke! [this test op]
      (let [[k v] (:value op)
            crash (if (= :read (:f op)) :fail :info)
            op (assoc op :addr addr)]
        (try+
          (case (:f op)
            :read (let [
                        ;[resp eq ids] (dbfetch cm addr k)
                        resp (fetch cm addr k)
                        status (-> resp :resp :status)
                        code (or (-> status :code) 0)
                        ;v (-> resp :resp :results first :value dec_ parse-long)
                        v (-> resp :resp :txns first :ID)
                        ;_ (when-not (nil? eq) (info "READ RESP" eq ids))
                        ]
                    (if (zero? code)
                      (assoc op :type :ok, :value (independent/tuple k v))
                      (assoc op :type :fail, :error status)
                      ))

            :write (let [resp (push cm addr k v)
                         status (-> resp :resp :status)
                         code (or (-> status :code) 0)
                         ;_ (info "WRITE RESP" code status resp)
                         ]
                     (if (zero? code)
                       (assoc op :type :ok)
                       (assoc op :type :info, :error status)
                       ))
            )

          (catch java.net.SocketTimeoutException e
            (assoc op :type crash, :error :timeout))

          (catch (and (instance? clojure.lang.ExceptionInfo %)) e
            (assoc op :type crash :error e))

          (catch (and (:errorCode %) (:message %)) e
            (assoc op :type crash :error e)))))

    (teardown! [_ test]
      (clj-http.conn-mgr/shutdown-manager cm))))

(def id (atom 0))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (swap! id inc)})
;(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn repeat-generator
  "repeat generator"
  [& g]
  (gen/seq (cycle g)))

(defn kwsuff
  "Adds suffix to keyword"
  [kw s]
  (keyword (str (name kw) (name s))))

(defn start-stop-nemesis
  "produces :start :stop nemesis"
  [name]
  (repeat-generator
    {:type :info, :f (kwsuff name "-start")}
    {:type :info, :f (kwsuff name "-stop")}
    )
  )

(defn killer-nemesis
  "kill node nemesis"
  [process]
  (nemesis/node-start-stopper rand-nth
                      (fn start [t n]
                        (kill! t n)
                        [:killed process])
                      (fn stop [t n]
                        (start! t n)
                        [:restarted process]))
  )

(defn plutodb-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name "bigchain"
          :os jepsen.os/noop
          :db (db "dev")
          :client (client nil nil)
          :nemesis (nemesis/compose
                     {{:part-start :start, :part-stop :stop} (nemesis/partition-random-halves)
                     {:hammer-start :start, :hammer-stop :stop} (nemesis/hammer-time "plutodb")
                     {:killer-start :start, :killer-stop :stop} (killer-nemesis "plutodb")
                     #{:scramble-clocks} (nemesis/clock-scrambler 5)
                     })
          :model  (model/cas-register)
          :checker (checker/compose
                     {:perf     (checker/perf)
                      :indep (independent/checker
                               (checker/compose
                                 {:timeline (timeline/html)
                                  :linear   (checker/linearizable)}))
                      })
          :generator (gen/concat
                       (->> (independent/concurrent-generator
                              5
                              (range 5)
                              (fn [k]
                                (->> (gen/mix [r w])
                                  (gen/stagger 1/30)
                                  (gen/limit 300))))
                         (gen/nemesis
                           (repeat-generator
                             (gen/sleep 3)
                             (gen/mix [
                                       (start-stop-nemesis :part)
                                       (start-stop-nemesis :hammer)
                                       (start-stop-nemesis :killer)
                                       ;(repeat-generator {:type :info, :f :scramble-clocks})
                                       ])
                             )
                           )
                         (gen/time-limit (:time-limit opts)))
                       (gen/nemesis
                         (gen/seq [
                                   {:type :info, :f :part-stop}
                                   {:type :info, :f :hammer-stop}
                                   ]))
                       )
          }
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn plutodb-test})
                   (cli/serve-cmd))
            args))
