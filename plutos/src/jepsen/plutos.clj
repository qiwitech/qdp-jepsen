(ns jepsen.plutos
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
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian])
  (:use [clojure.tools.nrepl.server :only (start-server stop-server)])
  )

(def dir     "/opt/plutos")

(defn node-url
  "An HTTP url for connecting to a node on a particular port."
  [node port]
  (str "http://" (name node) ":" port))

(defn peer-url
  "The HTTP url for other peers to talk to a node."
  [node]
  (node-url node 32377))

(defn client-url
  "The HTTP url clients use to talk to a node."
  [node]
  (str (node-url node 38388) "/db/"))

(defn cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:2380,bar=bar:2380,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (peer-url node)))
       (str/join ",")))

(defn db
  "Plutos for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        (info node "installing plutos" version "to" dir)
        (info  (let [url "http://control/plutos.tar"]
          (cu/install-archive! url dir)))

        (let [binary "sqldb"
              base (str dir "/" binary)
              ]
          (cu/start-daemon!
            {:logfile (str base ".log")
             :pidfile (str base ".pid")
             :chdir   dir}
            binary
            "--listen" ":38388"
            "--dbaddr" "mysql:3306"
            "--dbauth" "root:my-secret-pw"
            "--dbname" "plutodb"
            "--drop"
            ))

        (let [binary "plutos"
              base (str dir "/" binary)
              ]
          (cu/start-daemon!
            {:logfile (str base ".log")
             :pidfile (str base ".pid")
             :chdir   dir}
            binary
            "--listen" (str node ":31337")
            "--nodes" "n1:31337,n2:31337,n3:31337,n4:31337,n5:31337"
            "--db" ":38388"
            ))

        (let [binary "plutoapi"
              base (str dir "/" binary)
              ]
          (cu/start-daemon!
            {:logfile (str base ".log")
             :pidfile (str base ".pid")
             :chdir   dir}
            binary
            "--listen" ":9090"
            "--gate" (str node ":31337")
            "--plutodb" ":38388"
            ))

        (Thread/sleep 20000)))

    (teardown! [_ test node]
      (info node "tearing down plutos")
      (cu/stop-daemon! "plutoapi" (str dir "/plutoapi.pid"))
      (cu/stop-daemon! "plutos" (str dir "/plutos.pid"))
      (cu/stop-daemon! "plutodb" (str dir "/plutodb.pid"))
      ;(c/su
      ;  (c/exec :rm :-rf dir))
      )

    db/LogFiles
    (log-files [_ test node]
      [(str dir "/plutodb.log") (str dir "/plutos.log") (str dir "/plutoapi.log")])))

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
  (info "REQUEST" (str addr k) v)
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
    (info "REQUEST" (str addr k) v (:status resp) ans)
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

(defn transfer
  "make transfer"
  [h s addr acc]
  (let [binary (str dir "/plutoclient")]
    (c/with-session h s
      (c/exec binary "transfer" (str acc) 1 0))
    ))

(defn lasthash
  "get account last hash"
  [h s addr acc]
  (let [binary (str dir "/plutoclient")]
    (c/with-session h s
      (c/exec binary "prevhash" (str acc)))
    ))

(defn client
  "A client for a single compare-and-set register"
  [addr node]
  ;(info "CLIENT CONSTRUCTOR" addr cm)
  (reify client/Client
    (setup! [_ test node]
      (client (client-url node) node))

    (invoke! [this test op]
      (let [[k v] (:value op)
            crash (if (= :read (:f op)) :fail :info)
            op (assoc op :addr addr)
            session (get (:sessions test) node)]
       ; (info "TeSt", (keys test) (:nodes test) (:sessions test))
        (try+
          (case (:f op)
            :read (let [
                        resp (lasthash node session addr k)
                        _ (info "read" resp)
                        status (-> resp :resp :status)
                        code (or (-> status :code) 0)
                        v (-> resp :resp :results first :value dec_ parse-long)
                        ;v (-> resp :resp :txns first :ID)
                        ;_ (when-not (nil? eq) (info "READ RESP" eq ids))
                        ]
                    (if (zero? code)
                      (assoc op :type :ok, :value (independent/tuple k v))
                      (assoc op :type :fail, :error status)
                      ))

            :write (let [resp (transfer node session addr k)
                         _ (info "wrte" resp)
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
      )))

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

(defn plutos-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name "plutos"
          :os jepsen.os/noop
          :db (db "dev")
          :client (client nil nil)
          :nemesis (nemesis/compose
                     {{:part-start :start, :part-stop :stop} (nemesis/partition-random-halves)
                     {:hammer-db-start :start, :hammer-db-stop :stop} (nemesis/hammer-time "plutodb")
                     {:hammer-proc-start :start, :hammer-proc-stop :stop} (nemesis/hammer-time "plutos")
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
                                  (gen/limit 200))))
                         (gen/nemesis
                           (repeat-generator
                             (gen/sleep 5)
                             (gen/mix [
                                       (start-stop-nemesis :part)
                                       (start-stop-nemesis :hammer-db)
                                       (start-stop-nemesis :hammer-proc)
                                       (repeat-generator {:type :info, :f :scramble-clocks})
                                       ])
                             )
                           )
                         (gen/time-limit (:time-limit opts)))
                       (gen/nemesis
                         (gen/seq [
                                   {:type :info, :f :part-stop}
                                   {:type :info, :f :hammer-db-stop}
                                   {:type :info, :f :hammer-proc-stop}
                                   ]))
                       )
          }
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn plutos-test})
                   (cli/serve-cmd))
            args))
