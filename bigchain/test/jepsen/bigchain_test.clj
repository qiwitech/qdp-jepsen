(ns jepsen.bigchain-test
  (:use [clojure.test]
        [jepsen.bigchain])
  )

(deftest parse-long-test
  (do
    (is (= nil (parse-long nil)))
    (is (= 1 (parse-long "1")))
    (is (= 10 (parse-long "10")))
    (is (= 0 (parse-long "0")))
    (is (= 100 (parse-long "100")))
    ))

(deftest enc-dec
  (do
    (is (= "" (-> "" enc)))
    (is (= "" (-> "" enc dec_)))
    (is (= "0" (-> "0" enc dec_)))
    (is (= "1" (-> "1" enc dec_)))
    (is (= "9" (-> "9" enc dec_)))
    (is (= "900" (-> "900" enc dec_)))
    (is (= "aaa.!1" (-> "aaa.!1" enc dec_)))
    ))

(deftest enckv-test
  (do
    (is (= {} (-> {} enckv)))
    (is (= {:key "MQ=="} (-> {:key "1"} enckv)))
    (is (= {:key "azE="} (-> {:key "k1"} enckv)))
    ))

(deftest enclist-test
  (do
    (is (= [] (-> [] enclist)))
    (is (= [{}] (-> [{}] enclist)))
    (is (= [{:key "a2V5"}] (-> [{:key "key"}] enclist)))
    (is (= [{:value "dmFsdWU="}] (-> [{:value "value"}] enclist)))
    (is (= [{:op 1, :key "a2V5", :value "dmFsdWU="}] (-> [{:op 1, :key "key", :value "value"}] enclist)))
    ))

(deftest deckv-test
  (do
    (is (= {} (-> {} deckv)))
    (is (= {:key "1"} (-> {:key "MQ=="} deckv)))
    (is (= {:key "k1"} (-> {:key "azE="} deckv)))
    ))

(deftest declist-test
  (do
    (is (= [] (-> [] declist)))
    (is (= [{}] (-> [{}] declist)))
    (is (= [{:key "key"}] (-> [{:key "a2V5"}] declist)))
    (is (= [{:value "value"}] (-> [{:value "dmFsdWU="}] declist)))
    (is (= [{:op 1, :key "key", :value "value"}] (-> [{:op 1, :key "a2V5", :value "dmFsdWU="}] declist)))
    ))

(deftest enc-dec-list
  (do
    (is (= [] (-> [] enclist declist)))
    (is (= [{}] (-> [{}] enclist declist)))
    (is (= [{:op 1}] (-> [{:op 1}] enclist declist)))
    (is (= [{:key "1"}] (-> [{:key "1"}] enclist declist)))
    (is (= [{:value "1"}] (-> [{:value "1"}] enclist declist)))
    (is (= [{:key "1"} {:value "2"}] (-> [{:key "1"} {:value "2"}] enclist declist)))
   ))
