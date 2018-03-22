(ns our-service.util
  (:require [clojure.tools.logging :as log]
            [franzy.serialization.deserializers :as deserializers]
            [franzy.serialization.serializers :as serializers])
  (:import (org.apache.kafka.streams.kstream Reducer KeyValueMapper Predicate ValueJoiner ValueMapper KStreamBuilder KStream Transformer TransformerSupplier)
           (java.net Socket)
           (kafka.admin AdminUtils)
           (kafka.utils ZkUtils)
           (org.I0Itec.zkclient ZkClient)
           (org.apache.kafka.streams.processor ProcessorSupplier Processor ProcessorContext)
           (org.apache.kafka.streams KafkaStreams StreamsConfig)
           (org.apache.kafka.common.serialization Serializer Serde Serdes)
           (org.apache.kafka.clients.consumer ConsumerConfig)
           (java.util Properties)))

(defmacro reducer [kv & body]
  `(reify Reducer
     (apply [_# ~(first kv) ~(second kv)]
       ~@body)))

(defmacro kv-mapper [kv & body]
  `(reify KeyValueMapper
     (apply [_# ~(first kv) ~(second kv)]
       ~@body)))

(defmacro val-mapper [v & body]
  `(reify ValueMapper
     (apply [_# ~v]
       ~@body)))

(defmacro pred [kv & body]
  `(reify Predicate
     (test [_# ~(first kv) ~(second kv)]
       ~@body)))


(defmacro val-joiner [kv & body]
  `(reify ValueJoiner
     (apply [_# ~(first kv) ~(second kv)]
       ~@body)))

(defn for-ever
  [msg thunk]
  (loop []
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (log/info msg)
                        (log/debug e msg)
                        (Thread/sleep 3000)))]
      (result 0)
      (recur))))

(defn wait-for-kafka [host port]
  (for-ever "waiting for kafka"
    #(with-open [_ (Socket. host (int port))]
       true)))

(defn wait-for-topic [topic]
  (for-ever "waiting for topics"
    #(let [zk (ZkUtils/createZkClientAndConnection "zoo1:2181" 10000 10000)]
       (with-open [^ZkClient zk-client (._1 zk)]
         (when-not (AdminUtils/topicExists (ZkUtils. zk-client (._2 zk) false) topic)
           (log/info "Topic" topic "not created yet")
           (throw (RuntimeException.)))))))


;;;
;;; Serialization stuff
;;;

(deftype NotSerializeNil [^Serializer edn-serializer]
  Serializer
  (configure [_ configs isKey] (.configure edn-serializer configs isKey))
  (serialize [_ topic data]
    (when data (.serialize edn-serializer topic data)))
  (close [_] (.close edn-serializer)))

;; Can be global as they are thread-safe
(def serializer (NotSerializeNil. (serializers/edn-serializer)))
(def deserializer (deserializers/edn-deserializer))

(deftype EdnSerde []
  Serde
  (configure [this map b])
  (close [this])
  (serializer [this]
    serializer)
  (deserializer [this]
    deserializer))

(defn ^Properties kafka-config [config]
  (doto
    (Properties.)
    (.put StreamsConfig/BOOTSTRAP_SERVERS_CONFIG "kafka1:9092")
    (.put StreamsConfig/ZOOKEEPER_CONNECT_CONFIG "zoo1:2181")
    ;(.put StreamsConfig/CACHE_MAX_BYTES_BUFFERING_CONFIG 0)
    (.put StreamsConfig/COMMIT_INTERVAL_MS_CONFIG 1000)
    (.put StreamsConfig/KEY_SERDE_CLASS_CONFIG (class (Serdes/String)))
    (.put StreamsConfig/VALUE_SERDE_CLASS_CONFIG EdnSerde)
    (.put ConsumerConfig/AUTO_OFFSET_RESET_CONFIG "earliest")
    (.putAll config)))

(defn log-all-message []
  (let [^KStreamBuilder builder (KStreamBuilder.)
        raw-data-stream ^KStream (.stream builder #".*")
        _ (.process raw-data-stream (reify ProcessorSupplier
                                      (get [_]
                                        (let [ctx (volatile! nil)]
                                          (reify Processor
                                            (init [_ context]
                                              (vreset! ctx context))
                                            (process [_ k v]
                                              (let [^ProcessorContext ctx1  @ctx]
                                                (println "MSG -" (.topic ctx1) ":" k "," v)))
                                            (punctuate [_ timestamp])
                                            (close [_])))))
                    (into-array String []))
        streams (KafkaStreams. builder (kafka-config {StreamsConfig/APPLICATION_ID_CONFIG "log-all"}))]
    (.start streams)
    streams))

(defn processor [f store-name]
  (reify ProcessorSupplier
    (get [_]
      (let [ctx (volatile! nil)
            state (volatile! nil)]
        (reify Processor
          (init [_ context]
            (vreset! ctx context)
            (vreset! state (.getStateStore context store-name)))
          (process [_ k v]
            (f @state @ctx k v))
          (punctuate [_ timestamp])
          (close [_]))))))

(defn transform
  (^KStream [builder f store1-name store2-name]
   (.transform builder
               (reify TransformerSupplier
                 (get [_]
                   (let [ctx (volatile! nil)
                         state1 (volatile! nil)
                         state2 (volatile! nil)]
                     (reify Transformer
                       (init [_ context]
                         (vreset! ctx context)
                         (vreset! state1 (.getStateStore context store1-name))
                         (vreset! state2 (.getStateStore context store2-name)))
                       (transform [_ k v]
                         (f @state1 @state2 @ctx k v))
                       (punctuate [_ timestamp])
                       (close [_])))))
               (into-array [store1-name store2-name]))))