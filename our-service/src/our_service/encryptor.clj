(ns our-service.encryptor
  (:require
    [our-service.util :as util])
  (:import
    (org.apache.kafka.streams StreamsConfig KafkaStreams)
    (org.apache.kafka.streams.kstream KStreamBuilder)
    (org.apache.kafka.streams.state KeyValueStore Stores)
    (org.apache.kafka.streams.processor ProcessorContext TopologyBuilder)
    (org.apache.kafka.streams.processor.internals RecordCollector RecordCollector$Supplier)
    (our_service.util EdnSerde)))

;;;
;;; Application
;;;

(defn get-or-create-encryption-key [^KeyValueStore store ^ProcessorContext ctx k]
  (if-let [encryption-key (.get store k)]
    encryption-key
    (let [new-encryption-key (rand-int 10000000)]
      (.put store k new-encryption-key)
      (.forward ctx k new-encryption-key "encryption-keys")
      new-encryption-key)))

(defn ^String encrypted-topic-name [^String topic]
  (str (.substring topic 0 (clojure.string/last-index-of topic ".")) ".encrypted"))

(defn encrypt [^KeyValueStore store ^ProcessorContext ctx k v]
  (let [encryption-key (get-or-create-encryption-key store ctx k)
        rc ^RecordCollector (.recordCollector ^RecordCollector$Supplier ctx)]
    (.send rc
           (encrypted-topic-name (.topic ctx))
           k
           {:val            v
            :encryption-key encryption-key}
           ^Long (.timestamp ctx)
           (-> ctx .keySerde .serializer)
           (-> ctx .valueSerde .serializer)
           nil)))

(defn create-kafka-stream-topology []
  (let [^KStreamBuilder builder (KStreamBuilder.)
        store (-> (Stores/create "encryptor-state")
                  .withStringKeys
                  (.withValues (EdnSerde.))
                  .persistent
                  .enableCaching
                  .build)
        builder (-> builder
                    (.addSource "to-encrypt" #".*\.to-encrypt")
                    (.addProcessor "encryptor-processor"
                                   (util/processor #'encrypt "encryptor-state")
                                   (into-array ["to-encrypt"]))
                    (.addStateStore store (into-array ["encryptor-processor"]))

                    (.addSink "encryption-keys" "encryption-keys" (into-array ["encryptor-processor"])))]
    builder))

(defn start-kafka-streams []
  (let [builder (create-kafka-stream-topology)
        kafka-streams (KafkaStreams. ^TopologyBuilder builder
                                     (util/kafka-config {StreamsConfig/APPLICATION_ID_CONFIG       "encryptor"
                                                         StreamsConfig/PROCESSING_GUARANTEE_CONFIG "exactly_once"}))]
    (.start kafka-streams)
    kafka-streams))