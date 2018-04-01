(ns our-service.encryptor
  (:require
    [our-service.util :as util]
    [our-service.common :as common]
    [clojure.tools.logging :as log])
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

(defn generate-encryption-key []
  (rand-int 10000000))

(defn get-or-create-encryption-key [^KeyValueStore store ^ProcessorContext ctx k]
  (if-let [encryption-key (.get store k)]
    encryption-key
    (let [new-encryption-key (generate-encryption-key)]
      (.put store k new-encryption-key)
      (.forward ctx k new-encryption-key "encryption-keys")
      new-encryption-key)))

(defn ^String encrypted-topic-name [^String topic]
  (str (.substring topic 0 (clojure.string/last-index-of topic ".")) ".encrypted"))

(defn encrypt [^KeyValueStore store ^ProcessorContext ctx k v]
  (let [encryption-key (get-or-create-encryption-key store ctx k)
        rc ^RecordCollector (.recordCollector ^RecordCollector$Supplier ctx)]
    (if (common/tombstone? encryption-key)
      (log/info "Ignoring messages for a user that wanted to be forgotten" k)
      (.send rc
             (encrypted-topic-name (.topic ctx))
             k
             {:val            v
              :encryption-key encryption-key}
             ^Long (.timestamp ctx)
             (-> ctx .keySerde .serializer)
             (-> ctx .valueSerde .serializer)
             nil))))

(defn handle-gdpr [^KeyValueStore store ^ProcessorContext ctx k _]
  (.put store k common/tombstone)
  (.forward ctx k common/tombstone))

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

                    (.addSource "gdpr" (into-array ["gdpr"]))
                    (.addProcessor "gdpr-processor"
                                   (util/processor #'handle-gdpr "encryptor-state")
                                   (into-array ["gdpr"]))

                    (.addStateStore store (into-array ["encryptor-processor" "gdpr-processor"]))
                    (.addSink "encryption-keys" "encryption-keys" (into-array ["encryptor-processor" "gdpr-processor"])))]
    builder))

(defn start-kafka-streams []
  (let [builder (create-kafka-stream-topology)
        kafka-streams (KafkaStreams. ^TopologyBuilder builder
                                     (util/kafka-config {StreamsConfig/APPLICATION_ID_CONFIG       "encryptor"
                                                         StreamsConfig/PROCESSING_GUARANTEE_CONFIG "exactly_once"}))]
    (.start kafka-streams)
    kafka-streams))