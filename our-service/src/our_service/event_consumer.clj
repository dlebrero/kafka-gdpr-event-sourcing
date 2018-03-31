(ns our-service.event-consumer
  (:require
    [our-service.util :as util]
    [our-service.common :as common]
    [clojure.tools.logging :as log])
  (:import
    (org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder KeyValue)
    (org.apache.kafka.common.serialization Serdes)
    (org.apache.kafka.streams.state Stores KeyValueStore)
    (our_service.util EdnSerde)
    (org.apache.kafka.streams.processor ProcessorContext)
    (org.apache.kafka.streams.kstream KStream)))

;;;
;;; Application
;;;

(defn lexicographic-ordered-key [k partition offset]
  (format "%s-%04d-%030d" k partition offset))

(defn encryption-key-msg [^KeyValueStore missing-store
                          ^KeyValueStore encryption-keys-store
                          ^ProcessorContext ctx
                          k
                          encryption-key]
  (.put encryption-keys-store k encryption-key)
  (let [encrypted-items (.range missing-store
                                (lexicographic-ordered-key k 0 0)
                                (lexicographic-ordered-key k Integer/MAX_VALUE Integer/MAX_VALUE))]
    (doseq [^KeyValue encrypted-item (iterator-seq encrypted-items)]
      (.delete missing-store (.key encrypted-item))
      (when-not (common/tombstone? encryption-key)
        (.forward ctx k [encryption-key (.value encrypted-item)])))
    (when (common/tombstone? encryption-key)
      (.forward ctx k [encryption-key nil]))))

(defn encrypted-data-msg [^KeyValueStore missing-store
                          ^KeyValueStore encryption-keys-store
                          ^ProcessorContext ctx
                          k
                          encrypted-item]
  (if-let [encryption-key (.get encryption-keys-store k)]
    (.forward ctx k [encryption-key encrypted-item])
    (do
      (log/info "missing" k)
      (.put missing-store (lexicographic-ordered-key k (.partition ctx) (.offset ctx)) encrypted-item))))


(defn ^KStream decrypt [^StreamsBuilder builder topic]
  (let [encryption-keys-store-name "encryption-keys-local"
        encryption-keys-store (-> (Stores/keyValueStoreBuilder (Stores/persistentKeyValueStore encryption-keys-store-name)
                                                               (Serdes/String) (EdnSerde.))
                                  .withCachingEnabled
                                  .withLoggingDisabled)

        waiting-for-encryption-keys-store-name "waiting-for-encryption-keys-local"
        waiting-for-encryption-keys-store (-> (Stores/keyValueStoreBuilder
                                                (Stores/persistentKeyValueStore waiting-for-encryption-keys-store-name)
                                                (Serdes/String) (EdnSerde.))
                                              .withCachingEnabled)]
    (-> builder
        (.addStateStore encryption-keys-store)
        (.addStateStore waiting-for-encryption-keys-store)

        (.stream ["encryption-keys" topic])
        (util/transform (fn [wait-for-store encryption-keys-store ^ProcessorContext ctx k value]
                          (let [topic (.topic ctx)
                                f (if (= topic "encryption-keys") encryption-key-msg encrypted-data-msg)]
                            (f wait-for-store encryption-keys-store ctx k value)
                            nil))
                        waiting-for-encryption-keys-store-name encryption-keys-store-name)

        (.mapValues (util/val-mapper [encryption-key encrypted-item]
                                     (cond
                                       (common/tombstone? encryption-key)
                                       encryption-key

                                       (= encryption-key (:encryption-key encrypted-item))
                                       (:val encrypted-item)

                                       :else
                                       (throw (RuntimeException. (str "Encryption key for " encrypted-item " do not match '" encryption-key "'")))))))))
;;;
;;; Create topology, but do not start it
;;;
(defn create-kafka-stream-topology []
  (let [^StreamsBuilder builder (StreamsBuilder.)
        decrypted (-> builder
                      (decrypt "user-info.encrypted")
                      (.groupByKey)
                      (.reduce (util/reducer [v1 v2]
                                 (if (or (common/tombstone? v1)
                                         (common/tombstone? v2))
                                   common/tombstone
                                   (str v1 "," v2)))))]
    [builder decrypted]))

(defn start-kafka-streams []
  (let [[builder decrypted] (create-kafka-stream-topology)
        _ (.print decrypted)
        kafka-streams (KafkaStreams. (.build builder) (util/kafka-config {StreamsConfig/APPLICATION_ID_CONFIG "example-consumer"}))]
    (.start kafka-streams)
    kafka-streams))