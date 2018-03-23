(ns our-service.event-producer
  (:require
    [franzy.serialization.serializers :as serializers]
    [franzy.clients.producer.client :as client]
    [franzy.clients.producer.protocols :as producer]
    [compojure.core :refer [routes ANY GET POST]]
    [our-service.common :as common])
  (:use ring.middleware.params))

(defn for-ever
  [thunk]
  (loop []
    (if-let [result (try
                      [(thunk)]
                      (catch Exception e
                        (println e)
                        (Thread/sleep 100)))]
      (result 0)
      (recur))))

(def kafka-client (delay
                    (client/make-producer {:bootstrap.servers "kafka1:9092"
                                           :acks              "all"
                                           :retries           1
                                           :client.id         "example-producer"}
                                          (serializers/keyword-serializer)
                                          (serializers/edn-serializer))))

(defn produce-edn [m]
  (let [value (assoc m :ts (System/currentTimeMillis))]
    (for-ever
      #(producer/send-async! @kafka-client value))))

(defn forget [client]
  (produce-edn {:topic "gdpr"
                :key   client
                :value common/tombstone}))

(defn set-data [client pii-data]
  (produce-edn {:topic "user-info.to-encrypt"
                :key   client
                :value pii-data}))

(defn api []
  (routes
    (POST "/set-data" [client name]
      (set-data client {:name name})
      {:status 200
       :body   (pr-str "done!")})
    (POST "/forget-me!" [client]
      (forget client)
      {:status 200
       :body   (pr-str "done!")})))

(comment

  (set-data "client1232123" "Lebrero")
  (forget "client1232123")
  (set-data "client12345222" "Lebrero")
  (let [x (System/currentTimeMillis)]
    (do
      (set-data (str "client3446" x) "Dan")
      (set-data (str "client3446" x) "Lebrero")
      ))

  (set-data "client123452222" "Lebrero")
  (produce-edn {:topic "encryption-keys"
                :key   "client123452222"
                :value "forget-me!"})
  )
