(ns our-service.main
  (:require
    [clojure.tools.nrepl.server :as nrepl]
    [our-service.util :as k]
    [our-service.event-consumer :as event-consumer]
    [our-service.encryptor :as encryptor]
    [our-service.event-producer :as event-producer]
    [ring.adapter.jetty :as jetty]
    [clojure.tools.logging :as log]
    [our-service.util :as util])
  (:use ring.middleware.params)
  (:gen-class))

(defonce state (atom {}))

(defn -main [& args]
  (nrepl/start-server :port 3002 :bind "0.0.0.0")
  (log/info "Waiting for kafka to be ready")
  (k/wait-for-kafka "kafka1" 9092)
  (log/info "Waiting for topics to be created")
  (k/wait-for-topic "user-info.encrypted")
  (Thread/sleep 5000)
  (log/info "Starting Kafka Streams")
  (let [consumer (event-consumer/start-kafka-streams)
        encryptor (encryptor/start-kafka-streams)
        logger (util/log-all-message)
        web (event-producer/api)]
    (reset! state {:consumer  consumer
                   :web       web
                   :encryptor encryptor
                   :logger    logger
                   :jetty     (jetty/run-jetty
                                (wrap-params web)
                                {:port  80
                                 :join? false})})))


(comment

  (.close (:encryptor @state))
  (def e (encryptor/start-kafka-streams))

  (.close (:consumer @state))
  (def c (event-consumer/start-kafka-streams))

  )