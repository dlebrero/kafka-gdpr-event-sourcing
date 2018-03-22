(defproject our-service "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.apache.kafka/kafka-streams "1.0.0"]
                 [org.apache.kafka/kafka_2.12 "1.0.0" :exclusions [[org.slf4j/slf4j-log4j12]]]
                 [clj-kafka.franzy/core "2.0.7"]
                 [compojure "1.5.1"]
                 [ring/ring-jetty-adapter "1.5.0"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.1.7"]
                 [org.slf4j/jcl-over-slf4j "1.7.14"]
                 [org.slf4j/jul-to-slf4j "1.7.14"]
                 [org.slf4j/log4j-over-slf4j "1.7.14"]]
  :main our-service.main
  :aot [franzy.serialization.serializers]
  )
