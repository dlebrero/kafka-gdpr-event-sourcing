(ns our-service.common)

(def tombstone "forget-me!")

(defn tombstone? [val]
  (= "forget-me!" val))
