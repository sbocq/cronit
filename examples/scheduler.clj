(ns scheduler
  (:require [sbocq.cronit :as c])
  (:import [java.util.concurrent Executors TimeUnit]
           [java.time Duration ZonedDateTime]
           [java.time.format DateTimeFormatter]))

(defn run [{:keys [timers]}]
  (let [formatter (DateTimeFormatter/ofPattern "mm:ss")
        executor (Executors/newScheduledThreadPool 1)
        prompt-time (atom nil)]
    (letfn [(log [time s]
              ;; reuse the same prompt for logs that have the same time.
              (let [[old new] (swap-vals! prompt-time (constantly (.toEpochSecond time)))]
                (when-not (= old new)
                  (when old (println ""))
                  (print (.format time formatter) ""))
                (print s)))
            (command [cronit s]
              (fn []
                (when (c/valid? cronit)
                  (log (:current cronit) s))
                (let [cronit (c/next cronit)]
                  (.schedule executor
                             (command cronit s)
                             (-> (Duration/between (ZonedDateTime/now) (:current cronit))
                                 .toMillis)
                             TimeUnit/MILLISECONDS))))]
      (doseq [[s c-expr] (partition 2 timers)]
        ((command (c/init c-expr (ZonedDateTime/now)) s))))))
