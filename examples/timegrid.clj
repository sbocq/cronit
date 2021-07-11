(ns timegrid
  (:require [sbocq.cronit :as c]))

(defn show [{:keys [:context :c-expr :format :date :zone-id]}]
  (c/show c-expr {:context context, :format format, :date date, :zone-id zone-id}))
