(ns adworj.reporting-example
  (:require [adworj.credentials :as ac]
            [adworj.reporting :as ar]
            [clojure.java.io :as io]))

(defn -main [ads-properties client-customer-id]
  (let [credentials        (ac/offline-credentials ads-properties)
        session            (ar/reporting-session ads-properties credentials
                                                 :client-customer-id client-customer-id)]
    (with-open [report (ar/run session
                         ar/keywords-performance
                         "sample report" :range (ar/date-range :last-week)
                         :selected-fields [:ad-group-id
                                           :ad-group-name
                                           :ad-group-status
                                           :approval-status
                                           :average-cpc
                                           :average-cpm
                                           :average-position
                                           :campaign-id
                                           :campaign-name
                                           :campaign-status
                                           :click-type
                                           :clicks
                                           :conversions-many-per-click
                                           :cost
                                           :cost-per-conversion-many-per-click
                                           :cpc-bid
                                           :cpm-bid
                                           :ctr
                                           :date
                                           :device
                                           :first-page-cpc
                                           :keyword-id
                                           :impressions
                                           :keyword-text
                                           :quality-score
                                           :top-of-page-cpc])]
      (doseq [record (take 5 (ar/record-seq report))]
        (println (pr-str record))))))
