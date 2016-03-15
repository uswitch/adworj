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
                         :selected-fields [:account-descriptive-name
                                           :ad-network-type-1
                                           :ad-network-type-2
                                           :date
                                           :criteria
                                           :quality-score
                                           :creative-quality-score
                                           :post-click-quality-score])]
      (doseq [record (take 5 (ar/record-seq report))]
        (println (pr-str record))))))
