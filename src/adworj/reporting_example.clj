(ns adworj.reporting-example
  (:require [adworj.credentials :as ac]
            [adworj.reporting :as ar]
            [clojure.java.io :as io]))

(defn -main [ads-properties client-customer-id]
  (let [credentials        (ac/offline-credentials ads-properties)
        session            (ar/reporting-session ads-properties credentials
                                                 :client-customer-id client-customer-id)]
    (with-open [report (ar/run session
                         ar/search-query-performance
                         "sample report" :range (ar/date-range :last-week)
                         :selected-fields [:average-cpc :average-cpm :average-position :campaign-id :campaign-name :clicks :date :conversion-value :conversions :conversions-many-per-click :conversion-rate :cost :cost-per-conversion :cost-per-conversion-many-per-click :ctr :impressions :value-per-conversion :value-per-conversion-many-per-click :view-through-conversions])]
      (doseq [record (take 5 (ar/record-seq report))]
        (println (pr-str record))))))
