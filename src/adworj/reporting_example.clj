(ns adworj.reporting-example
  (:require [adworj.credentials :as ac]
            [adworj.reporting :as ar]
            [clojure.java.io :as io]))

(defn -main [ads-properties client-customer-id]
  (let [credentials        (ac/offline-credentials ads-properties)
        session            (ar/reporting-session ads-properties credentials
                                                 :client-customer-id client-customer-id)]
    (with-open [report (ar/run ar/paid-and-organic-query session
                         "sample report" :range (ar/date-range :last-week)
                         :selected-fields [:date :organic-clicks-per-query :organic-queries :organic-clicks :combined-ads-organic-clicks-per-query :combined-ads-organic-clicks :combined-ads-organic-queries])]
      (doseq [record (take 100 (ar/record-seq report))]
        (println (pr-str record))))))
