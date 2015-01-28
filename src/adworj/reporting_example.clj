(ns adworj.reporting-example
  (:require [adworj.credentials :as ac]
	    [adworj.reporting :as ar]
	     [clojure.java.io :as io]))

(defn -main [ads-properties client-customer-id]
  (let [credentials        (ac/offline-credentials ads-properties)
	session            (ar/reporting-session ads-properties credentials
						 :client-customer-id client-customer-id)]

    (let [report-spec (ar/report-specification ar/paid-and-organic-query "sample report"
					       :range (ar/date-range :last-week))]
      (with-open [rdr (io/reader (ar/report-stream session report-spec))]
	(doseq [record (take 5 (ar/records rdr report-spec))]
	  (println "Record: " record))))

  ;; (println ads-properties)
  ;; (println client-customer-id))
))
