(ns adworj.reporting-test
  (:require [adworj.reporting :as r]
            [clj-time.format :as tf]
            [clojure.test :refer :all])
  (:import [com.google.api.ads.adwords.lib.jaxb.v201409 ReportDefinitionDateRangeType]
           [com.google.api.ads.adwords.lib.jaxb.v201409 ReportDefinitionReportType]))

(deftest report-definition-test
  (let [d (r/report-definition r/search-query "pauls test")]
    (is (= ReportDefinitionDateRangeType/YESTERDAY (.getDateRangeType d))))
  (let [start-day (tf/parse "2015-01-01")
        end-day   (tf/parse "2015-01-02")
        d (r/report-definition r/search-query "custom range test"
                               :range (r/date-range start-day end-day))]
    (is (= ReportDefinitionDateRangeType/CUSTOM_DATE (.getDateRangeType d)))
    (is (= "20150101" (.. d getSelector getDateRange getMin)))
    (is (= "20150102" (.. d getSelector getDateRange getMax))))
  (is (= ReportDefinitionReportType/SEARCH_QUERY_PERFORMANCE_REPORT
         (.getReportType (r/report-definition r/search-query "testing")))))
