(ns adworj.reporting-test
  (:require [adworj.reporting :as r]
            [clj-time.format :as tf]
            [clojure.test :refer :all])
  (:import [com.google.api.ads.adwords.lib.jaxb.v201710 ReportDefinitionDateRangeType]
           [com.google.api.ads.adwords.lib.jaxb.v201710 ReportDefinitionReportType]))

(deftest report-specification-test
  (let [d (r/report-definition r/search-query-performance
                               "pauls test"
                               (r/date-range :yesterday)
                               (r/all-fields r/search-query-performance))]
    (is (= ReportDefinitionDateRangeType/YESTERDAY (.getDateRangeType d))))
  (let [start-day (tf/parse "2015-01-01")
        end-day   (tf/parse "2015-01-02")
        d (r/report-definition r/search-query-performance
                               "custom range test"
                               (r/date-range start-day end-day)
                               (r/all-fields r/search-query-performance))]
    (is (= ReportDefinitionDateRangeType/CUSTOM_DATE (.getDateRangeType d)))
    (is (= "20150101" (.. d getSelector getDateRange getMin)))
    (is (= "20150102" (.. d getSelector getDateRange getMax)))
    (is (= 42 (count (.. d getSelector getFields)))))
  (is (= ReportDefinitionReportType/SEARCH_QUERY_PERFORMANCE_REPORT
         (.getReportType (r/report-definition r/search-query-performance
                               "testing"
                               (r/date-range :last-week)
                               (r/all-fields r/search-query-performance))))))

(deftest report-field-mapping-test
  (is (= "AdGroupName"  (get-in r/search-query-performance [:field-mappings :ad-group-name]) )))

(deftest report-field-names-test
  (is (= 42 (count (r/all-fields r/search-query-performance))))
  (is (= :account-currency-code (first (r/all-fields r/search-query-performance)))))

(deftest coercions-test
  (is (= {:bar "baz" :foo 1} (r/coerce-record {:foo (constantly 1)} {:foo "123" :bar "baz"}))))
