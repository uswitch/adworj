(ns adworj.reporting
  (:require [clj-time.format :as tf]
            [clj-time.core :as tc])
  (:import [com.google.api.ads.adwords.lib.jaxb.v201409 ReportDefinition ReportDefinitionReportType]
           [com.google.api.ads.adwords.lib.jaxb.v201409 DownloadFormat]
           [com.google.api.ads.adwords.lib.jaxb.v201409 DateRange Selector ReportDefinitionDateRangeType]))

(def adwords-date-format (tf/formatter "yyyyMMdd"))


(defrecord Report [type field-mappings])

(defn date-range
  "specify the date range for the report to cover. can be a set of predefined options,
  or a custom date range. start and end dates must be formattable by clj-time (e.g. (tc/now))"
  ([type]
   {:type (condp = type
            :today               ReportDefinitionDateRangeType/TODAY
            :yesterday           ReportDefinitionDateRangeType/YESTERDAY
            :last-7-days         ReportDefinitionDateRangeType/LAST_7_DAYS
            :last-week           ReportDefinitionDateRangeType/LAST_WEEK
            :last-business-week  ReportDefinitionDateRangeType/LAST_BUSINESS_WEEK
            :this-month          ReportDefinitionDateRangeType/THIS_MONTH
            :last-month          ReportDefinitionDateRangeType/LAST_MONTH
            :all-time            ReportDefinitionDateRangeType/ALL_TIME
            :last-14-days        ReportDefinitionDateRangeType/LAST_14_DAYS
            :last-30-days        ReportDefinitionDateRangeType/LAST_30_DAYS
            :this-week-sun-today ReportDefinitionDateRangeType/THIS_WEEK_SUN_TODAY
            :this-week-mon-today ReportDefinitionDateRangeType/THIS_WEEK_MON_TODAY
            :last-week-sun-sat   ReportDefinitionDateRangeType/LAST_WEEK_SUN_SAT)})
  ([start end]
   {:type ReportDefinitionDateRangeType/CUSTOM_DATE
    :min  (tf/unparse adwords-date-format start)
    :max  (tf/unparse adwords-date-format end)}))

(defn- selector []
  (Selector. ))

(defn report-definition [{:keys [type] :as report} name & {:keys [range]
                                                           :or   {range (date-range :yesterday)}}]
  (let [definition (doto (ReportDefinition. )
                     (.setReportName name)
                     (.setReportType type)
                     (.setDownloadFormat DownloadFormat/GZIPPED_CSV))
        {:keys [type min max]} range
        sel        (selector)]
    (if (= type ReportDefinitionDateRangeType/CUSTOM_DATE)
      (let [dr (doto (DateRange. )
                 (.setMax max)
                 (.setMin min))]
        (.setDateRangeType definition type)
        (.setDateRange sel dr))
      (.setDateRangeType definition type))
    (.setSelector definition sel)
    definition))

(defn report [type & field-mappings]
  (Report. type (apply array-map field-mappings)))

(defmacro defreport [name type & field-mappings]
  `(def ~name (report ~type ~@field-mappings)))

(defreport search-query ReportDefinitionReportType/SEARCH_QUERY_PERFORMANCE_REPORT
  :ad-group-name         "AdGroupName"
  :ad-group-id           "AdGroupId"
  :average-position      "AveragePosition"
  :campaign-id           "CampaignId"
  :campaign-name         "CampaignName"
  :clicks                "Clicks"
  :click-conversion-rate "ConversionRate"
  :client-name           "CustomerDescriptiveName"
  :conversion-rate       "ConversionRateManyPerClick"
  :cost                  "Cost"
  :ctr                   "Ctr"
  :date                  "Date"
  :device                "Device"
  :impressions           "Impressions"
  :query                 "Query")
