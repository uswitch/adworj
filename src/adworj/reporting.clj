(ns adworj.reporting
  (:require [clj-time.format :as tf]
            [clj-time.core :as tc]
            [adworj.credentials :as ac])
  (:import [com.google.api.ads.adwords.lib.jaxb.v201409 ReportDefinition ReportDefinitionReportType]
           [com.google.api.ads.adwords.lib.jaxb.v201409 DownloadFormat]
           [com.google.api.ads.adwords.lib.jaxb.v201409 DateRange Selector ReportDefinitionDateRangeType]
           [com.google.api.ads.adwords.lib.client AdWordsSession]
           [com.google.api.ads.adwords.lib.client.reporting ReportingConfiguration$Builder]
           [com.google.api.client.auth.oauth2 Credential]
           [com.google.api.ads.adwords.lib.utils.v201409 ReportDownloader DetailedReportDownloadResponseException]
           [java.util.zip GZIPInputStream]))

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

(defn selected-field-names [report & fields]
  (let [mappings (:field-mappings report)
        selected (or fields (keys mappings))]
    (map (partial get mappings) selected)))

(defn- selector []
  (Selector. ))

(defn zero-impressionable? [^ReportDefinitionReportType report-type]
  (cond (= ReportDefinitionReportType/SEARCH_QUERY_PERFORMANCE_REPORT report-type) false
        :default true))

(defn report-definition [report name & {:keys [range selected-fields]
                                        :or   {range (date-range :yesterday)}}]
  (let [definition             (doto (ReportDefinition. )
                                 (.setReportName name)
                                 (.setReportType (:type report))
                                 (.setDownloadFormat DownloadFormat/GZIPPED_CSV))
        {:keys [min max]} range
        sel                    (Selector. )]
    (if (= (:type range) ReportDefinitionDateRangeType/CUSTOM_DATE)
      (let [dr (doto (DateRange. )
                 (.setMax max)
                 (.setMin min))]
        (.setDateRangeType definition (:type range))
        (.setDateRange sel dr))
      (.setDateRangeType definition (:type range)))
    (.. sel getFields (addAll (apply selected-field-names report selected-fields)))
    (doto definition
      (.setIncludeZeroImpressions (zero-impressionable? (:type report)))
      (.setSelector sel))))

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





(defn configure-session-for-reporting
  "optimizes session configuration for reporting."
  [^AdWordsSession adwords-session]
  (.setReportingConfiguration adwords-session
                              (-> (ReportingConfiguration$Builder. )
                                  (.skipReportHeader true)
                                  (.skipReportSummary true)
                                  (.build))))

(defn reporting-session
  "creates an adwords session optimized for reporting. opts are those accepted
  by credentials/adwords-session."
  [config-file ^Credential credential & opts]
  (doto (apply ac/adwords-session config-file credential opts)
    (configure-session-for-reporting)))


(defn report-stream
  "provides uncompressed access to the stream of report data. returns an input stream
  that should be closed when finished.
  adwords-session: optimally a reporting-session to avoid header + summary"
  [adwords-session report-definition]
  (let [downloader (ReportDownloader. adwords-session)
        response   (.downloadReport downloader report-definition)]
    (GZIPInputStream. (.getInputStream response))))
