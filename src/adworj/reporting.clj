(ns adworj.reporting
  (:require [clj-time.format :as tf]
            [clj-time.core :as tc]
            [adworj.credentials :as ac]
            [clojure.data.csv :as csv]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojure.string :as s])
  (:import [com.google.api.ads.adwords.lib.jaxb.v201609 ReportDefinition ReportDefinitionReportType]
           [com.google.api.ads.adwords.lib.jaxb.v201609 DownloadFormat]
           [com.google.api.ads.adwords.lib.jaxb.v201609 DateRange Selector ReportDefinitionDateRangeType]
           [com.google.api.ads.adwords.lib.client AdWordsSession]
           [com.google.api.ads.adwords.lib.client.reporting ReportingConfiguration$Builder]
           [com.google.api.client.auth.oauth2 Credential]
           [com.google.api.ads.adwords.lib.utils.v201609 ReportDownloader DetailedReportDownloadResponseException]
           [com.google.api.ads.adwords.axis.v201609.cm ReportDefinitionServiceInterface]
           [com.google.api.ads.adwords.axis.factory AdWordsServices]
           [java.util.zip GZIPInputStream]))

(def adwords-date-format (tf/formatter "yyyyMMdd"))

(defrecord ReportSpecification [type field-mappings])

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


(defn all-fields [report]
  (keys (:field-mappings report)))

(defn selected-field-names
  [report & fields]
  (let [selected  (set fields)
        available (set (all-fields report))]
    (when-not (set/subset? selected available)
      (throw (ex-info "selected fields unavailable in report" {:selected  selected
                                                               :available available
                                                               :diff      (set/difference selected available)}))))
  (let [mappings (:field-mappings report)
        field-name (fn [field] (let [m (get mappings field)]
                                (if (string? m) m (:name m))))]
    (map field-name fields)))

(defn- selector []
  (Selector. ))

(defn zero-impressionable? [^ReportDefinitionReportType report-type]
  (condp = report-type
    ReportDefinitionReportType/SEARCH_QUERY_PERFORMANCE_REPORT false
    ReportDefinitionReportType/PAID_ORGANIC_QUERY_REPORT       false
    ReportDefinitionReportType/GEO_PERFORMANCE_REPORT          false
    ReportDefinitionReportType/KEYWORDS_PERFORMANCE_REPORT     false
    ReportDefinitionReportType/AD_PERFORMANCE_REPORT           false
    ReportDefinitionReportType/ACCOUNT_PERFORMANCE_REPORT      false
    ReportDefinitionReportType/CLICK_PERFORMANCE_REPORT        false
    true))


(defn report-definition [report name range selected-fields]
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
      (.setSelector sel))))

(defn report-specification [type & field-mappings]
  (ReportSpecification. type (apply array-map field-mappings)))

(defmacro defreport [name type & field-mappings]
  `(def ~name (report-specification ~type ~@field-mappings)))

(defn remove-thousandths-separator [s]
  (s/replace s #"\," ""))

(defn parse-long [s] (Long/valueOf (remove-thousandths-separator s)))

(defn parse-int [s] (Integer/valueOf (remove-thousandths-separator s)))

(defn parse-double [s] (Double/valueOf (remove-thousandths-separator s)))

(defn parse-percentage [s]
  (/ (Double/valueOf (re-find #"^[\d.]+" s)) 100))

(defreport account-performance ReportDefinitionReportType/ACCOUNT_PERFORMANCE_REPORT
  :account-currency-code               "AccountCurrencyCode"
  :account-descriptive-name            "AccountDescriptiveName"
  :account-time-zone-id                "AccountTimeZoneId"
  :active-view-cpm                     {:name "ActiveViewCpm" :parse parse-long}
  :active-view-impressions             {:name "ActiveViewImpressions" :parse parse-long}
  :ad-network-type-1                   "AdNetworkType1"
  :ad-network-type-2                   "AdNetworkType2"
  :all-conversion-rate                 {:name "AllConversionRate" :parse parse-percentage}
  :all-conversion-value                {:name "AllConversionValue" :parse parse-double}
  :all-conversions                     {:name "AllConversions" :parse parse-long}
  :average-cpc                         {:name "AverageCpc" :parse parse-long}
  :average-cpm                         {:name "AverageCpm" :parse parse-long}
  :average-position                    {:name "AveragePosition" :parse parse-double}
  :manage-clients?                     "CanManageClients"
  :click-type                          "ClickType"
  :clicks                              {:name "Clicks" :parse parse-long}
  :conversions                         {:name "Conversions" :parse parse-double}
  :conversion-rate                     {:name "ConversionRate" :parse parse-percentage}
  :conversion-value                    {:name "ConversionValue" :parse parse-double}
  :conversions                         {:name "Conversions" :parse parse-double}
  :cost                                {:name "Cost" :parse parse-long}
  :cost-per-all-conversion             {:name "CostPerAllConversion" :parse parse-long}
  :cost-per-conversion                 {:name "CostPerConversion" :parse parse-long}
  :ctr                                 {:name "Ctr" :parse parse-percentage}
  :customer-descriptive-name           "CustomerDescriptiveName"
  :date                                "Date"
  :hour                                {:name "HourOfDay" :parse parse-int}
  :device                              "Device"
  :estimated-cross-device-conversions  {:name "EstimatedCrossDeviceConversions" :parse parse-long}
  :external-customer-id                "ExternalCustomerId"
  :impressions                         "Impressions"
  :invalid-click-rate                  {:name "InvalidClickRate" :parse parse-percentage}
  :invalid-clicks                      {:name "InvalidClicks" :parse parse-long}
  :auto-tagging?                       "IsAutoTaggingEnabled"
  :test-account?                       "IsTestAccount"
  :primary-company-name                "PrimaryCompanyName"
  :slot                                "Slot")

(defreport keywords-performance ReportDefinitionReportType/KEYWORDS_PERFORMANCE_REPORT
  :account-currency-code                  "AccountCurrencyCode"
  :account-descriptive-name               "AccountDescriptiveName"
  :account-time-zone-id                   "AccountTimeZoneId"
  :active-view-cpm                        {:name "ActiveViewCpm" :parse parse-long}
  :active-view-impressions                {:name "ActiveViewImpressions" :parse parse-long}
  :ad-group-id                            "AdGroupId"
  :ad-group-name                          "AdGroupName"
  :ad-group-status                        "AdGroupStatus"
  :ad-network-type-1                      "AdNetworkType1"
  :ad-network-type-2                      "AdNetworkType2"
  :approval-status                        "ApprovalStatus"
  :average-cpc                            {:name "AverageCpc" :parse parse-long}
  :average-cpm                            {:name "AverageCpm" :parse parse-long}
  :average-pageviews                      {:name "AveragePageviews" :parse parse-double}
  :average-position                       {:name "AveragePosition" :parse parse-double}
  :average-time-on-site                   {:name "AverageTimeOnSite" :parse parse-double}
  :bid-type                               "BidType"
  :bidding-strategy-id                    "BiddingStrategyId"
  :bidding-strategy-name                  "BiddingStrategyName"
  :bidding-strategy-type                  "BiddingStrategyType"
  :bounce-rate                            {:name "BounceRate" :parse parse-double}
  :campaign-id                            "CampaignId"
  :campaign-name                          "CampaignName"
  :campaign-status                        "CampaignStatus"
  :click-assisted-conversion-value        {:name "ClickAssistedConversionValue" :parse parse-double}
  :click-assisted-conversions             {:name "ClickAssistedConversions" :parse parse-long}
  :click-assisted-conversions-over-last-click-conversions
  {:name "ClickAssistedConversionsOverLastClickConversions"
   :parse parse-double}
  :conversion-rate                        {:name "ConversionRate" :parse parse-percentage}
  :click-type                             "ClickType"
  :clicks                                 {:name "Clicks" :parse parse-long}
  :conversion-value                       {:name "ConversionValue" :parse parse-double}
  :conversions                            {:name "Conversions" :parse parse-double}
  :cost                                   {:name "Cost" :parse parse-long}
  :cost-per-conversion                    {:name "CostPerConversion" :parse parse-long}
  :cpc-bid                                "CpcBid"
  :cpc-bid-source                         "CpcBidSource"
  :cpm-bid                                "CpmBid"
  :criteria-destination-url               "CriteriaDestinationUrl"
  :criteria                               "Criteria"
  :ctr                                    {:name "Ctr" :parse parse-percentage}
  :customer-descriptive-name              "CustomerDescriptiveName"
  :date                                   "Date"
  :day-of-week                            "DayOfWeek"
  :device                                 "Device"
  :enhanced-cpc-enabled                   "EnhancedCpcEnabled"
  :external-customer-id                   "ExternalCustomerId"
  :first-page-cpc                         {:name "FirstPageCpc" :parse parse-long}
  :keyword-id                             "Id"
  :has-quality-score                               "HasQualityScore"
  :impression-assisted-conversion-value   {:name "ImpressionAssistedConversionValue" :parse parse-double}
  :impression-assisted-conversions        {:name "ImpressionAssistedConversions" :parse parse-long}
  :impression-assisted-conversions-over-last-click-conversions "ImpressionAssistedConversionsOverLastClickConversions"
  :impressions                            {:name "Impressions" :parse parse-long}
  :is-negative                            "IsNegative"
  :keyword-match-type                     "KeywordMatchType"
  :keyword-text                           "Criteria"
  :label-ids                              "LabelIds"
  :labels                                 "Labels"
  :month                                  "Month"
  :month-of-year                          "MonthOfYear"
  :percent-new-visitors                   "PercentNewVisitors"
  :primary-company-name                   "PrimaryCompanyName"
  :quality-score                          {:name "QualityScore" :parse parse-long}
  :quarter                                "Quarter"
  :search-exact-match-impression-share    {:name "SearchExactMatchImpressionShare" :parse parse-percentage}
  :search-impression-share                {:name "SearchImpressionShare" :parse parse-percentage}
  :search-rank-lost-impression-share      {:name "SearchRankLostImpressionShare" :parse parse-percentage}
  :slot                                   "Slot"
  :status                                 "Status"
  :top-of-page-cpc                        "TopOfPageCpc"
  :tracking-url-template                  "TrackingUrlTemplate"
  :url-custom-parameters                  "UrlCustomParameters"
  :value-per-conversion    {:name "ValuePerConversion" :parse parse-double}
  :view-through-conversions               {:name "ViewThroughConversions" :parse parse-double}
  :week                                   "Week"
  :year                                   "Year"
  :search-predicted-ctr                   "SearchPredictedCtr"
  :creative-quality-score                 "CreativeQualityScore"
  :post-click-quality-score               "PostClickQualityScore")

(defreport paid-and-organic-query ReportDefinitionReportType/PAID_ORGANIC_QUERY_REPORT
  :account-currency-code                 "AccountCurrencyCode"
  :account-descriptive-name              "AccountDescriptiveName"
  :account-time-zone-id                  "AccountTimeZoneId"
  :ad-group-id                           "AdGroupId"
  :ad-group-name                         "AdGroupName"
  :average-cpc                           {:name "AverageCpc" :parse parse-long}
  :average-position                      {:name "AveragePosition" :parse parse-double}
  :campaign-id                           "CampaignId"
  :campaign-name                         "CampaignName"
  :clicks                                {:name "Clicks" :parse parse-long}
  :combined-ads-organic-clicks           {:name "CombinedAdsOrganicClicks" :parse parse-long}

  :combined-ads-organic-clicks-per-query {:name "CombinedAdsOrganicClicksPerQuery" :parse parse-percentage}
  :combined-ads-organic-queries          {:name "CombinedAdsOrganicQueries" :parse parse-long}
  :ctr                                   {:name "Ctr" :parse parse-percentage}
  :customer-descriptive-name             "CustomerDescriptiveName"
  :date                                  "Date"
  :external-customer-id                  "ExternalCustomerId"
  :impressions                           {:name "Impressions" :parse parse-long}
  :keyword-id                            "KeywordId"
  :keyword-text-matching-query           "KeywordTextMatchingQuery"
  :organic-average-position              {:name "OrganicAveragePosition" :parse parse-double}
  :organic-clicks                        {:name "OrganicClicks" :parse parse-long}
  :organic-clicks-per-query              {:name "OrganicClicksPerQuery" :parse parse-percentage}
  :organic-impressions                   {:name "OrganicImpressions" :parse parse-long}
  :organic-impressions-per-query         {:name "OrganicImpressionsPerQuery" :parse parse-double}
  :organic-queries                       {:name "OrganicQueries" :parse parse-long}
  :primary-company-name                  "PrimaryCompanyName"
  :query-match-type                      "QueryMatchType"
  :search-query                          "SearchQuery"
  :serp-type                             "SerpType")

(defreport search-query-performance ReportDefinitionReportType/SEARCH_QUERY_PERFORMANCE_REPORT
  :account-currency-code               "AccountCurrencyCode"
  :account-descriptive-name            "AccountDescriptiveName"
  :account-time-zone-id                "AccountTimeZoneId"
  :ad-format                           "AdFormat"
  :ad-group-name                       "AdGroupName"
  :ad-group-id                         "AdGroupId"
  :ad-group-status                     "AdGroupStatus"
  :ad-network-type-1                   "AdNetworkType1"
  :ad-network-type-2                   "AdNetworkType2"
  :average-cpc                         {:name "AverageCpc" :parse parse-long}
  :average-cpm                         {:name "AverageCpm" :parse parse-long}
  :average-position                    {:name "AveragePosition" :parse parse-double}
  :campaign-id                         "CampaignId"
  :campaign-name                       "CampaignName"
  :campaign-status                     "CampaignStatus"
  :clicks                              {:name "Clicks" :parse parse-long}
  :conversion-value                    {:name "ConversionValue" :parse parse-double}
  :conversions                         {:name "Conversions" :parse parse-double}
  :client-name                         "CustomerDescriptiveName"
  :conversion-rate                     {:name "ConversionRate" :parse parse-percentage}
  :cost                                {:name "Cost" :parse parse-long}
  :cost-per-conversion                 {:name "CostPerConversion" :parse parse-long}
  :creative-id                         "CreativeId"
  :ctr                                 {:name "Ctr" :parse parse-percentage}
  :date                                "Date"
  :day-of-week                         "DayOfWeek"
  :destination-url                     "DestinationUrl"
  :device                              "Device"
  :external-customer-id                "ExternalCustomerId"
  :impressions                         {:name "Impressions" :parse parse-long}
  :keyword-id                          "KeywordId"
  :keyword-text-matching-query         "KeywordTextMatchingQuery"
  :query-match-type-with-variant       "QueryMatchTypeWithVariant"
  :month                               "Month"
  :month-of-year                       "MonthOfYear"
  :primary-company-name                "PrimaryCompanyName"
  :quarter                             "Quarter"
  :query                               "Query"
  :value-per-conversion                {:name "ValuePerConversion" :parse parse-double}
  :view-through-conversions            {:name "ViewThroughConversions" :parse parse-long}
  :week                                "Week"
  :year                                "Year")

(defreport geo-performance ReportDefinitionReportType/GEO_PERFORMANCE_REPORT
  :account-currency-code               "AccountCurrencyCode"
  :account-descriptive-name            "AccountDescriptiveName"
  :account-time-zone-id                "AccountTimeZoneId"
  :ad-format                           "AdFormat"
  :ad-group-id                         "AdGroupId"
  :ad-group-name                       "AdGroupName"
  :ad-group-status                     "AdGroupStatus"
  :ad-network-type-1                   "AdNetworkType1"
  :ad-network-type-2                   "AdNetworkType2"
  :average-cpc                         {:name "AverageCpc" :parse parse-long}
  :average-cpm                         {:name "AverageCpm" :parse parse-long}
  :average-position                    {:name "AveragePosition" :parse parse-double}
  :campaign-id                         "CampaignId"
  :campaign-name                       "CampaignName"
  :campaign-status                     "CampaignStatus"
  :country-criteria-id                 "CountryCriteriaId"
  :city-criteria-id                    "CityCriteriaId"
  :clicks                              {:name "Clicks" :parse parse-long}
  :conversion-rate                     {:name "ConversionRate" :parse parse-percentage}
  :conversion-value                    {:name "ConversionValue" :parse parse-double}
  :conversions                         {:name "Conversions" :parse parse-double}
  :cost                                {:name "Cost" :parse parse-long}
  :cost-per-conversion                 {:name "CostPerConversion" :parse parse-long}
  :ctr                                 {:name "Ctr" :parse parse-percentage}
  :customer-descriptive-name           "CustomerDescriptiveName"
  :date                                "Date"
  :day-of-week                         "DayOfWeek"
  :device                              "Device"
  :external-customer-id                "ExternalCustomerId"
  :impressions                         {:name "Impressions" :parse parse-long}
  :is-targeting-location               "IsTargetingLocation"
  :location-type                       "LocationType"
  :metro-criteria-id                   "MetroCriteriaId"
  :month                               "Month"
  :month-of-year                       "MonthOfYear"
  :most-specific-criteria-id           "MostSpecificCriteriaId"
  :primary-company-name                "PrimaryCompanyName"
  :quarter                             "Quarter"
  :region-criteria-id                  "RegionCriteriaId"
  :value-per-conversion                {:name "ValuePerConversion" :parse parse-double}
  :view-through-conversions            {:name "ViewThroughConversions" :parse parse-long}
  :week                                "Week"
  :year                                "Year")

;;https://developers.google.com/adwords/api/docs/appendix/reports#criteria
(defreport criteria-performance ReportDefinitionReportType/CRITERIA_PERFORMANCE_REPORT
  :account-currency-code                           "AccountCurrencyCode"
  :account-descriptive-name                        "AccountDescriptiveName"
  :account-time-zone-id                            "AccountTimeZoneId"
  :ad-group-id                                     "AdGroupId"
  :ad-group-name                                   "AdGroupName"
  :ad-group-status                                 "AdGroupStatus"
  :ad-network-type-1                               "AdNetworkType1"
  :ad-network-type-2                               "AdNetworkType2"
  :approval-status                                 "ApprovalStatus"
  :average-cpc                                     {:name "AverageCpc" :parse parse-long}
  :average-cpm                                     {:name "AverageCpm" :parse parse-long}
  :average-position                                {:name "AveragePosition" :parse parse-double}
  :bid-modifier                                    "BidModifier"
  :bid-type                                        "BidType"
  :campaign-id                                     "CampaignId"
  :campaign-name                                   "CampaignName"
  :campaign-status                                 "CampaignStatus"
  :click-type                                      "ClickType"
  :clicks                                          {:name "Clicks" :parse parse-long}
  :conversion-rate                                 {:name "ConversionRate" :parse parse-percentage}
  :conversion-value                                "ConversionValue"
  :conversions                                     {:name "Conversions" :parse parse-double}
  :cost                                            {:name "Cost" :parse parse-long}
  :cost-per-conversion                             {:name "CostPerConversion" :parse parse-long}
  :cpc-bid                                         {:name "CpcBid" :parse parse-long}
  :cpc-bid-source                                  "CpcBidSource"
  :cpm-bid                                         "CpmBid"
  :criteria                                        "Criteria"
  :criteria-destination-url                        "CriteriaDestinationUrl"
  :criteria-type                                   "CriteriaType"
  :ctr                                             {:name "Ctr" :parse parse-percentage}
  :customer-descriptive-name                       "CustomerDescriptiveName"
  :date                                            "Date"
  :day-of-week                                     "DayOfWeek"
  :device                                          "Device"
  :display-name                                    "DisplayName"
  :enhanced-cpc-enabled                            "EnhancedCpcEnabled"
  :external-customer-id                            "ExternalCustomerId"
  :final-app-urls                                  "FinalAppUrls"
  :final-mobile-urls                               "FinalMobileUrls"
  :final-urls                                      "FinalUrls"
  :first-page-cpc                                  {:name "FirstPageCpc" :parse parse-long}
  :has-quality-score                               "HasQualityScore"
  :id                                              "Id"
  :impressions                                     {:name "Impressions" :parse parse-long}
  :is-negative                                     "IsNegative"
  :label-ids                                       "LabelIds"
  :labels                                          "Labels"
  :month                                           "Month"
  :month-of-year                                   "MonthOfYear"
  :parameter                                       "Parameter"
  :primary-company-name                            "CompanyName"
  :quality-score                                   {:name "QualityScore" :parse parse-long}
  :quarter                                         "Quarter"
  :slot                                            "Slot"
  :status                                          "Status"
  :top-of-page-cpc                                 {:name "TopOfPageCpc" :parse parse-long}
  :tracking-url-template                           "TrackingUrlTemplate"
  :url-custom-parameters                           "UrlCustomParameters"
  :value-per-conversion                            {:name "ValuePerConversion" :parse parse-double}
  :view-through-conversions                        {:name "ViewThroughConversions" :parse parse-long}
  :week                                            "Week"
  :year                                            "Year"
  :creative-quality-score                          "CreativeQualityScore")

(defreport age-range-performance ReportDefinitionReportType/AGE_RANGE_PERFORMANCE_REPORT
  :account-currency-code               "AccountCurrencyCode"
  :account-descriptive-name            "AccountDescriptiveName"
  :account-time-zone-id                "AccountTimeZoneId"
  :ad-group-id                         "AdGroupId"
  :ad-group-name                       "AdGroupName"
  :ad-group-status                     "AdGroupStatus"
  :ad-network-type-1                   "AdNetworkType1"
  :ad-network-type-2                   "AdNetworkType2"
  :average-cpc                         "AverageCpc"
  :average-cpm                         "AverageCpm"
  :bid-modifier                        "BidModifier"
  :bid-type                            "BidType"
  :campaign-id                         "CampaignId"
  :campaign-name                       "CampaignName"
  :campaign-status                     "CampaignStatus"
  :click-type                          "ClickType"
  :clicks                              "Clicks"
  :conversion-rate                     "ConversionRate"
  :conversion-value                    "ConversionValue"
  :conversions                         "Conversions"
  :cost                                {:name "Cost" :parse parse-long}
  :cost-per-conversion                 {:name "CostPerConversion" :parse parse-long}
  :cpc-bid                             "CpcBid"
  :cpc-bid-source                      "CpcBidSource"
  :cpm-bid                             "CpmBid"
  :cpm-bid-source                      "CpmBidSource"
  :criteria                            "Criteria"
  :criteria-destination-url            "CriteriaDestinationUrl"
  :ctr                                 "Ctr"
  :customer-descriptive-name           "CustomerDescriptiveName"
  :date                                "Date"
  :day-of-week                         "DayOfWeek"
  :destination-url                     "DestinationUrl"
  :device                              "Device"
  :external-customer-id                "ExternalCustomerId"
  :id                                  "Id"
  :impressions                         "Impressions"
  :is-negative                         "IsNegative"
  :is-restrict                         "IsRestrict"
  :month                               "Month"
  :month-of-year                       "MonthOfYear"
  :primary-company-name                "PrimaryCompanyName"
  :quarter                             "Quarter"
  :status                              "Status"
  :value-per-conversion                {:name "ValuePerConversion" :parse parse-double}
  :view-through-conversions            "ViewThroughConversions"
  :week                                "Week"
  :year                                "Year")

(defreport audience-performance ReportDefinitionReportType/AUDIENCE_PERFORMANCE_REPORT
  :account-currency-code               "AccountCurrencyCode"
  :account-descriptive-name            "AccountDescriptiveName"
  :account-time-zone-id                "AccountTimeZoneId"
  :ad-group-id                         "AdGroupId"
  :ad-group-name                       "AdGroupName"
  :ad-group-status                     "AdGroupStatus"
  :ad-network-type-1                   "AdNetworkType1"
  :ad-network-type-2                   "AdNetworkType2"
  :average-cpc                         {:name "AverageCpc" :parse parse-long}
  :average-cpm                         {:name "AverageCpm" :parse parse-long}
  :average-position                    {:name "AveragePosition" :parse parse-double}
  :bid-modifier                        "BidModifier"
  :bid-type                            "BidType"
  :campaign-id                         "CampaignId"
  :campaign-name                       "CampaignName"
  :campaign-status                     "CampaignStatus"
  :click-type                          "ClickType"
  :clicks                              {:name "Clicks" :parse parse-long}
  :conversion-rate                     {:name "ConversionRate" :parse parse-percentage}
  :conversion-value                    {:name "ConversionValue" :parse parse-double}
  :conversions                         {:name "Conversions" :parse parse-double}
  :cost                                {:name "Cost" :parse parse-long}
  :cost-per-conversion                 {:name "CostPerConversion" :parse parse-long}
  :cpc-bid                             "CpcBid"
  :cpc-bid-source                      "CpcBidSource"
  :cpm-bid                             "CpmBid"
  :cpm-bid-source                      "CpmBidSource"
  :criteria                            "Criteria"
  :criteria-destination-url            "CriteriaDestinationUrl"
  :ctr                                 {:name "Ctr" :parse parse-percentage}
  :customer-descriptive-name           "CustomerDescriptiveName"
  :date                                "Date"
  :day-of-week                         "DayOfWeek"
  :destination-url                     "DestinationUrl"
  :device                              "Device"
  :external-customer-id                "ExternalCustomerId"
  :id                                  "Id"
  :impressions                         {:name "Impressions" :parse parse-long}
  :is-negative                         "IsNegative"
  :is-restrict                         "IsRestrict"
  :month                               "Month"
  :month-of-year                       "MonthOfYear"
  :primary-company-name                "PrimaryCompanyName"
  :quarter                             "Quarter"
  :slot                                "Slot"
  :status                              "Status"
  :value-per-conversion                {:name "ValuePerConversion" :parse parse-double}
  :view-through-conversions            "ViewThroughConversions"
  :week                                "Week"
  :year                                "Year")

(defreport placeholder-feed-item ReportDefinitionReportType/PLACEHOLDER_FEED_ITEM_REPORT
  :account-currency-code               "AccountCurrencyCode"
  :account-descriptive-name            "AccountDescriptiveName"
  :account-time-zone-id                "AccountTimeZoneId"
  :ad-group-id                         "AdGroupId"
  :ad-group-name                       "AdGroupName"
  :ad-id                               "AdId"
  :ad-network-type-1                   "AdNetworkType1"
  :ad-network-type-2                   "AdNetworkType2"
  :attribute-values                    "AttributeValues"
  :average-cpc                         "AverageCpc"
  :average-cpm                         "AverageCpm"
  :average-position                    "AveragePosition"
  :campaign-id                         "CampaignId"
  :campaign-name                       "CampaignName"
  :click-type                          "ClickType"
  :clicks                              "Clicks"
  :conversion-rate                     "ConversionRate"
  :conversion-value                    "ConversionValue"
  :conversions                         "Conversions"
  :cost                                {:name "Cost" :parse parse-long}
  :cost-per-conversion                 {:name "CostPerConversion" :parse parse-long}
  :ctr                                 "Ctr"
  :customer-descriptive-name           "CustomerDescriptiveName"
  :date                                "Date"
  :day-of-week                         "DayOfWeek"
  :device                              "Device"
  :device-preference                   "DevicePreference"
  :end-time                            "EndTime"
  :external-customer-id                "ExternalCustomerId"
  :feed-id                             "FeedId"
  :feed-item-id                        "FeedItemId"
  :impressions                         "Impressions"
  :is-self-action                      "IsSelfAction"
  :keyword-match-type                  "KeywordMatchType"
  :keyword-text                        "Criteria"
  :month                               "Month"
  :month-of-year                       "MonthOfYear"
  :placeholder-type                    "PlaceholderType"
  :primary-company-name                "PrimaryCompanyName"
  :quarter                             "Quarter"
  :scheduling                          "Scheduling"
  :slot                                "Slot"
  :start-time                          "StartTime"
  :status                              "Status"
  :url-custom-parameters               "UrlCustomParameters"
  :validation-details                  "ValidationDetails"
  :value-per-conversion                {:name "ValuePerConversion" :parse parse-double}
  :week                                "Week"
  :year                                "Year")

(defreport campaign-negative-keywords-performance ReportDefinitionReportType/CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT
  :account-currency-code                "AccountCurrencyCode"
  :account-descriptive-name             "AccountDescriptiveName"
  :account-time-zone-id                 "AccountTimeZoneId"
  :campaign-id                          "CampaignId"
  :customer-descriptive-name            "CustomerDescriptiveName"
  :external-customer-id                 "ExternalCustomerId"
  :id                                   "Id"
  :is-negative                          "IsNegative"
  :keyword-match-type                   "KeywordMatchType"
  :primary-company-name                 "PrimaryCompanyName"
  :criteria                             "Criteria")

(defreport ad-customizers-feed-item ReportDefinitionReportType/AD_CUSTOMIZERS_FEED_ITEM_REPORT
  :ad-group-id                          "AdGroupId"
  :ad-group-name                        "AdGroupName"
  :ad-id                                "AdId"
  :ad-network-type-1                    "AdNetworkType1"
  :ad-network-type-2                    "AdNetworkType2"
  :average-cpc                          "AverageCpc"
  :average-cpm                          "AverageCpm"
  :average-position                     "AveragePosition"
  :campaign-id                          "CampaignId"
  :campaign-name                        "CampaignName"
  :clicks                               "Clicks"
  :conversion-rate                      "ConversionRate"
  :conversion-value                     "ConversionValue"
  :conversions                          "Conversions"
  :cost                                {:name "Cost" :parse parse-long}
  :cost-per-conversion                 {:name "CostPerConversion" :parse parse-long}
  :ctr                                  "Ctr"
  :date                                 "Date"
  :day-of-week                          "DayOfWeek"
  :device                               "Device"
  :feed-id                              "FeedId"
  :feed-item-attributes                 "FeedItemAttributes"
  :feed-item-end-time                   "FeedItemEndTime"
  :feed-item-id                         "FeedItemId"
  :feed-item-start-time                 "FeedItemStartTime"
  :feed-item-status                     "FeedItemStatus"
  :impressions                          "Impressions"
  :month                                "Month"
  :month-of-year                        "MonthOfYear"
  :quarter                              "Quarter"
  :slot                                 "Slot"
  :value-per-conversion                {:name "ValuePerConversion" :parse parse-double}
  :week                                 "Week"
  :year                                 "Year")

(defreport click-performance ReportDefinitionReportType/CLICK_PERFORMANCE_REPORT
  :account-descriptive-name "AccountDescriptiveName"
  :ad-group-id              "AdGroupId"
  :ad-group-name            "AdGroupName"
  :ad-group-status          "AdGroupStatus"
  :campaign-id              "CampaignId"
  :campaign-name            "CampaignNamecriterion"
  :campaign-status          "CampaignStatus"
  :creative-id              "CreativeId"
  :date                     "Date"
  :device                   "Device"
  :external-customer-id     "ExternalCustomerId"
  :gclid                    "GclId"
  :page                     "Page"
  :slot                     "Slot"
  :user-list-id             "UserListId")

(defreport ad-performance ReportDefinitionReportType/AD_PERFORMANCE_REPORT
  :account-currency-code                                        "AccountCurrencyCode"
  :account-descriptive-name                                     "AccountDescriptiveName"
  :account-time-zone-id                                         "AccountTimeZoneId"
  :ad-group-ad-disapproval-reasons                              "AdGroupAdDisapprovalReasons"
  :ad-group-id                                                  "AdGroupId"
  :ad-group-name                                                "AdGroupName"
  :ad-group-status                                              "AdGroupStatus"
  :ad-network-type-1                                            "AdNetworkType1"
  :ad-network-type-2                                            "AdNetworkType2"
  :ad-type                                                      "AdType"
  :average-cpc                                                  {:name "AverageCpc" :parse parse-long}
  :average-cpm                                                  {:name "AverageCpm" :parse parse-long}
  :average-pageviews                                            "AveragePageviews"
  :average-position                                             {:name "AveragePosition" :parse parse-double}
  :average-time-on-site                                         "AverageTimeOnSite"
  :bounce-rate                                                  "BounceRate"
  :campaign-id                                                  "CampaignId"
  :campaign-name                                                "CampaignName"
  :campaign-status                                              "CampaignStatus"
  :click-assisted-conversion-value                              "ClickAssistedConversionValue"
  :click-assisted-conversions                                   "ClickAssistedConversions"
  :click-assisted-conversions-over-last-click-conversions       "ClickAssistedConversionsOverLastClickConversions"
  :click-type                                                   "ClickType"
  :clicks                                                       {:name "Clicks" :parse parse-long}
  :conversion-rate                                              {:name "ConversionRate" :parse parse-percentage}
  :conversion-value                                             {:name "ConversionValue" :parse parse-double}
  :conversions                                                  {:name "Conversions" :parse parse-double}
  :cost                                                         {:name "Cost" :parse parse-long}
  :cost-per-conversion                                          {:name "CostPerConversion" :parse parse-long}
  :creative-approval-status                                     "CreativeApprovalStatus"
  :creative-destination-url                                     "CreativeDestinationUrl"
  :creative-final-app-urls                                      "CreativeFinalAppUrls"
  :creative-final-mobile-urls                                   "CreativeFinalMobileUrls"
  :creative-final-urls                                          "CreativeFinalUrls"
  :creative-tracking-url-template                               "CreativeTrackingUrlTemplate"
  :creative-url-custom-parameters                               "CreativeUrlCustomParameters"
  :criterion-id                                                 "CriterionId"
  :ctr                                                          {:name "Ctr" :parse parse-percentage}
  :customer-descriptive-name                                    "CustomerDescriptiveName"
  :date                                                         "Date"
  :day-of-week                                                  "DayOfWeek"
  :description-1                                                "Description1"
  :description-2                                                "Description2"
  :device                                                       "Device"
  :device-preference                                            "DevicePreference"
  :display-url                                                  "DisplayUrl"
  :external-customer-id                                         "ExternalCustomerId"
  :headline                                                     "Headline"
  :id                                                           "Id"
  :image-ad-url                                                 "ImageAdUrl"
  :image-creative-name                                          "ImageCreativeName"
  :impression-assisted-conversion-value                         {:name "ImpressionAssistedConversionValue" :parse parse-double}
  :impression-assisted-conversions                              {:name "ImpressionAssistedConversions" :parse parse-long}
  :impression-assisted-conversions-over-last-click-conversions  "ImpressionAssistedConversionsOverLastClickConversions"
  :impressions                                                  {:name "Impressions" :parse parse-long}
  :is-negative                                                  "IsNegative"
  :label-ids                                                    "LabelIds"
  :labels                                                       "Labels"
  :month                                                        "Month"
  :month-of-year                                                "MonthOfYear"
  :percent-new-visitors                                         "PercentNewVisitors"
  :primary-company-name                                         "PrimaryCompanyName"
  :quarter                                                      "Quarter"
  :shared-set-name                                              "SharedSetName"
  :slot                                                         "Slot"
  :status                                                       "Status"
  :trademarks                                                   "Trademarks"
  :value-per-conversion                                         {:name "ValuePerConversion" :parse parse-double}
  :view-through-conversions                                     "ViewThroughConversions"
  :week                                                         "Week"
  :year                                                         "Year")


(defn configure-session-for-reporting
  "optimizes session configuration for reporting."
  [^AdWordsSession adwords-session opts]
  (.setReportingConfiguration adwords-session
                              (-> (ReportingConfiguration$Builder. )
                                  (.skipReportHeader true)
                                  (.skipReportSummary true)
                                  (.includeZeroImpressions (:include-zero-impressions opts false))
                                  (.build))))

(defn reporting-session
  "creates an adwords session optimized for reporting. opts are those accepted
  by credentials/adwords-session."
  [config-file ^Credential credential & opts]
  (doto (apply ac/adwords-session config-file credential opts)
    (configure-session-for-reporting opts)))


(defn report-stream
  "provides uncompressed access to the stream of report data. returns an input stream
  that should be closed when finished.
  adwords-session: optimally a reporting-session to avoid header + summary"
  [adwords-session report-definition]
  (let [downloader (ReportDownloader. adwords-session)
        response   (.downloadReport downloader report-definition)]
    (GZIPInputStream. (.getInputStream response))))

(defn coercions [report]
  (into {} (for [[field x] (:field-mappings report)]
             (when-let [parse-fn (:parse x)]
               [field parse-fn]))))

(defn coerce-record [coercions m]
  (into m (for [[field coerce] coercions]
            (when-let [existing-value (field m)]
              (try [field (coerce existing-value)]
                   (catch NumberFormatException e
                     (throw (ex-info "error coercing record" {:field     field
                                                              :coerceion coerce
                                                              :original  m
                                                              :raw       existing-value}))))))))

(defn remove-empty-cell-dashes [m]
  (into {} (for [[k v] m] [k (when-not (re-find #"--$" v) v)])))

(defn records
  "reads records from the input stream. returns a lazy sequence of
  records. converts records into clojure maps with their attributes
  as keywords.
  e.g. (with-open [rdr (reader (report-stream ...))]
         (doseq [rec (records reader selected-fields)]
           ...
    produces => [{:ad-group-name ..."
  [reader selected-fields coercions]
  (let [records (csv/read-csv reader)]
    (->> (rest records) ;; drop header row
         (map (fn [cells]
                (apply array-map (interleave selected-fields cells))))
         (map remove-empty-cell-dashes)
         (map (partial coerce-record coercions)))))

(defn save-to-file
  "runs report and writes (uncompressed) csv data directly to out-file.
  e.g.
  (def session (reporting-session ...
  (save-to-file session
                (report-definition search-query \"searches\" :range (date-range :last-week))
                \"out.csv\")"
  [adwords-session report-definition out-file]
  (let [is (report-stream adwords-session report-definition)]
    (with-open [reader (io/reader is)]
      (with-open [writer (io/writer out-file)]
        (io/copy reader writer)))))

(defprotocol RecordReader (record-seq [this]))

(defn run [session report name & {:keys [range selected-fields]
                                  :or   {range           (date-range :yesterday)
                                         selected-fields (all-fields report)}}]
  (let [report-def (report-definition report name range selected-fields)
        r (io/reader (report-stream session report-def))]
    (reify
      RecordReader
      (record-seq [this]
        (records r selected-fields (coercions report)))
      java.io.Closeable
      (close [this]
        (.close r)))))
