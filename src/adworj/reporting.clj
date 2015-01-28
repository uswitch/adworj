(ns adworj.reporting
  (:require [clj-time.format :as tf]
            [clj-time.core :as tc]
            [adworj.credentials :as ac]
            [clojure.data.csv :as csv]
            [clojure.set :as set]
            [clojure.java.io :as io])
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

(defn all-fields [report]
  (keys (:field-mappings report)))

(defn selected-field-names
  [report & fields]
  {:pre [(set/subset? (set fields) (set (all-fields report)))]}
  (let [mappings (:field-mappings report)]
    (map (partial get mappings) fields)))

(defn- selector []
  (Selector. ))

(defn zero-impressionable? [^ReportDefinitionReportType report-type]
  (condp = report-type
    ReportDefinitionReportType/SEARCH_QUERY_PERFORMANCE_REPORT false
    ReportDefinitionReportType/PAID_ORGANIC_QUERY_REPORT false
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
      (.setIncludeZeroImpressions (zero-impressionable? (:type report)))
      (.setSelector sel))))

(defn report-specification [report name & {:keys [range selected-fields]
                                           :or   {range           (date-range :yesterday)
                                                  selected-fields (all-fields report)}}]
  {:definition (report-definition report name range selected-fields)
   :selected-fields selected-fields})

(defn report [type & field-mappings]
  (Report. type (apply array-map field-mappings)))

(defmacro defreport [name type & field-mappings]
  `(def ~name (report ~type ~@field-mappings)))

(defreport paid-and-organic-query ReportDefinitionReportType/PAID_ORGANIC_QUERY_REPORT
  :account-currency-code                 "AccountCurrencyCode"
  :account-descriptive-name              "AccountDescriptiveName"
  :account-time-zone-id                  "AccountTimeZoneId"
  :ad-group-id                           "AdGroupId"
  :ad-group-name                         "AdGroupName"
  :average-cpc                           "AverageCpc"
  :average-position                      "AveragePosition"
  :campaign-id                           "CampaignId"
  :campaign-name                         "CampaignName"
  :clicks                                "Clicks"
  :combined-ads-organic-clicks           "CombinedAdsOrganicClicks"
  :combined-ads-organic-clicks-per-query "CombinedAdsOrganicClicksPerQuery"
  :combined-ads-organic-queries          "CombinedAdsOrganicQueries"
  :ctr                                   "Ctr"
  :customer-descriptive-name             "CustomerDescriptiveName"
  :date                                  "Date"
  :external-customer-id                  "ExternalCustomerId"
  :impressions                           "Impressions"
  :keyword-id                            "KeywordId"
  :keyword-text-matching-query           "KeywordTextMatchingQuery"
  :match-type                            "MatchType"
  :organic-average-position              "OrganicAveragePosition"
  :organic-clicks                        "OrganicClicks"
  :organic-clicks-per-query              "OrganicClicksPerQuery"
  :organic-impressions                   "OrganicImpressions"
  :organic-impressions-per-query         "OrganicImpressionsPerQuery"
  :organic-queries                       "OrganicQueries"
  :primary-company-name                  "PrimaryCompanyName"
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
  :average-cpc                         "AverageCpc"
  :average-cpm                         "AverageCpm"
  :average-position                    "AveragePosition"
  :campaign-id                         "CampaignId"
  :campaign-name                       "CampaignName"
  :campaign-status                     "CampaignStatus"
  :clicks                              "Clicks"
  :conversion-category-name            "ConversionCategoryName"
  :click-conversion-rate               "ConversionRate"
  :conversion-type-name                "ConversionTypeName"
  :conversion-value                    "ConversionValue"
  :conversions                         "Conversions"
  :conversions-many-per-click          "ConversionsManyPerClick"
  :client-name                         "CustomerDescriptiveName"
  :conversion-rate                     "ConversionRateManyPerClick"
  :cost                                "Cost"
  :cost-per-conversion                 "CostPerConversion"
  :cost-per-conversion-many-per-click  "CostPerConversionManyPerClick"
  :creative-id                         "CreativeId"
  :ctr                                 "Ctr"
  :date                                "Date"
  :day-of-week                         "DayOfWeek"
  :destination-url                     "DestinationUrl"
  :device                              "Device"
  :external-customer-id                "ExternalCustomerId"
  :impressions                         "Impressions"
  :keyword-id                          "KeywordId"
  :keyword-text-matching-query         "KeywordTextMatchingQuery"
  :match-type                          "MatchType"
  :match-type-with-variant             "MatchTypeWithVariant"
  :month                               "Month"
  :month-of-year                       "MonthOfYear"
  :primary-company-name                "PrimaryCompanyName"
  :quarter                             "Quarter"
  :query                               "Query"
  :value-per-conversion                "ValuePerConversion"
  :value-per-conversion-many-per-click "ValuePerConversionManyPerClick"
  :view-through-conversions            "ViewThroughConversions"
  :week                                "Week"
  :year                                "Year")

(defreport geo-performance ReportDefinitionReportType/GEO_PERFORMANCE_REPORT
  :account-currency-code               "Currency"
  :account-descriptive-name            "AccountDescriptiveName"
  :account-time-zone-id                "AccountTimeZoneId"
  :ad-format                           "AdFormat"
  :ad-group-id                         "AdGroupId"
  :ad-group-name                       "AdGroupName"
  :ad-group-status                     "AdGroupStatus"
  :ad-network-type-1                   "AdNetworkType1"
  :ad-network-type-2                   "AdNetworkType2"
  :average-cpc                         "AverageCpc"
  :average-cpm                         "AverageCpm"
  :average-position                    "AveragePosition"
  :campaign-id                         "CampaignId"
  :campaign-name                       "CampaignName"
  :campaign-status                     "CampaignStatus"
  :city-criteria-id                    "CityCriteriaId"
  :clicks                              "Clicks"
  :conversion-category-name            "ConversionCategoryName"
  :conversion-rate                     "ConversionRate"
  :conversion-rate-many-per-click      "ConversionRateManyPerClick"
  :conversion-tracker-id               "ConversionTrackerId"
  :conversion-type-name                "ConversionTypeName"
  :conversion-value                    "ConversionValue"
  :conversions                         "Conversions"
  :conversions-many-per-click          "ConversionsManyPerClick"
  :cost                                "Cost"
  :cost-per-conversion                 "CostPerConversion"
  :cost-per-conversion-many-per-click  "CostPerConversionManyPerClick"
  :country-criteria-id                 "CountryCriteriaId"
  :ctr                                 "Ctr"
  :customer-descriptive-name           "CustomerDescriptiveName"
  :date                                "Date"
  :day-of-week                         "DayOfWeek"
  :device                              "Device"
  :external-customer-id                "ExternalCustomerId"
  :impressions                         "Impressions"
  :is-targeting-location               "IsTargetingLocation"
  :location-type                       "LocationType"
  :metro-criteria-id                   "MetroCriteriaId"
  :month                               "Month"
  :month-of-year                       "MonthOfYear"
  :most-specific-criteria-id           "MostSpecificCriteriaId"
  :primary-company-name                "PrimaryCompanyName"
  :quarter                             "Quarter"
  :region-criteria-id                  "RegionCriteriaId"
  :value-per-conversion                "ValuePerConversion"
  :value-per-conversion-many-per-click "ValuePerConversionManyPerClick"
  :view-through-conversions            "ViewThroughConversions"
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
  :advertiser-experiment-segmentation-bin          "AdvertiserExperimentSegmentationBin"
  :approval-status                                 "ApprovalStatus"
  :average-cpc                                     "AverageCpc"
  :average-cpm                                     "AverageCpm"
  :average-position                                "AveragePosition"
  :bid-modifier                                    "BidModifier"
  :bid-type                                        "BidType"
  :campaign-id                                     "CampaignId"
  :campaign-name                                   "CampaignName"
  :campaign-status                                 "CampaignStatus"
  :click-significance                              "ClickSignificance"
  :click-type                                      "ClickType"
  :clicks                                          "Clicks"
  :conversion-category-name                        "ConversionCategoryName"
  :conversion-many-per-click-significance          "ConversionManyPerClickSignificance"
  :conversion-rate                                 "ConversionRate"
  :conversion-rate-many-per-click                  "ConversionRateManyPerClick"
  :conversion-rate-many-per-click-significance     "ConversionRateManyPerClickSignificance"
  :conversion-rate-significance                    "ConversionRateSignificance"
  :conversion-significance                         "ConversionSignificance"
  :conversion-type-name                            "ConversionTypeName"
  :conversion-value                                "ConversionValue"
  :conversions                                     "Conversions"
  :conversions-many-per-click                      "ConversionsManyPerClick"
  :cost                                            "Cost"
  :cost-per-conversion                             "CostPerConversion"
  :cost-per-conversion-many-per-click              "CostPerConversionManyPerClick"
  :cost-per-conversion-many-per-click-significance "CostPerConversionManyPerClickSignificance"
  :cost-per-conversion-significance                "CostPerConversionSignificance"
  :cost-significance                               "CostSignificance"
  :cpc-bid                                         "CpcBid"
  :cpc-bid-source                                  "CpcBidSource"
  :cpc-significance                                "CpcSignificance"
  :cpm-bid                                         "CpmBid"
  :cpm-significance                                "CpmSignificance"
  :criteria                                        "Criteria"
  :criteria-destination-url                        "CriteriaDestinationUrl"
  :criteria-type                                   "CriteriaType"
  :ctr                                             "Ctr"
  :ctr-significance                                "CtrSignificance"
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
  :first-page-cpc                                  "FirstPageCpc"
  :id                                              "Id"
  :impression-significance                         "ImpressionSignificance"
  :impressions                                     "Impressions"
  :is-negative                                     "IsNegative"
  :label-ids                                       "LabelIds"
  :labels                                          "Labels"
  :month                                           "Month"
  :month-of-year                                   "MonthOfYear"
  :parameter                                       "Parameter"
  :position-significance                           "PositionSignificance"
  :primary-company-name                            "CompanyName"
  :quality-score                                   "QualityScore"
  :quarter                                         "Quarter"
  :slot                                            "Slot"
  :status                                          "Status"
  :top-of-page-cpc                                 "TopOfPageCpc"
  :tracking-url-template                           "TrackingUrlTemplate"
  :url-custom-parameters                           "UrlCustomParameters"
  :value-per-conversion                            "ValuePerConversion"
  :value-per-conversion-many-per-click             "ValuePerConversionManyPerClick"
  :view-through-conversions                        "ViewThroughConversions"
  :view-through-conversions-significance           "ViewThroughConversionsSignificance"
  :week                                            "Week"
  :year                                            "Year")

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
  :conversion-category-name            "ConversionCategoryName"
  :conversion-rate                     "ConversionRate"
  :conversion-rate-many-per-click      "ConversionRateManyPerClick"
  :conversion-type-name                "ConversionTypeName"
  :conversion-value                    "ConversionValue"
  :conversions                         "Conversions"
  :conversions-many-per-click          "ConversionsManyPerClick"
  :cost                                "Cost"
  :cost-per-conversion                 "CostPerConversion"
  :cost-per-conversion-many-per-click  "CostPerConversionManyPerClick"
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
  :value-per-conversion                "ValuePerConversion"
  :value-per-conversion-many-per-click "ValuePerConversionManyPerClick"
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
  :average-cpc                         "AverageCpc"
  :average-cpm                         "AverageCpm"
  :average-position                    "AveragePosition"
  :bid-modifier                        "BidModifier"
  :bid-type                            "BidType"
  :campaign-id                         "CampaignId"
  :campaign-name                       "CampaignName"
  :campaign-status                     "CampaignStatus"
  :click-type                          "ClickType"
  :clicks                              "Clicks"
  :conversion-category-name            "ConversionCategoryName"
  :conversion-rate                     "ConversionRate"
  :conversion-rate-many-per-click      "ConversionRateManyPerClick"
  :conversion-type-name                "ConversionTypeName"
  :conversion-value                    "ConversionValue"
  :conversions                         "Conversions"
  :conversions-many-per-click          "ConversionsManyPerClick"
  :cost                                "Cost"
  :cost-per-conversion                 "CostPerConversion"
  :cost-per-conversion-many-per-click  "CostPerConversionManyPerClick"
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
  :slot                                "Slot"
  :status                              "Status"
  :value-per-conversion                "ValuePerConversion"
  :value-per-conversion-many-per-click "ValuePerConversionManyPerClick"
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
  :conversion-category-name            "ConversionCategoryName"
  :conversion-rate                     "ConversionRate"
  :conversion-rate-many-per-click      "ConversionRateManyPerClick"
  :conversion-type-name                "ConversionTypeName"
  :conversion-value                    "ConversionValue"
  :conversions                         "Conversions"
  :conversions-many-per-click          "ConversionsManyPerClick"
  :cost                                "Cost"
  :cost-per-conversion                 "CostPerConversion"
  :cost-per-conversion-many-per-click  "CostPerConversionManyPerClick"
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
  :keyword-text                        "KeywordText"
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
  :value-per-conversion                "ValuePerConversion"
  :value-per-conversion-many-per-click "ValuePerConversionManyPerClick"
  :week                                "Week"
  :year                                "Year")

(defreport ad-extensions-performance ReportDefinitionReportType/AD_EXTENSIONS_PERFORMANCE_REPORT
  :account-currency-code                "AccountCurrencyCode"
  :account-descriptive-name             "AccountDescriptiveName"
  :account-time-zone-id                 "AccountTimeZoneId"
  :ad-extension-id                      "AdExtensionId"
  :ad-extension-type                    "AdExtensionType"
  :ad-network-type-1                    "AdNetworkType1"
  :ad-network-type-2                    "AdNetworkType2"
  :approval-status                      "ApprovalStatus"
  :average-cpc                          "AverageCpc"
  :average-cpm                          "AverageCpm"
  :average-position                     "AveragePosition"
  :average-cost-for-offline-interaction "AverageCostForOfflineInteraction"
  :campaign-id                          "CampaignId"
  :click-type                           "ClickType"
  :clicks                               "Clicks"
  :conversion-rate                      "ConversionRate"
  :conversion-rate-many-per-click       "ConversionRateManyPerClick"
  :conversion-value                     "ConversionValue"
  :conversions                          "Conversions"
  :conversions-many-per-click           "ConversionsManyPerClick"
  :cost                                 "Cost"
  :cost-per-conversion                  "CostPerConversion"
  :cost-per-conversion-many-per-click   "CostPerConversionManyPerClick"
  :ctr                                  "Ctr"
  :customer-descriptive-name            "CustomerDescriptiveName"
  :date                                 "Date"
  :day-of-week                          "DayOfWeek"
  :device                               "Device"
  :external-customer-id                 "ExternalCustomerId"
  :impressions                          "Impressions"
  :location-extension-source            "LocationExtensionSource"
  :month                                "Month"
  :month-of-year                        "MonthOfYear"
  :num-offline-impressions              "NumOfflineImpressions"
  :num-offline-interactions             "NumOfflineInteractions"
  :offline-interaction-cost             "OfflineInteractionCost"
  :offline-interaction-rate             "OfflineInteractionRate"
  :primary-company-name                 "PrimaryCompanyName"
  :quarter                              "Quarter"
  :slot                                 "Slot"
  :status                               "Status"
  :value-per-conversion                 "ValuePerConversion"
  :view-through-conversions             "ViewThroughConversions"
  :week                                 "Week"
  :year                                 "Year")

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
  :keyword-text                         "KeywordText"
  :placement-url                        "PlacementUrl"
  :primary-company-name                 "PrimaryCompanyName"
  :user-list-id                         "UserListId")

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
  :conversion-category-name             "ConversionCategoryName"
  :conversion-rate                      "ConversionRate"
  :conversion-rate-many-per-click       "ConversionRateManyPerClick"
  :conversion-type-name                 "ConversionTypeName"
  :conversion-value                     "ConversionValue"
  :conversions                          "Conversions"
  :conversions-many-per-click           "ConversionsManyPerClick"
  :cost                                 "Cost"
  :cost-per-conversion                  "CostPerConversion"
  :cost-per-conversion-many-per-click   "CostPerConversionManyPerClick"
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
  :value-per-conversion                 "ValuePerConversion"
  :value-per-conversion-many-per-click  "ValuePerConversionManyPerClick"
  :week                                 "Week"
  :year                                 "Year")


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

(defn records
  "reads records from the input stream. returns a lazy sequence of
  records. converts records into clojure maps with their attributes
  as keywords.
  e.g. (with-open [rdr (reader (report-stream ...))]
         (doseq [rec (records reader selected-fields)]
           ...
    produces => [{:ad-group-name ..."
  [reader selected-fields]
  (let [records (csv/read-csv reader)]
    (map (fn [cells]
           (apply array-map (interleave selected-fields cells)))
         (rest records))))

(defn save-to-file
  "runs report and writes (uncompressed) csv data directly to out-file.
  e.g.
  (def session (reporting-session ...
  (save-to-file session
                (report-specification search-query \"searches\" :range (date-range :last-week))
                \"out.csv\")"
  [adwords-session report-specification out-file]
  (let [is (report-stream adwords-session (:definition report-specification))]
    (with-open [reader (io/reader is)]
      (with-open [writer (io/writer out-file)]
        (io/copy reader writer)))))

(defprotocol RecordReader (readRecords [this]))

(defn make-report [report name range selected-fields]
  (let [report-def (report-definition report name range selected-fields)]
    (fn [session]
      (let [r (io/reader (report-stream session report-def))]
        (reify
          RecordReader
          (readRecords [this]
            (records r selected-fields))
          java.io.Closeable
          (close [this]
            (.close r)))))))

(defmacro defreporter [reporter report]
  `(defn ~reporter [name# & {:keys [range# selected-fields#]
                   :or   {range#           (date-range :yesterday)
                          selected-fields# (all-fields ~report)}}]
     (make-report ~report name# range# selected-fields#)))

(defreporter paid-and-organic-query-report paid-and-organic-query)
