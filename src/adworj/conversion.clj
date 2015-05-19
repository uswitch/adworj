(ns adworj.conversion
  (:require [clj-time.core :as tc]
            [clj-time.format :as tf])
  (:import [com.google.api.ads.adwords.axis.v201409.cm OfflineConversionFeed OfflineConversionFeedOperation OfflineConversionFeedServiceInterface ConversionTrackerCategory UploadConversion ConversionTrackerOperation ConversionTrackerServiceInterface Operator]
           [com.google.api.ads.adwords.axis.factory AdWordsServices]))


(def conversion-category {:default ConversionTrackerCategory/DEFAULT
                          :page-view ConversionTrackerCategory/PAGE_VIEW
                          :purchase ConversionTrackerCategory/PURCHASE
                          :signup ConversionTrackerCategory/SIGNUP
                          :lead ConversionTrackerCategory/LEAD
                          :remarketing ConversionTrackerCategory/REMARKETING
                          :download ConversionTrackerCategory/DOWNLOAD})

(defn upload-conversion
  "creates a conversion suitable for being reported offline."
  [name & {:keys [category viewthrough-lookback-days ctc-lookback-days currency-code]
           :or   {category                   :default
                  viewthrough-lookback-days  30
                  ctc-lookback-days          30
                  currency-code              "GBP"}}]
  (let [cat (conversion-category category)]
    (doto (UploadConversion. )
      (.setName name)
      (.setCategory cat)
      (.setViewthroughLookbackWindow (int viewthrough-lookback-days))
      (.setCtcLookbackWindow (int ctc-lookback-days))
      (.setDefaultRevenueCurrencyCode currency-code))))

(defn- tracker-service [session]
  (let [adwords (AdWordsServices. )]
    (.get adwords session ConversionTrackerServiceInterface)))

(defn- add-conversion-op [conversion]
  (doto (ConversionTrackerOperation. )
    (.setOperator Operator/ADD)
    (.setOperand conversion)))

(defn create-conversions
  "adds the conversion to your account. it will raise an ApiException if multiple conversions
   with the same name are attempted to be created, "
  [session upload-conversions]
  (let [service (tracker-service session)
        ops (map add-conversion-op upload-conversions)]
    (.mutate service (into-array ConversionTrackerOperation ops))))


;; once you've defined a conversion type you can report conversions through
;; the offline conversion feed service

(def conversion-time-format (tf/formatter "yyyyMMdd HHmmss Z"))

(defn conversion-feed
  "represents an individual conversion event; associated to an ad click through
   the gclid (google click id) parameter"
  [name gclid & {:keys [time value currency-code]
                 :or   {time          (tc/now)
                        value         0.0
                        currency-code "GBP"}}]
  (doto (OfflineConversionFeed. )
    (.setConversionName name)
    (.setConversionTime (tf/unparse conversion-time-format time))
    (.setConversionValue value)
    (.setConversionCurrencyCode currency-code)
    (.setGoogleClickId gclid)))

(defn- conversion-feed-service [session]
  (let [adwords (AdWordsServices. )]
    (.get adwords session OfflineConversionFeedServiceInterface)))

(defn- add-feed-op [conversion-feed]
  (doto (OfflineConversionFeedOperation. )
    (.setOperator Operator/ADD)
    (.setOperand conversion-feed)))

(defn upload-conversions
  [session conversion-feeds]
  (let [service (conversion-feed-service session)
        ops (map add-feed-op conversion-feeds)]
    (.mutate service (into-array OfflineConversionFeedOperation ops))))
