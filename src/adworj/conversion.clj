(ns adworj.conversion
  (:require [clj-time.core :as tc]
            [clj-time.format :as tf]
            [clojure.set :as s]
            [clojure.string :as st])
  (:import [com.google.api.ads.adwords.axis.v201506.cm ApiError RateExceededError ApiException OfflineConversionFeed OfflineConversionFeedReturnValue OfflineConversionFeedOperation OfflineConversionFeedServiceInterface ConversionTrackerCategory UploadConversion ConversionTrackerOperation ConversionTrackerServiceInterface Operator Selector]
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

(def provided? (complement st/blank?))

(defn conversion-feed
  "represents an individual conversion event; associated to an ad click through
   the gclid (google click id) parameter"
  [name gclid & {:keys [time value currency-code]
                 :or   {time          (tc/now)
                        value         0.0
                        currency-code "GBP"}}]
  {:pre [(provided? name) (provided? gclid)]}
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


(defprotocol ToClojure
  (to-clojure [_]))

(extend-protocol ToClojure
  RateExceededError
  (to-clojure [error] {:rate-name     (.getRateName error)
                       :rate-scope    (.getRateScope error)
                       :delay-seconds (.getRetryAfterSeconds error)
                       :trigger       (.getTrigger error)
                       :type          (.getApiErrorType error)
                       :field-path    (.getFieldPath error)
                       :error         (.getErrorString error)})
  ApiError
  (to-clojure [error] {:trigger    (.getTrigger error)
                       :type       (.getApiErrorType error)
                       :field-path (.getFieldPath error)
                       :error      (.getErrorString error)})
  OfflineConversionFeedReturnValue
  (to-clojure [return] (map to-clojure (.getValue return)))
  OfflineConversionFeed
  (to-clojure [feed] {:gclid    (.getGoogleClickId feed)
                      :name     (.getConversionName feed)
                      :time     (when-let [time (.getConversionTime feed)]
                                  (tf/parse conversion-time-format t))
                      :value    (.getConversionValue feed)
                      :currency (.getConversionCurrencyCode feed)}))

(defn- decorate-errors
  [conversion-feeds errors]
  (let [opidx #"operations\[(\d+)\].*"]
    (letfn [(assoc-conversion [{:keys [field-path] :as error}]
              (if-let [[_ index] (re-matches opidx field-path)]
                (let [indexint (Integer/valueOf index)]
                  (assoc error
                    :feed-index indexint
                    :conversion (nth conversion-feeds indexint)))
                error))]
      (map assoc-conversion errors))))

(defn upload-conversions
  [session conversion-feeds]
  (let [service (conversion-feed-service session)
        ops (map add-feed-op conversion-feeds)]
    (try {:conversions (to-clojure (.mutate service
                                            (into-array OfflineConversionFeedOperation ops)))
          :succeeded-indexes (range (count conversion-feeds))}
         (catch ApiException e
           (let [errs (->> (.getErrors e)
                           (map to-clojure)
                           (decorate-errors conversion-feeds))
                 failed (set (map :feed-index errs))
                 all    (set (range (count conversion-feeds)))]
             {:failed-indexes    failed
              :succeeded-indexes (s/difference all failed)
              :errors            errs})))))
