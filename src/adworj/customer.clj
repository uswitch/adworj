(ns adworj.customer
  (:import [com.google.api.ads.adwords.axis.v201409.mcm CustomerServiceInterface]
           [com.google.api.ads.adwords.axis.factory AdWordsServices]))

(defrecord Customer [company-name currency-code customer-id date-time-zone descriptive-name tracking-url-template test-account?])

(defn customer-service [adwords-session]
  (let [services (AdWordsServices. )]
    (.get services adwords-session CustomerServiceInterface)))

(defn customer [customer-service]
  (let [c (.get customer-service)]
    (map->Customer {:company-name          (.getCompanyName c)
                    :currency-code         (.getCurrencyCode c)
                    :customer-id           (.getCustomerId c)
                    :date-time-zone        (.getDateTimeZone c)
                    :descriptive-name      (.getDescriptiveName c)
                    :tracking-url-template (.getTrackingUrlTemplate c)})))
