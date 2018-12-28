(ns adworj.customer
  (:import [com.google.api.ads.adwords.axis.v201806.mcm CustomerServiceInterface]
           [com.google.api.ads.adwords.axis.factory AdWordsServices]))

(defrecord Customer [currency-code customer-id date-time-zone descriptive-name tracking-url-template test-account?])

(defn customer-service [adwords-session]
  (let [services (AdWordsServices. )]
    (.get services adwords-session CustomerServiceInterface)))

(defn customer [customer-service]
  (let [customers (.getCustomers customer-service)]
    (->> customers
      (map (fn [c]
             (map->Customer {:currency-code         (.getCurrencyCode c)
                             :customer-id           (.getCustomerId c)
                             :date-time-zone        (.getDateTimeZone c)
                             :descriptive-name      (.getDescriptiveName c)
                             :tracking-url-template (.getTrackingUrlTemplate c)}))))))
