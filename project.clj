(defproject adworj "0.5.0-SNAPSHOT"
  :description "Clojure library to ease interacting with the Google AdWords API"
  :url "https://github.com/uswitch/adworj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.csv "0.1.4"]
                 [com.google.api-ads/ads-lib "3.12.0"]
                 [com.google.api-ads/adwords-axis "3.12.0"]
                 [clj-time "0.14.2"]
                 [joda-time "2.9.9"]])
