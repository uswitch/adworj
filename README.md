# adworj

Clojure library to ease interacting with the Google AdWords API. It is built upon [Google's AdWords API Java Client Library](https://github.com/googleads/googleads-java-lib).

## OAuth 2 Authentication

Google's APIs are moving towards requiring OAuth 2 authentication. To do this you need to:

1. Visit [https://console.developers.google.com/](https://console.developers.google.com/) and (if necessary create a project).
2. Click on **Credentials** in the API &amp; auth menu on the left.
3. Ceate a new Client ID for a **native application**. The redirect URI should be: `urn:ietf:wg:oauth:2.0:oob`
4. You'll also need your AdWords API developer token.

Once you've generated all the tokens and keys you can use `adworj.credentials` namespace to help generate the necessary credentials.

Ultimately you need to retrieve the **refresh token** that is provided by the OAuth Credentials. Having retrieved this during the OAuth flow you can either create `offline-credentials` using the refresh token directly, or, store the refresh token in your `ads.properties`.

## ads.properties

The easiest way to configure Google's client library is via a properties file. This should look something like:

    api.adwords.clientId=(from the developer console above)
    api.adwords.clientSecret=(from the developer console above)
    api.adwords.developerToken=(from the adwords console)

Once you've authenticated you can add an entry for the refresh token:

    api.adwords.refreshToken=(from oauth authentication/authorization flow)

# Reporting API

Having successfully authenticated, and updated the properties file with your configuration, you can run a report as follows:

```clojure
(ns reporter
  (:require [adworj.credentials :as ac]
             [adworj.reporting :as ar]
             [clojure.java.io :as io]))

(def client-customer-id "123-456-7890")
(def credentials        (ac/offline-credentials "./ads.properties"))
(def session            (ar/reporting-session "./ads.properties" credentials
                                              :client-customer-id client-customer-id))

(let [report-def (ar/report-definition ar/paid-and-organic-query "sample report"
                                       :range (ar/date-range :last-week))]
  (with-open [rdr (io/reader (ar/report-stream session report-def))]
    (doseq [record (ar/records rdr)]
      (println "Record: " record))))
```

## License

Copyright &copy; 2015 uSwitch Limited.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
