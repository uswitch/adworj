(ns adworj.credentials
  (:import [com.google.api.ads.common.lib.auth OfflineCredentials$Builder OfflineCredentials$Api]
           [com.google.api.ads.common.lib.auth GoogleClientSecretsBuilder GoogleClientSecretsBuilder$Api]
           [com.google.api.client.googleapis.auth.oauth2 GoogleCredential$Builder]
           [org.apache.commons.configuration Configuration BaseConfiguration]
           [com.google.api.client.http.javanet NetHttpTransport]
           [com.google.api.client.json.jackson2 JacksonFactory]
           [com.google.api.ads.adwords.lib.client AdWordsSession$Builder]
           [com.google.api.client.googleapis.auth.oauth2 GoogleAuthorizationCodeFlow$Builder]))

(defn client-secrets [config-file]
  (-> (GoogleClientSecretsBuilder. )
      (.forApi GoogleClientSecretsBuilder$Api/ADWORDS)
      (.fromFile config-file)
      (.build)))

;;; It's necessary to authenticate with OAuth 2 to have a token
;;; that can be refreshed. This is a "Client ID for native application".
;;; https://code.google.com/p/google-api-ads-java/source/browse/examples/dfp_axis/src/main/java/dfp/axis/auth/GetRefreshToken.java

(defn auth-flow [client-secrets]
  (let [transport (NetHttpTransport.)
        jackson   (JacksonFactory. )]
    (-> (GoogleAuthorizationCodeFlow$Builder. transport
                                              jackson
                                              client-secrets
                                              ["https://www.googleapis.com/auth/adwords"])
        (.setAccessType "offline")
        (.build))))

(defn authorization-url
  "provides the URL that must be opened and authenticated by a user to access a auth code"
  [auth-flow]
  (-> auth-flow (.newAuthorizationUrl) (.setRedirectUri "urn:ietf:wg:oauth:2.0:oob") (.build)))


(defn oauth-credentials
  "returns the refresh token, requires authorization code to be entered having successfully
  authenticated above."
  [auth-flow client-secrets authorization-code]
  (let [req (.newTokenRequest auth-flow authorization-code)]
    (.setRedirectUri req "urn:ietf:wg:oauth:2.0:oob")
    (let [resp (.execute req)
          cred (-> (GoogleCredential$Builder. )
                   (.setTransport (NetHttpTransport.))
                   (.setJsonFactory (JacksonFactory. ))
                   (.setClientSecrets client-secrets)
                   (.build))]
      (.setFromTokenResponse cred resp)
      cred)))

(defn refresh-token [oauth-credentials]
  (.getRefreshToken oauth-credentials))

(defn offline-credentials
  "requires authentication configuration with client id, secret
  and a refresh token."
  [config-file refresh-token]
  (-> (OfflineCredentials$Builder. )
      (.forApi OfflineCredentials$Api/ADWORDS)
      (.fromFile config-file)
      (.withRefreshToken refresh-token)
      (.build)
      (.generateCredential)))

(defn adwords-session [oauth-credentials]
  (-> (AdWordsSession$Builder. )
      (.withOAuth2Credential oauth-credentials)
      (.build)))


(comment
  (def secrets (client-secrets "./ads.properties"))
  (def flow    (auth-flow secrets))

  (println "please authenticate at: " (authorization-url flow))

  (def auth-code "copy code here!")

  (def oauth-creds (oauth-credentials flow secrets auth-code))

  (def offline-creds (offline-credentials "./ads.properties" (refresh-token oauth-creds)))
)
