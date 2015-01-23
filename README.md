# adworj

Clojure library to ease interacting with the Google AdWords API. It is built upon [Google's AdWords API Java Client Library](https://github.com/googleads/googleads-java-lib).

## OAuth 2 Authentication

Google's APIs are moving towards requiring OAuth 2 authentication. To do this you need to:

1. Visit [https://console.developers.google.com/](https://console.developers.google.com/) and (if necessary create a project).
2. Click on **Credentials** in the API &amp; auth menu on the left.
3. Ceate a new Client ID for a **native application**. The redirect URI should be: `urn:ietf:wg:oauth:2.0:oob`
4. You'll also need your AdWords API developer token.

Once you've generated all the tokens and keys you can use `adworj.credentials` namespace to help generate the necessary credentials.

## ads.properties

The easiest way to configure Google's client library is via a properties file. This should look something like:

    api.adwords.clientId=(from the developer console above)
    api.adwords.clientSecret=(from the developer console above)
    api.adwords.developerToken=(from the adwords console)

## License

Copyright &copy; 2015 uSwitch Limited.

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
