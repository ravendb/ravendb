package raven.abstractions.oauth;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import raven.abstractions.closure.Action1;

public class BasicAuthenticator extends AbstractAuthenticator {
  private final String apiKey;
  private final boolean enableBasicAuthenticationOverUnsecuredHttp;

  public BasicAuthenticator(String apiKey, boolean enableBasicAuthenticationOverUnsecuredHttp) {
    this.apiKey = apiKey;
    this.enableBasicAuthenticationOverUnsecuredHttp = enableBasicAuthenticationOverUnsecuredHttp;
  }

  @Override
  public Action1<HttpRequest> doOAuthRequest(String oauthSource) {
    HttpClient httpClient = new DefaultHttpClient();
    try {

      HttpGet authRequest = prepareOAuthRequest(oauthSource);
      HttpResponse httpResponse = httpClient.execute(authRequest);
      try {
        final String response = IOUtils.toString(httpResponse.getEntity().getContent());
        return new Action1<HttpRequest>() {

          @Override
          public void apply(HttpRequest request) {
            request.setHeader("Authorization", "Bearer " + response);
          }
        };
      } finally {
        EntityUtils.consume(httpResponse.getEntity());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      httpClient.getConnectionManager().shutdown();
    }
  }

  private HttpGet prepareOAuthRequest(String oauthSource) {
    HttpGet get = new HttpGet();
    get.setHeader("grant_type", "client_credentials");
    get.setHeader("Accept", "application/json;charset=UTF-8");
    if (StringUtils.isNotEmpty(apiKey)) {
      get.setHeader("Api-Key", apiKey);
    }

    if (!oauthSource.startsWith("https") && !enableBasicAuthenticationOverUnsecuredHttp) {
      throw new IllegalStateException(BASICO_AUTH_OVER_HTTP_ERROR);
    }

    return get;
  }

  private final static String BASICO_AUTH_OVER_HTTP_ERROR = "Attempting to authenticate using basic security over HTTP would expose user credentials (including the password) in clear text to anyone sniffing the network." +
      "Your OAuth endpoint should be using HTTPS, not HTTP, as the transport mechanism." +
      "You can setup the OAuth endpoint in the RavenDB server settings ('Raven/OAuthTokenServer' configuration value), or setup your own behavior by providing a value for: " +
      "documentStore.Conventions.HandleUnauthorizedResponse " +
      "If you are on an internal network or requires this for testing, you can disable this warning by calling: " +
      "documentStore.getJsonRequestFactory().enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers = true; ";

}
