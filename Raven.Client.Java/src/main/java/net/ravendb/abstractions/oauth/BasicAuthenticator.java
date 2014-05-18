package net.ravendb.abstractions.oauth;

import net.ravendb.abstractions.closure.Action1;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;


public class BasicAuthenticator extends AbstractAuthenticator {
  private final boolean enableBasicAuthenticationOverUnsecuredHttp;
  private final CloseableHttpClient httpClient;

  public BasicAuthenticator(CloseableHttpClient httpClient, boolean enableBasicAuthenticationOverUnsecuredHttp) {
    this.enableBasicAuthenticationOverUnsecuredHttp = enableBasicAuthenticationOverUnsecuredHttp;
    this.httpClient = httpClient;
  }

  @Override
  public Action1<HttpRequest> doOAuthRequest(String oauthSource, String apiKey) {
    try {
      HttpGet authRequest = prepareOAuthRequest(oauthSource, apiKey);
      try (CloseableHttpResponse httpReponse = httpClient.execute(authRequest)) {
        final String response = IOUtils.toString(httpReponse.getEntity().getContent());
        return new Action1<HttpRequest>() {

          @Override
          public void apply(HttpRequest request) {
            request.setHeader("Authorization", "Bearer " + response);
          }
        };
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private HttpGet prepareOAuthRequest(String oauthSource, String apiKey) {
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
