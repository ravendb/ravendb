package net.ravendb.abstractions.oauth;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.connection.WebRequestEventArgs;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;


public abstract class AbstractAuthenticator {

  protected String currentOauthToken;

  public void configureRequest(Object sender, WebRequestEventArgs e) {
    if (StringUtils.isEmpty(currentOauthToken)) {
      return;
    }
    setHeader(e.getRequest(), "Authorization", currentOauthToken);
  }

  protected void setAuthorization(HttpClient httpClient) {
    if (StringUtils.isEmpty(currentOauthToken)) {
      return;
    }
  }

  protected static void setHeader(HttpUriRequest request, String key, String value) {
    request.setHeader(key, value);
  }

  public abstract Action1<HttpRequest> doOAuthRequest(String oauthSource, String apiKey);

}
