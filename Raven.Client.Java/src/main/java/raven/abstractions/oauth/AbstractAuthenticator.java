package raven.abstractions.oauth;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpRequest;
import org.apache.http.client.methods.HttpUriRequest;

import raven.abstractions.closure.Action1;
import raven.abstractions.connection.WebRequestEventArgs;

public abstract class AbstractAuthenticator {

  protected String currentOauthToken;

  public void configureRequest(Object sender, WebRequestEventArgs e) {
    if (StringUtils.isEmpty(currentOauthToken)) {
      return;
    }
    setHeader(e.getRequest(), "Authorization", currentOauthToken);
  }

  protected static void setHeader(HttpUriRequest request, String key, String value) {
    request.setHeader(key, value);
  }

  public abstract Action1<HttpRequest> doOAuthRequest(String oauthSource);

}
