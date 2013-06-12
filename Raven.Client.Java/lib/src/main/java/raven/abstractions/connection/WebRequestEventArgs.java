package raven.abstractions.connection;

import org.apache.commons.httpclient.HttpMethodBase;

public class WebRequestEventArgs {
  private HttpMethodBase request;

  public WebRequestEventArgs(HttpMethodBase request) {
    super();
    this.request = request;
  }

  /**
   * @return the request
   */
  public HttpMethodBase getRequest() {
    return request;
  }


}
