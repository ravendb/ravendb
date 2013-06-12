package raven.abstractions.connection;

import org.apache.commons.httpclient.HttpMethodBase;

import raven.abstractions.basic.EventArgs;

public class WebRequestEventArgs extends EventArgs {
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
