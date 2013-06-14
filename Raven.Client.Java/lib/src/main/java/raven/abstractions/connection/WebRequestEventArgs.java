package raven.abstractions.connection;


import org.apache.http.client.methods.HttpRequestBase;

import raven.abstractions.basic.EventArgs;

public class WebRequestEventArgs extends EventArgs {
  private HttpRequestBase request;

  public WebRequestEventArgs(HttpRequestBase request) {
    super();
    this.request = request;
  }

  /**
   * @return the request
   */
  public HttpRequestBase getRequest() {
    return request;
  }

}
