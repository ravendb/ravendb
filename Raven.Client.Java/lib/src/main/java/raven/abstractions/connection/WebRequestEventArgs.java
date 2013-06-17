package raven.abstractions.connection;


import org.apache.http.client.methods.HttpUriRequest;

import raven.abstractions.basic.EventArgs;

public class WebRequestEventArgs extends EventArgs {
  private HttpUriRequest request;

  public WebRequestEventArgs(HttpUriRequest request) {
    super();
    this.request = request;
  }

  /**
   * @return the request
   */
  public HttpUriRequest getRequest() {
    return request;
  }

}
