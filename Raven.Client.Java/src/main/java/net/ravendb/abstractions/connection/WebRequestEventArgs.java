package net.ravendb.abstractions.connection;


import net.ravendb.abstractions.basic.EventArgs;

import org.apache.http.client.methods.HttpUriRequest;


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
