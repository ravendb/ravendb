package net.ravendb.abstractions.connection;


import net.ravendb.abstractions.basic.EventArgs;

import org.apache.http.client.methods.HttpUriRequest;


public class WebRequestEventArgs extends EventArgs {
  private HttpUriRequest request;
  private OperationCredentials credentials;

  public WebRequestEventArgs(HttpUriRequest request) {
    super();
    this.request = request;
  }
  

  public WebRequestEventArgs(HttpUriRequest request, OperationCredentials credentials) {
    super();
    this.request = request;
    this.credentials = credentials;
  }



  public OperationCredentials getCredentials() {
    return credentials;
  }

  public void setCredentials(OperationCredentials credentials) {
    this.credentials = credentials;
  }


  /**
   * @return the request
   */
  public HttpUriRequest getRequest() {
    return request;
  }

}
