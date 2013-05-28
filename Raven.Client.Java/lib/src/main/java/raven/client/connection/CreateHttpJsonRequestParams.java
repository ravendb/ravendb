package raven.client.connection;

import java.io.Serializable;

public class CreateHttpJsonRequestParams implements Serializable {
  private HttpMethods method;
  private String url;
  private ServerClient serverClient;

  /**
   * @return the serverClient
   */
  public ServerClient getServerClient() {
    return serverClient;
  }

  /**
   * @return the url
   */
  public String getUrl() {
    return url;
  }

  /**
   * @return the method
   */
  public HttpMethods getMethod() {
    return method;
  }


  public CreateHttpJsonRequestParams(ServerClient serverClient, String url, HttpMethods method) {
    super();
    this.method = method;
    this.url = url;
    this.serverClient = serverClient;
  }

}
