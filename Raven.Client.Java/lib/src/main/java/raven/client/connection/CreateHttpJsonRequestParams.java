package raven.client.connection;

import java.io.Serializable;

public class CreateHttpJsonRequestParams implements Serializable {
  private String method;
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
  public String getMethod() {
    return method;
  }

  /**
   * @param method the method to set
   */
  public void setMethod(String method) {
    this.method = method;
  }


  public CreateHttpJsonRequestParams(ServerClient serverClient, String url, String method) {
    super();
    this.method = method;
    this.url = url;
    this.serverClient = serverClient;
  }



}
