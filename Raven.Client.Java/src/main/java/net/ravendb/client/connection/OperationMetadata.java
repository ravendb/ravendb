package net.ravendb.client.connection;

import net.ravendb.abstractions.connection.OperationCredentials;


public class OperationMetadata {

  private String url;
  private OperationCredentials credentials;

  public String getUrl() {
    return url;
  }

  public OperationCredentials getCredentials() {
    return credentials;
  }

  public OperationMetadata(String url) {
    super();
    this.url = url;
  }

  public OperationMetadata(String url, OperationCredentials credentials) {
    this.url = url;
    this.credentials = new OperationCredentials(credentials.getApiKey());
  }

  public OperationMetadata(OperationMetadata opMeta) {
    this.url = opMeta.getUrl();
    this.credentials = new OperationCredentials(opMeta.getCredentials());
  }


}
