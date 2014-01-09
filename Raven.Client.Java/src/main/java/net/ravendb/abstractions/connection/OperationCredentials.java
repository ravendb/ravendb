package net.ravendb.abstractions.connection;


public class OperationCredentials {
  private String apiKey;

  public String getApiKey() {
    return apiKey;
  }

  public OperationCredentials() {
    super();
  }

  public OperationCredentials(OperationCredentials operationCredentials) {
    this.apiKey = operationCredentials.apiKey;
  }


  public OperationCredentials(String apiKey) {
    super();
    this.apiKey = apiKey;
  }


  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

}
