package net.ravendb.abstractions.data;

public class RavenConnectionStringOptions {
  private String defaultDatabase;
  private String url;
  private String apiKey;
  private String currentOAuthToken;
  private FailoverServers failoverServers;


  public FailoverServers getFailoverServers() {
    return failoverServers;
  }


  public void setFailoverServers(FailoverServers failoverServers) {
    this.failoverServers = failoverServers;
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  public String getApiKey() {
    return apiKey;
  }

  public String getCurrentOAuthToken() {
    return currentOAuthToken;
  }

  public String getDefaultDatabase() {
    return defaultDatabase;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  public void setCurrentOAuthToken(String currentOAuthToken) {
    this.currentOAuthToken = currentOAuthToken;
  }

  public void setDefaultDatabase(String defaultDatabase) {
    this.defaultDatabase = defaultDatabase;
  }


  @Override
  public String toString() {
    String user = "<none>";
    return String.format("Url: %s, User: %s, DefaultDatabase: %s, Api Key: %s",
        url, user, defaultDatabase, apiKey);
  }
}
