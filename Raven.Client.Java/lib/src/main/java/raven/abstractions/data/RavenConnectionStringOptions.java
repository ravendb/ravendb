package raven.abstractions.data;

import java.util.UUID;

import raven.client.connection.Credentials;

public class RavenConnectionStringOptions {
  private Credentials credentials;
  private boolean enlistInDistributedTransactions;
  private String defaultDatabase;
  private UUID resourceManagerId;
  private String url;
  private String apiKey;
  private String currentOAuthToken;


  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  public String getApiKey() {
    return apiKey;
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public String getCurrentOAuthToken() {
    return currentOAuthToken;
  }

  public String getDefaultDatabase() {
    return defaultDatabase;
  }

  public UUID getResourceManagerId() {
    return resourceManagerId;
  }

  public boolean isEnlistInDistributedTransactions() {
    return enlistInDistributedTransactions;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  public void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }

  public void setCurrentOAuthToken(String currentOAuthToken) {
    this.currentOAuthToken = currentOAuthToken;
  }

  public void setDefaultDatabase(String defaultDatabase) {
    this.defaultDatabase = defaultDatabase;
  }

  public void setEnlistInDistributedTransactions(boolean enlistInDistributedTransactions) {
    this.enlistInDistributedTransactions = enlistInDistributedTransactions;
  }

  public void setResourceManagerId(UUID resourceManagerId) {
    this.resourceManagerId = resourceManagerId;
  }

  @Override
  public String toString() {
    String user = "<none>";
    //TODO: extract from credentials
    return String.format("Url: %s, User: %s, EnlistInDistributedTransactions: %s, DefaultDatabase: %s, ResourceManagerId: %s, Api Key: %s",
        url, user, enlistInDistributedTransactions, defaultDatabase, resourceManagerId, apiKey);
  }
}
