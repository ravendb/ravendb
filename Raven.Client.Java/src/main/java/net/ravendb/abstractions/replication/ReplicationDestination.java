package net.ravendb.abstractions.replication;

public class ReplicationDestination {

  private String url;
  private String username;
  private String password;
  private String domain;
  private String apiKey;
  private String database;
  private TransitiveReplicationOptions transitiveReplicationBehavior;
  private Boolean ignoredClient;
  private Boolean disabled;
  private String clientVisibleUrl;

  public String getApiKey() {
    return apiKey;
  }

  public String getClientVisibleUrl() {
    return clientVisibleUrl;
  }

  public String getDatabase() {
    return database;
  }

  public Boolean getDisabled() {
    return disabled;
  }

  public String getDomain() {
    return domain;
  }

  public Boolean getIgnoredClient() {
    return ignoredClient;
  }

  public String getPassword() {
    return password;
  }

  public TransitiveReplicationOptions getTransitiveReplicationBehavior() {
    return transitiveReplicationBehavior;
  }

  public String getUrl() {
    return url;
  }

  public String getUsername() {
    return username;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  public void setClientVisibleUrl(String clientVisibleUrl) {
    this.clientVisibleUrl = clientVisibleUrl;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setDisabled(Boolean disabled) {
    this.disabled = disabled;
  }

  public void setDomain(String domain) {
    this.domain = domain;
  }

  public void setIgnoredClient(Boolean ignoredClient) {
    this.ignoredClient = ignoredClient;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setTransitiveReplicationBehavior(TransitiveReplicationOptions transitiveReplicationBehavior) {
    this.transitiveReplicationBehavior = transitiveReplicationBehavior;
  }

  public void setUrl(String url) {
    this.url = url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public enum TransitiveReplicationOptions {

    // Don't replicate replicated documents
    NONE("Changed only"),

    // Replicate replicated documents
    REPLICATE("Changed and replicated");

    private String description;

    private TransitiveReplicationOptions(String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

  }

}
