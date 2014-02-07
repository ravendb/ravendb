package net.ravendb.abstractions.replication;

import net.ravendb.abstractions.basic.UseSharpEnum;

import org.apache.commons.lang.builder.HashCodeBuilder;

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

  @UseSharpEnum
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

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(apiKey).append(clientVisibleUrl)
      .append(database).append(disabled).append(domain)
      .append(ignoredClient).append(ignoredClient).append(password).append(transitiveReplicationBehavior).append(username).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ReplicationDestination other = (ReplicationDestination) obj;
    if (apiKey == null) {
      if (other.apiKey != null) return false;
    } else if (!apiKey.equals(other.apiKey)) return false;
    if (clientVisibleUrl == null) {
      if (other.clientVisibleUrl != null) return false;
    } else if (!clientVisibleUrl.equals(other.clientVisibleUrl)) return false;
    if (database == null) {
      if (other.database != null) return false;
    } else if (!database.equals(other.database)) return false;
    if (disabled == null) {
      if (other.disabled != null) return false;
    } else if (!disabled.equals(other.disabled)) return false;
    if (domain == null) {
      if (other.domain != null) return false;
    } else if (!domain.equals(other.domain)) return false;
    if (ignoredClient == null) {
      if (other.ignoredClient != null) return false;
    } else if (!ignoredClient.equals(other.ignoredClient)) return false;
    if (password == null) {
      if (other.password != null) return false;
    } else if (!password.equals(other.password)) return false;
    if (transitiveReplicationBehavior != other.transitiveReplicationBehavior) return false;
    if (username == null) {
      if (other.username != null) return false;
    } else if (!username.equals(other.username)) return false;
    return true;
  }



}
