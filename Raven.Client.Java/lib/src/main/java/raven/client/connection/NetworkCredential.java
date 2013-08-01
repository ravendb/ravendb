package raven.client.connection;

import java.net.URI;

public class NetworkCredential implements ICredentials {
  private String domain;
  private String userName;
  private String password;

  public NetworkCredential() {
    super();
  }

  public NetworkCredential(String userName, String password) {
    super();
    this.userName = userName;
    this.password = password;
  }

  public NetworkCredential(String domain, String userName, String password) {
    super();
    this.domain = domain;
    this.userName = userName;
    this.password = password;
  }

  public String getDomain() {
    return domain;
  }
  public void setDomain(String domain) {
    this.domain = domain;
  }
  public String getUserName() {
    return userName;
  }
  public void setUserName(String userName) {
    this.userName = userName;
  }
  public String getPassword() {
    return password;
  }
  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public NetworkCredential getCredential(URI uri, String authType) {
    return this;
  }


}
