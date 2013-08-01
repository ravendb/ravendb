package raven.client.document;

import raven.client.connection.ICredentials;

public class OpenSessionOptions {
  private String database;
  private ICredentials credentials;
  private boolean forceReadFromMaster;


  public String getDatabase() {
    return database;
  }
  public void setDatabase(String database) {
    this.database = database;
  }
  public ICredentials getCredentials() {
    return credentials;
  }
  public void setCredentials(ICredentials credentials) {
    this.credentials = credentials;
  }
  public boolean isForceReadFromMaster() {
    return forceReadFromMaster;
  }
  public void setForceReadFromMaster(boolean forceReadFromMaster) {
    this.forceReadFromMaster = forceReadFromMaster;
  }


}
