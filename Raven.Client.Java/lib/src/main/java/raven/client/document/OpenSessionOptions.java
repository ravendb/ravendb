package raven.client.document;

import raven.client.connection.Credentials;

public class OpenSessionOptions {
  private String database;
  private Credentials credentials;
  private boolean forceReadFromMaster;


  public String getDatabase() {
    return database;
  }
  public void setDatabase(String database) {
    this.database = database;
  }
  public Credentials getCredentials() {
    return credentials;
  }
  public void setCredentials(Credentials credentials) {
    this.credentials = credentials;
  }
  public boolean isForceReadFromMaster() {
    return forceReadFromMaster;
  }
  public void setForceReadFromMaster(boolean forceReadFromMaster) {
    this.forceReadFromMaster = forceReadFromMaster;
  }


}
