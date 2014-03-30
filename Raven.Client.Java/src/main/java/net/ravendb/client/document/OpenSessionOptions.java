package net.ravendb.client.document;

public class OpenSessionOptions {
  private String database;
  private boolean forceReadFromMaster;


  public String getDatabase() {
    return database;
  }
  public void setDatabase(String database) {
    this.database = database;
  }
  public boolean isForceReadFromMaster() {
    return forceReadFromMaster;
  }
  public void setForceReadFromMaster(boolean forceReadFromMaster) {
    this.forceReadFromMaster = forceReadFromMaster;
  }


}
