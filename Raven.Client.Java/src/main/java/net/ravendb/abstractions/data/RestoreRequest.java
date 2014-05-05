package net.ravendb.abstractions.data;


public class RestoreRequest {

  private String backupLocation;

  private String databaseLocation;

  private String databaseName;

  private String journalsLocation;

  private String indexesLocation;

  private boolean defrag;

  public String getBackupLocation() {
    return backupLocation;
  }

  public void setBackupLocation(String backupLocation) {
    this.backupLocation = backupLocation;
  }

  public String getDatabaseLocation() {
    return databaseLocation;
  }

  public void setDatabaseLocation(String databaseLocation) {
    this.databaseLocation = databaseLocation;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getJournalsLocation() {
    return journalsLocation;
  }

  public void setJournalsLocation(String journalsLocation) {
    this.journalsLocation = journalsLocation;
  }

  public String getIndexesLocation() {
    return indexesLocation;
  }

  public void setIndexesLocation(String indexesLocation) {
    this.indexesLocation = indexesLocation;
  }

  public boolean isDefrag() {
    return defrag;
  }

  public void setDefrag(boolean defrag) {
    this.defrag = defrag;
  }

}
