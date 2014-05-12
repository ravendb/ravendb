package net.ravendb.abstractions.data;


public class BackupRequest {
  private String backupLocation;

  private DatabaseDocument databaseDocument;

  public String getBackupLocation() {
    return backupLocation;
  }

  public void setBackupLocation(String backupLocation) {
    this.backupLocation = backupLocation;
  }

  public DatabaseDocument getDatabaseDocument() {
    return databaseDocument;
  }

  public void setDatabaseDocument(DatabaseDocument databaseDocument) {
    this.databaseDocument = databaseDocument;
  }

}
