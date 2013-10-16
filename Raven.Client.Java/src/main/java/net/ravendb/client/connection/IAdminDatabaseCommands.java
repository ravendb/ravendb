package net.ravendb.client.connection;

import net.ravendb.abstractions.data.DatabaseDocument;

public interface IAdminDatabaseCommands {
  /**
   *  Disables all indexing
   */
  public void stopIndexing();

  /**
   * Enables indexing
   */
  public void startIndexing();

  /**
   * Begins a backup operation
   * @param backupLocation
   * @param databaseDocument
   */
  public void startBackup(String backupLocation, DatabaseDocument databaseDocument);

  /**
   * Begins a restore operation
   * @param restoreLocation
   * @param databaseLocation
   */
  public void startRestore(String restoreLocation, String databaseLocation);

  /**
   * Begins a restore operation
   * @param restoreLocation
   * @param databaseLocation
   */
  public void startRestore(String restoreLocation, String databaseLocation, String databaseName);

  /**
   * Begins a restore operation
   * @param restoreLocation
   * @param databaseLocation
   */
  public void startRestore(String restoreLocation, String databaseLocation, String databaseName, boolean defrag);

  /**
   *  Get the indexing status
   * @return
   */
  public String getIndexingStatus();

}
