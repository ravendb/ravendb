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
   *  Get the indexing status
   * @return
   */
  public String getIndexingStatus();

}
