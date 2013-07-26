package raven.client.connection;

import raven.abstractions.data.DatabaseDocument;

public interface IAdminDatabaseCommands {
  /**
   * Creates new database
   * @param databaseDocument
   */
  public void createDatabase(DatabaseDocument databaseDocument);
}
