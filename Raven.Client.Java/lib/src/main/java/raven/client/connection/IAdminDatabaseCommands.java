package raven.client.connection;

import raven.abstractions.data.DatabaseDocument;

public interface IAdminDatabaseCommands {
  /**
   * Creates new database
   * @param databaseDocument
   */
  public void createDatabase(DatabaseDocument databaseDocument);

  /**
   *  Ensures that the database exists, creating it if needed
   * @param name
   * @param ignoreFailures
   */
  public void ensureDatabaseExists(String name, boolean ignoreFailures);
}
