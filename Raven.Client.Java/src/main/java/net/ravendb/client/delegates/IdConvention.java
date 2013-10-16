package net.ravendb.client.delegates;

import net.ravendb.client.connection.IDatabaseCommands;


public interface IdConvention {
  public String findIdentifier(String dbName, IDatabaseCommands databaseCommands, Object entity);
}
