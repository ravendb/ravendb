package raven.client.delegates;

import raven.client.connection.IDatabaseCommands;


public interface IdConvention {
  public String findIdentifier(String dbName, IDatabaseCommands databaseCommands, Object entity);
}
