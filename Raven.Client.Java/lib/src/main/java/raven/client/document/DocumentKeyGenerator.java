package raven.client.document;

import raven.client.connection.IDatabaseCommands;

public interface DocumentKeyGenerator {
  /**
   * Generates document key
   * @param dbName
   * @param dbCommands
   * @param entity
   * @return
   */
  public String generate(String dbName, IDatabaseCommands dbCommands, Object entity);
}
