package raven.client.document;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import raven.abstractions.data.Constants;
import raven.client.connection.IDatabaseCommands;

public class MultiDatabaseHiLoGenerator {

  private final int capacity;
  private final ConcurrentMap<String, MultiTypeHiLoKeyGenerator> generators = new ConcurrentHashMap<String, MultiTypeHiLoKeyGenerator>();

  public MultiDatabaseHiLoGenerator(int capacity) {
    this.capacity = capacity;
  }


  public String generateDocumentKey(String dbName, IDatabaseCommands databaseCommands, DocumentConvention conventions, Object entity) {
    String key = dbName;
    if (key == null) {
      key = Constants.SYSTEM_DATABASE;
    }
    MultiTypeHiLoKeyGenerator generator = generators.putIfAbsent(dbName, new MultiTypeHiLoKeyGenerator(capacity));
    return generator.generateDocumentKey(databaseCommands, conventions, entity);
  }
}
