package raven.client.document;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import raven.abstractions.data.Constants;
import raven.client.connection.IDatabaseCommands;

public class MultiDatabaseHiLoGenerator {

  private final int capacity;
  private final ConcurrentMap<String, MultiTypeHiLoKeyGenerator> generators = new ConcurrentHashMap<>();

  public MultiDatabaseHiLoGenerator(int capacity) {
    this.capacity = capacity;
  }


  public String generateDocumentKey(String dbName, IDatabaseCommands databaseCommands, DocumentConvention conventions, Object entity) {
    String key = dbName;
    if (key == null) {
      key = Constants.SYSTEM_DATABASE;
    }
    generators.putIfAbsent(key, new MultiTypeHiLoKeyGenerator(capacity));
    MultiTypeHiLoKeyGenerator generator = generators.get(key);
    return generator.generateDocumentKey(databaseCommands, conventions, entity);
  }
}
