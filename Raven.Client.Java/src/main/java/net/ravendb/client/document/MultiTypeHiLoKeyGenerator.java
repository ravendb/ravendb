package net.ravendb.client.document;

import java.util.HashMap;
import java.util.Map;

import net.ravendb.client.connection.IDatabaseCommands;

import org.apache.commons.lang.StringUtils;


/**
 * Generate a hilo key for each given type
 */
public class MultiTypeHiLoKeyGenerator {
  private final int capacity;
  private final Object generatorLock = new Object();
  private Map<String, HiLoKeyGenerator> keyGeneratorsByTag = new HashMap<>();

  /**
   * Initializes a new instance of the {@link MultiTypeHiLoKeyGenerator} class.
   * @param capacity
   */
  public MultiTypeHiLoKeyGenerator(int capacity) {
    this.capacity = capacity;
  }

  /**
   * Generates the document key.
   * @param databaseCommands
   * @param conventions
   * @param entity
   * @return
   */
  public String generateDocumentKey(IDatabaseCommands databaseCommands, DocumentConvention conventions, Object entity) {
    String typeTagName = conventions.getTypeTagName(entity.getClass());
    if (StringUtils.isEmpty(typeTagName)) { //ignore empty tags
      return null;
    }
    String tag = conventions.getTransformTypeTagNameToDocumentKeyPrefix().transform(typeTagName);
    if (keyGeneratorsByTag.containsKey(tag)) {
      return keyGeneratorsByTag.get(tag).generateDocumentKey(databaseCommands, conventions, entity);
    }
    HiLoKeyGenerator value = null;
    synchronized (generatorLock) {
      if (keyGeneratorsByTag.containsKey(tag)) {
        return keyGeneratorsByTag.get(tag).generateDocumentKey(databaseCommands, conventions, entity);
      }

      value = new HiLoKeyGenerator(tag, capacity);
      keyGeneratorsByTag.put(tag, value);
    }

    return value.generateDocumentKey(databaseCommands, conventions, entity);
  }
}
