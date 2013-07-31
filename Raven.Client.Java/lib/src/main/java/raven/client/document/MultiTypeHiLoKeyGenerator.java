package raven.client.document;

import raven.client.connection.IDatabaseCommands;

/**
 * Generate a hilo key for each given type
 */
public class MultiTypeHiLoKeyGenerator {
  private final int capacity;
  //TODO: finish me!

  /**
   * Initializes a new instance of the {@link MultiTypeHiLoKeyGenerator} class.
   * @param capacity
   */
  public MultiTypeHiLoKeyGenerator(int capacity) {
    this.capacity = capacity;
  }

  public String generateDocumentKey(IDatabaseCommands databaseCommands, DocumentConvention conventions, Object entity) {
    //TODO :
    return null;
  }
}
