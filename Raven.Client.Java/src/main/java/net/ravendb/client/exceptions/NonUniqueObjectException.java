package net.ravendb.client.exceptions;

/**
 * This exception is thrown when a separate instance of an entity is added to the session
 * when a different entity with the same key already exists within the session.
 *
 */
public class NonUniqueObjectException extends RuntimeException {

  public NonUniqueObjectException() {
    super();
  }

  public NonUniqueObjectException(String message, Throwable cause) {
    super(message, cause);
  }

  public NonUniqueObjectException(String message) {
    super(message);
  }

  public NonUniqueObjectException(Throwable cause) {
    super(cause);
  }

}
