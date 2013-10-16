package net.ravendb.abstractions.exceptions;

public class DocumentDoesNotExistsException extends RuntimeException {

  public DocumentDoesNotExistsException() {
    super();
  }

  public DocumentDoesNotExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public DocumentDoesNotExistsException(String message) {
    super(message);
  }

  public DocumentDoesNotExistsException(Throwable cause) {
    super(cause);
  }

}
