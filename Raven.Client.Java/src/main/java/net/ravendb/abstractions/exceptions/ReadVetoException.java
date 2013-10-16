package net.ravendb.abstractions.exceptions;

public class ReadVetoException extends RuntimeException {

  public ReadVetoException() {
    super();
  }

  public ReadVetoException(String message, Throwable cause) {
    super(message, cause);
  }

  public ReadVetoException(String message) {
    super(message);
  }

  public ReadVetoException(Throwable cause) {
    super(cause);
  }

}
