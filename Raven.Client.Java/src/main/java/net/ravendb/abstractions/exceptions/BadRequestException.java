package net.ravendb.abstractions.exceptions;

public class BadRequestException extends RuntimeException {

  public BadRequestException() {
    super();
  }

  public BadRequestException(String message, Throwable cause) {
    super(message, cause);
  }

  public BadRequestException(String message) {
    super(message);
  }

  public BadRequestException(Throwable cause) {
    super(cause);
  }

}
