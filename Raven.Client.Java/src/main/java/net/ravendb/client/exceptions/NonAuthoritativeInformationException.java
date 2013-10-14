package net.ravendb.client.exceptions;

public class NonAuthoritativeInformationException extends RuntimeException {

  public NonAuthoritativeInformationException() {
    super();
  }

  public NonAuthoritativeInformationException(String message, Throwable cause) {
    super(message, cause);
  }

  public NonAuthoritativeInformationException(String message) {
    super(message);
  }

  public NonAuthoritativeInformationException(Throwable cause) {
    super(cause);
  }

}
