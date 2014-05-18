package net.ravendb.abstractions.exceptions;

public class ServerClientException extends RuntimeException {

  public ServerClientException() {
    super();
  }

  public ServerClientException(String message, Throwable cause) {
    super(message, cause);
  }

  public ServerClientException(String message) {
    super(message);
  }

  public ServerClientException(Throwable cause) {
    super(cause);
  }

}
