package net.ravendb.abstractions.exceptions;

public class TransformCompilationException extends RuntimeException {

  public TransformCompilationException() {
    super();
  }

  public TransformCompilationException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransformCompilationException(String message) {
    super(message);
  }

  public TransformCompilationException(Throwable cause) {
    super(cause);
  }

}
