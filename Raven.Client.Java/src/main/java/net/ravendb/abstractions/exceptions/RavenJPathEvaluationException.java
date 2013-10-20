package net.ravendb.abstractions.exceptions;

public class RavenJPathEvaluationException extends RuntimeException {

  public RavenJPathEvaluationException() {
    super();
  }

  public RavenJPathEvaluationException(String message, Throwable cause) {
    super(message, cause);
  }

  public RavenJPathEvaluationException(String message) {
    super(message);
  }

  public RavenJPathEvaluationException(Throwable cause) {
    super(cause);
  }

}
