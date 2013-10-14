package net.ravendb.abstractions.exceptions;

public class JsonWriterException extends RuntimeException {

  public JsonWriterException() {
    super();
  }

  public JsonWriterException(String message, Throwable cause) {
    super(message, cause);
  }

  public JsonWriterException(String message) {
    super(message);
  }

  public JsonWriterException(Throwable cause) {
    super(cause);
  }

}
