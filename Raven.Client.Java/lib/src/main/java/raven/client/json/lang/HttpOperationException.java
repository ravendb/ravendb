package raven.client.json.lang;

public class HttpOperationException extends RuntimeException {
  private int statusCode;

  /**
   * @return the statusCode
   */
  public int getStatusCode() {
    return statusCode;
  }

  public HttpOperationException(int statusCode) {
    super("statusCode = " + statusCode);
    this.statusCode = statusCode;
  }

  public HttpOperationException(String message, Throwable cause, int statusCode) {
    super("statusCode = " + statusCode + message, cause);
    this.statusCode = statusCode;
  }

  public HttpOperationException(String message, int statusCode) {
    super("statusCode = " + statusCode + message);
    this.statusCode = statusCode;
  }

  public HttpOperationException(Throwable cause, int statusCode) {
    super(cause);
    this.statusCode = statusCode;
  }




}
