package raven.client.json.lang;

import org.apache.commons.httpclient.HttpMethodBase;

public class HttpOperationException extends RuntimeException {
  private HttpMethodBase methodBase;

  /**
   * @return the methodBase
   */
  public HttpMethodBase getMethodBase() {
    return methodBase;
  }

  public HttpOperationException(HttpMethodBase methodBase) {
    super("statusCode = " + methodBase.getStatusCode());
    this.methodBase = methodBase;
  }

  public int getStatusCode() {
    return methodBase.getStatusCode();
  }

  public HttpOperationException(String message, Throwable cause, HttpMethodBase methodBase) {
    super("statusCode = " + methodBase.getStatusCode() + message, cause);
    this.methodBase = methodBase;
  }

  public HttpOperationException(String message, HttpMethodBase methodBase) {
    super("statusCode = " + methodBase.getStatusCode() + message);
    this.methodBase = methodBase;
  }

  public HttpOperationException(Throwable cause, HttpMethodBase methodBase) {
    super(cause);
    this.methodBase = methodBase;
  }




}
