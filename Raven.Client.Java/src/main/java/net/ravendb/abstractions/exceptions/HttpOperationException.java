package net.ravendb.abstractions.exceptions;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;


public class HttpOperationException extends RuntimeException {
  private HttpRequest webRequest;
  private HttpResponse httpResponse;
  /**
   * @return the webRequest
   */
  public HttpRequest getWebRequest() {
    return webRequest;
  }
  /**
   * @param webRequest the webRequest to set
   */
  public void setWebRequest(HttpRequest webRequest) {
    this.webRequest = webRequest;
  }
  /**
   * @return the httpResponse
   */
  public HttpResponse getHttpResponse() {
    return httpResponse;
  }
  /**
   * @param httpResponse the httpResponse to set
   */
  public void setHttpResponse(HttpResponse httpResponse) {
    this.httpResponse = httpResponse;
  }

  public HttpOperationException(String message, Throwable cause, HttpRequest webRequest, HttpResponse httpResponse) {
    super(message, cause);
    this.webRequest = webRequest;
    this.httpResponse = httpResponse;
  }

  public int getStatusCode() {
    return httpResponse.getStatusLine().getStatusCode();
  }


}
