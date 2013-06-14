package raven.abstractions.exceptions;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;


public class HttpOperationException extends RuntimeException {
  private HttpRequestBase webRequest;
  private HttpResponse httpResponse;
  /**
   * @return the webRequest
   */
  public HttpRequestBase getWebRequest() {
    return webRequest;
  }
  /**
   * @param webRequest the webRequest to set
   */
  public void setWebRequest(HttpRequestBase webRequest) {
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

  public HttpOperationException(String message, Throwable cause, HttpRequestBase webRequest, HttpResponse httpResponse) {
    super(message, cause);
    this.webRequest = webRequest;
    this.httpResponse = httpResponse;
  }

  public int getStatusCode() {
    return httpResponse.getStatusLine().getStatusCode();
  }


}
