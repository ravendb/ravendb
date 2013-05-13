package raven.client.connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

import raven.client.json.RavenJToken;
import raven.client.json.lang.HttpOperationException;

public class HttpJsonRequest implements AutoCloseable {

  private HttpClient httpClient;
  private HttpMethodBase methodBase;
  private int respCode;

  public HttpJsonRequest(HttpClient httpClient, HttpMethodBase methodBase) {
    super();
    this.httpClient = httpClient;
    this.methodBase = methodBase;
  }

  @Override
  public void close() throws Exception {
    if (methodBase != null) {
      methodBase.releaseConnection();
    }
  }

  public String getResponseHeader(String key) {
    Header responseHeader = methodBase.getResponseHeader(key);
    if (responseHeader != null) {
      return responseHeader.getValue();
    }
    return null;
  }


  /**
   * @return
   * @see org.apache.commons.httpclient.HttpMethodBase#getResponseHeaders()
   */
  public Header[] getResponseHeaders() {
    return methodBase.getResponseHeaders();
  }

  public RavenJToken getResponseAsJson(int expectedStatus) throws HttpException, IOException {
    respCode = httpClient.executeMethod(methodBase);

    if (expectedStatus != respCode && HttpStatus.SC_UNAUTHORIZED != respCode
        && HttpStatus.SC_FORBIDDEN != respCode && HttpStatus.SC_PRECONDITION_FAILED != respCode) {
      throw new HttpOperationException(respCode);
    }

    if (HttpStatus.SC_FORBIDDEN == respCode) {
      handleForbiddenResponse();
    }
    if (HttpStatus.SC_PRECONDITION_FAILED == respCode || HttpStatus.SC_FORBIDDEN == respCode) {
      handleUnauthorizedResponse(respCode);
    }

    // we have expectedStatus

    return readJsonInternal();
  }

  private RavenJToken readJsonInternal() throws IOException {
    InputStream bodyAsStream = methodBase.getResponseBodyAsStream();
    return RavenJToken.parse(bodyAsStream);
  }

  private void handleUnauthorizedResponse(int respCode) {
    throw new HttpOperationException(respCode);
  }


  /**
   * @return the respCode
   */
  public int getResponseCode() {
    return respCode;
  }

  protected void handleForbiddenResponse() {
    throw new HttpOperationException(HttpStatus.SC_FORBIDDEN);
  }

  public void write(String string) throws UnsupportedEncodingException {
    //TODO: check cast
    EntityEnclosingMethod postMethod = (EntityEnclosingMethod) methodBase;
    postMethod.setRequestEntity(new StringRequestEntity(string, "application/json", "utf-8"));
    // TODO Auto-generated method stub

  }

}
