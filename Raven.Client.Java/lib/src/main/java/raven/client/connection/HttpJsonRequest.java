package raven.client.connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.lang.StringUtils;

import raven.abstractions.exceptions.HttpOperationException;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.connection.ServerClient.HandleReplicationStatusChangesCallback;
import raven.client.document.FailoverBehavior;

//TODO: review me
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


  public byte[] getResponseBytes() throws IOException {
    return methodBase.getResponseBody();
  }

  /**
   * @return
   * @see org.apache.commons.httpclient.HttpMethodBase#getResponseHeaders()
   */
  public Header[] getResponseHeaders() {
    return methodBase.getResponseHeaders();
  }

  public RavenJToken getResponseAsJson(Integer... expectedStatus) throws IOException {
    respCode = httpClient.executeMethod(methodBase);

    if (!Arrays.asList(expectedStatus).contains(respCode) && HttpStatus.SC_UNAUTHORIZED != respCode
        && HttpStatus.SC_FORBIDDEN != respCode && HttpStatus.SC_PRECONDITION_FAILED != respCode) {
      throw new HttpOperationException(methodBase);
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
    throw new HttpOperationException(methodBase);
  }


  /**
   * @return the respCode
   */
  public int getResponseCode() {
    return respCode;
  }

  protected void handleForbiddenResponse() {
    throw new HttpOperationException(methodBase);
  }

  public void write(InputStream is) {
    if (methodBase.isRequestSent()) {
      throw new IllegalStateException("Request was already sent!");
    }
    EntityEnclosingMethod postMethod = (EntityEnclosingMethod) methodBase;
    postMethod.setRequestEntity(new InputStreamRequestEntity(is));
  }

  public void write(String string) throws UnsupportedEncodingException {
    if (methodBase.isRequestSent()) {
      throw new IllegalStateException("Request was already sent!");
    }
    EntityEnclosingMethod postMethod = (EntityEnclosingMethod) methodBase;
    postMethod.setRequestEntity(new StringRequestEntity(string, "application/json", "utf-8"));
  }



  public void executeRequest() throws HttpException, IOException {
    respCode = httpClient.executeMethod(methodBase);

    if (respCode >= 400) {
      throw new HttpOperationException(methodBase);
    }
  }

  public UUID getEtagHeader() {
    return etagHeaderToGuid(methodBase.getResponseHeader("ETag"));
  }

  private UUID etagHeaderToGuid(Header responseHeader) {
    if (StringUtils.isEmpty(responseHeader.getValue())) {
      throw new IllegalStateException("Response didn't had an ETag header");
    }
    String value = responseHeader.getValue();
    if (value.startsWith("\"")) {
      return UUID.fromString(value.substring(1, value.length() -2));
    }
    return  UUID.fromString(value);
  }

  public RavenJObject filterHeadersAttachment() {
    // TODO Auto-generated method stub
    return null;
  }

  public HttpJsonRequest addReplicationStatusHeaders(String url, String serverUrl, ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior,
      HandleReplicationStatusChangesCallback handleReplicationStatusChangesCallback) {
    // TODO Auto-generated method stub
    return null;
  }

}
