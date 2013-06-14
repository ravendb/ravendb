package raven.client.connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import javax.management.RuntimeErrorException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpParamsNames;
import org.apache.http.util.EntityUtils;

import raven.abstractions.connection.profiling.RequestResultArgs;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.exceptions.HttpOperationException;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.connection.ServerClient.HandleReplicationStatusChangesCallback;
import raven.client.connection.profiling.RequestStatus;
import raven.client.document.DocumentConvention;
import raven.client.document.FailoverBehavior;

//TODO: review me
/**
 * TODO:
 *
 * How to set content type (in POST / PUT)
 * How to set request timeout
 * request - ifModifiedSince
 * request.Accept
 * request.Connection
 * requset.SendChunked
 *
 */
public class HttpJsonRequest implements AutoCloseable {

  private final String url;
  private final HttpMethods method;

  private volatile HttpRequestBase webRequest;
  private CachedRequest cachedRequestDetails;
  private final HttpJsonRequestFactory factory;
  private final ServerClient owner;
  private final DocumentConvention conventions;
  private String postedData;
  private final StopWatch sp;
  boolean shouldCacheRequest;
  private InputStream postedStream;
  private boolean writeCalled;
  private boolean disabledAuthRetries;
  private String primaryUrl;
  private String operationUrl;
  private Map<String, String> responseHeaders;
  private boolean skipServerCheck;

  private HttpClient httpClient;
  private int responseStatusCode;


  //TODO: public Action<NameValueCollection, string, string> HandleReplicationStatusChanges = delegate { };

  /**
   * @return the skipServerCheck
   */
  public boolean isSkipServerCheck() {
    return skipServerCheck;
  }

  public HttpJsonRequest(CreateHttpJsonRequestParams requestParams, HttpJsonRequestFactory factory) {
    sp = new StopWatch();
    sp.start();

    this.url = requestParams.getUrl();
    this.factory = factory;
    this.owner = requestParams.getOwner();
    this.conventions = requestParams.getConvention();
    this.method = requestParams.getMethod();
    this.httpClient = factory.getHttpClient();
    this.webRequest = createWebRequest(requestParams);
    if (factory.isDisableRequestCompression() == false && requestParams.isDisableRequestCompression() == false) {
      switch (requestParams.getMethod()) {
      case POST:
      case PUT:
      case PATCH:
      case EVAL:
        webRequest.addHeader("Content-Encoding", "gzip");
        break;
      }
    }
    //TODO: now client works only as GZIP!
    webRequest.addHeader("Accept-Encoding", "gzip");
    // content type is set in RequestEntity
    //TODO: header client version
    writeMetadata(requestParams.getMetadata());
    requestParams.updateHeaders(webRequest);

  }

  private void writeMetadata(RavenJObject metadata) {
    // TODO Auto-generated method stub

  }

  public void disableAuthentication() {
    //TODO: webRequest.setDoAuthentication(false);
    //TODO: httpClient.getState().clearCredentials();
    disabledAuthRetries = true;
  }

  /* TODO:
   * Async methods:
   * public Task ExecuteRequestAsync()
   * public async Task<RavenJToken> ReadResponseJsonAsync()
   * public async Task<byte[]> ReadResponseBytesAsync()
   */

  private HttpRequestBase createWebRequest(CreateHttpJsonRequestParams requestParams) {

    String url = requestParams.getUrl();
    HttpRequestBase baseMethod = null;


    switch (requestParams.getMethod()) {
    case GET:
      baseMethod = new HttpGet(url);
      break;
    case POST:
      baseMethod = new HttpPost(url);
      break;
    case PUT:
      baseMethod = new HttpPut(url);
      break;
    case DELETE:
      baseMethod = new HttpDelete(url);
      break;
    default:
      throw new IllegalArgumentException("Unknown method: " + requestParams.getMethod());
    }

    /*TODO
    webRequest.UseDefaultCredentials = true;
    webRequest.Credentials = requestParams.Credentials;
     */
    return baseMethod;
  }

  public CachedRequest getCachedRequestDetails() {
    //TODO:
    return null;
  }


  @Override
  public void close() throws Exception {

  }

  private UUID etagHeaderToGuid(Header responseHeader) {
    if (StringUtils.isEmpty(responseHeader.getValue())) {
      throw new IllegalStateException("Response didn't had an ETag header");
    }
    String value = responseHeader.getValue();
    if (value.startsWith("\"")) {
      return UUID.fromString(value.substring(1, value.length() - 2));
    }
    return UUID.fromString(value);
  }

  public void executeRequest() throws IOException {
    readResponseJson();
  }

  public byte[] readResponseBytes() throws IOException {
    if (writeCalled == false) {
      //TODO:  content length set to 0
    }
    HttpResponse httpResponse = null;
    try {
      httpResponse = getResponse();
      try (InputStream response = httpResponse.getEntity().getContent()) {
        //TODO: do we need http decompression ?
        responseHeaders = extractHeaders(httpResponse.getAllHeaders());
        return IOUtils.toByteArray(response);
      }
    } finally {
      EntityUtils.consumeQuietly(httpResponse.getEntity());
    }

  }

  private Map<String, String> extractHeaders(Header[] httpResponseHeaders) {
    Map<String, String> result = new HashMap<>();
    for (Header header: httpResponseHeaders) {
      result.put(header.getName(), header.getValue());
    }
    return result;
  }

  private HttpResponse getResponse() {
    HttpResponse httpResponse = null;
    try {
      httpResponse = httpClient.execute(webRequest);
      if (httpResponse.getStatusLine().getStatusCode() >= 400) {
        throw new HttpOperationException("Invalid status code:" + httpResponse.getStatusLine().getStatusCode(),null, webRequest, httpResponse);
      }
      return httpResponse;
    } catch(Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  /**
   * @return the responseStatusCode
   */
  public int getResponseStatusCode() {
    return responseStatusCode;
  }

  private static String getPathAndQuery(URI src) {
    return src.getPath() + ((src.getQuery() != null) ? "?" + src.getQuery() : "");
  }

  public RavenJToken readResponseJson() throws IOException {
    if (skipServerCheck) {
      RavenJToken result = factory.getCachedResponse(this, null);

      RequestResultArgs args = new RequestResultArgs();
      args.setDurationMilliseconds(calculateDuration());
      args.setMethod(method);
      args.setHttpResult(getResponseStatusCode());
      args.setStatus(RequestStatus.AGGRESSIVELY_CACHED);
      args.setResult(result.toString());
      args.setUrl(getPathAndQuery(webRequest.getURI()));
      args.setPostedData(postedData);

      factory.invokeLogRequest(owner, args);

      return result;
    }
    int retries = 0;
    while (true) {
      try {
        if (writeCalled == false) {
          //TODO: set content length
        }
        return readJsonInternal();
      } catch (HttpOperationException e) {
        if (++retries >= 3 || disabledAuthRetries) {
          throw e;
        }
        if (e.getStatusCode() != HttpStatus.SC_UNAUTHORIZED &&
            e.getStatusCode() != HttpStatus.SC_FORBIDDEN &&
            e.getStatusCode() != HttpStatus.SC_PRECONDITION_FAILED) {
          throw e;
        }
        if (e.getStatusCode() == HttpStatus.SC_FORBIDDEN) {
          handleForbiddenResponse();
          throw e;
        }
        if (handleUnauthorizedResponse() == false) {
          throw e;
        }
      }
    }
  }

  private double calculateDuration() {
    // TODO Auto-generated method stub
    return 0;
  }

  public RavenJObject filterHeadersAttachment() {
    // TODO Auto-generated method stub
    return null;
  }


  /**
   * @return
   * @see org.apache.commons.httpclient.HttpMethodBase#getResponseHeaders()
   */
  public Map<String, String> getResponseHeaders() {
    return responseHeaders;
  }

  protected void handleForbiddenResponse() {
    //TODO:
    throw new RuntimeException();
  }

  private boolean handleUnauthorizedResponse() {
    //TODO: finish me
    throw new RuntimeException();
  }

  private RavenJToken readJsonInternal() {
    HttpResponse response = null;
    InputStream responseStream = null;
    try {
      try {
        response = getResponse(); //TODO: we double throw httpOperationException
        responseStream = response.getEntity().getContent();
        sp.stop();
      } catch (Exception e) {
        sp.stop();
        RavenJToken result = handleErrors(e);
        if (result == null) {
          throw new RuntimeException(e.getMessage(), e);
        }
        return result;
      }

      responseHeaders = extractHeaders(response.getAllHeaders());
      responseStatusCode = response.getStatusLine().getStatusCode();

      //TODO: HandleReplicationStatusChanges(ResponseHeaders, primaryUrl, operationUrl);
      //TODO: do we need gzip decoder ?
      //TODO: close responseStream?

      RavenJToken data = RavenJToken.parse(responseStream); //TODO replace with try load

      if (HttpMethods.GET == method && shouldCacheRequest) {
        factory.cacheResponse(url, data, responseHeaders);
      }

      RequestResultArgs args = new RequestResultArgs();
      args.setDurationMilliseconds(calculateDuration());
      args.setMethod(method);
      args.setHttpResult(getResponseStatusCode());
      args.setStatus(RequestStatus.SEND_TO_SERVER);
      args.setResult(data.toString());
      args.setUrl(getPathAndQuery(webRequest.getURI()));
      args.setPostedData(postedData);

      factory.invokeLogRequest(owner, args);

      //TODO: invoke log request

      return data;
    } finally {
      if (response != null) {
        EntityUtils.consumeQuietly(response.getEntity());
      }
    }
  }
  private RavenJToken handleErrors(Exception e) {
    // TODO Auto-generated method stub
    throw new IllegalStateException("Not implemented yet!", e);
  }

  //TODO: private void RecreateWebRequest(Action<HttpWebRequest> action)
  //TODO:public HttpJsonRequest AddOperationHeaders(IDictionary<string, string> operationsHeaders)
  //TODO: public HttpJsonRequest AddOperationHeader(string key, string value)

  public void write(InputStream is) {
    writeCalled = true;
    postedStream = is;
    //TODO: interceptors to handle gzip compression
    HttpEntityEnclosingRequestBase requestMethod = (HttpEntityEnclosingRequestBase) webRequest;
    InputStreamEntity streamEntity = new InputStreamEntity(is, 0, ContentType.APPLICATION_JSON);
    streamEntity.setChunked(true);
    requestMethod.setEntity(streamEntity);
  }

  public void prepareForLongRequest() {
    setTimeout(6 * 3600);
    //TODO: webRequest.AllowWriteStreamBuffering = false;
  }


  private void setTimeout(int seconds) {
    // TODO Auto-generated method stub

  }

  public void rawExecuteRequest() {
    //TODO: implemenent me!
  }

  public void write(String data) throws UnsupportedEncodingException {
    writeCalled = true;
    postedData = data;

    HttpEntityEnclosingRequestBase requestMethod = (HttpEntityEnclosingRequestBase) webRequest;
    requestMethod.setEntity(new StringEntity(data, ContentType.APPLICATION_JSON));

    //TODO: use HttpRequestHelper.WriteDataToRequest(webRequest, data, factory.DisableRequestCompression); (compression)
  }

  public void setShouldCacheRequest(boolean b) {
    this.shouldCacheRequest  = b;
  }

  public boolean getShouldCacheRequest() {
    return this.shouldCacheRequest;
  }

  public void setCachedRequestDetails(CachedRequest cachedRequest) {
    this.cachedRequestDetails = cachedRequest;
  }

  public void setSkipServerCheck(boolean skipServerCheck) {
    this.skipServerCheck = skipServerCheck;
  }

  /**
   * @return the methodBase
   */
  public HttpRequestBase getWebRequest() {
    return webRequest;
  }

  public void setResponseStatusCode(int scNotModified) {
    // TODO Auto-generated method stub

  }

  public void setResponseHeaders(Map<String, String> map) {
    this.responseHeaders = map;
  }

  public HttpJsonRequest addReplicationStatusHeaders(String primaryUrl, String currentUrl, ReplicationInformer replicationInformer, FailoverBehavior failoverBehavior,
      HandleReplicationStatusChangesCallback handleReplicationStatusChangesCallback) {
    // TODO Auto-generated method stub
    return this;
  }


}
