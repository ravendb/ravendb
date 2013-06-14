package raven.client.connection;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.InputStreamRequestEntity;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.params.HttpParams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;

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

  private volatile HttpMethodBase webRequest;
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
        webRequest.addRequestHeader("Content-Encoding", "gzip");
        break;
      }
    }

    //TODO: webRequest.addRequestHeader("Accept-Encoding", "gzip");
    //TODO: webRequest.ContentType = "application/json; charset=utf-8";
    //TODO: header client version
    writeMetadata(requestParams.getMetadata());
    requestParams.updateHeaders(webRequest);

  }

  private void writeMetadata(RavenJObject metadata) {
    // TODO Auto-generated method stub

  }

  public void disableAuthentication() {
    webRequest.setDoAuthentication(false);
    httpClient.getState().clearCredentials();
    disabledAuthRetries = true;
  }

  /* TODO:
   * Async methods:
   * public Task ExecuteRequestAsync()
   * public async Task<RavenJToken> ReadResponseJsonAsync()
   * public async Task<byte[]> ReadResponseBytesAsync()
   */

  private HttpMethodBase createWebRequest(CreateHttpJsonRequestParams requestParams) {

    String url = requestParams.getUrl();
    HttpMethodBase baseMethod = null;


    switch (requestParams.getMethod()) {
    case GET:
      baseMethod = new GetMethod(url);
      break;
    case POST:
      baseMethod = new PostMethod(url);
      break;
    case PUT:
      baseMethod = new PutMethod(url);
      break;
    case DELETE:
      baseMethod = new DeleteMethod(url);
      break;
    default:
      throw new IllegalArgumentException("Unknown method: " + requestParams.getMethod());
    }

    baseMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(0, false));
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
    if (webRequest != null) {
      webRequest.releaseConnection();
    }
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

  public void executeRequest() throws HttpException, IOException {
    readResponseJson();
  }

  public byte[] readResponseBytes() {
    if (writeCalled == false) {
      //TODO:  content length set to 0
    }
    try {
      executeMethod();
      try (InputStream response = webRequest.getResponseBodyAsStream()) {
        //TODO: do we need http decompression ?
        responseHeaders = extractHeaders(webRequest.getResponseHeaders());
        return IOUtils.toByteArray(response);
      }

    } catch (IOException e) {
      throw new HttpOperationException(e.getMessage(), e, webRequest);
    }
  }

  private Map<String, String> extractHeaders(Header[] httpResponseHeaders) {
    Map<String, String> result = new HashMap<>();
    for (Header header: httpResponseHeaders) {
      result.put(header.getName(), header.getValue());
    }
    return result;
  }

  private void executeMethod() {
    try {
      if (webRequest.isRequestSent()) {
        return;
      }
      int responseStatus = httpClient.executeMethod(webRequest);
      if (responseStatus >= 400) {
        throw new HttpOperationException(webRequest);
      }
    } catch (Exception e) {
      throw new HttpOperationException(e.getMessage(), e, webRequest);
    }
  }

  /**
   * @return the responseStatusCode
   */
  public int getResponseStatusCode() {
    return responseStatusCode;
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
      args.setUrl(webRequest.getURI().getPathQuery());
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

  public UUID getEtagHeader() {
    return etagHeaderToGuid(webRequest.getResponseHeader("ETag"));
  }

  public byte[] getResponseBytes() throws IOException {
    return webRequest.getResponseBody();
  }


  public String getResponseHeader(String key) {
    Header responseHeader = webRequest.getResponseHeader(key);
    if (responseHeader != null) {
      return responseHeader.getValue();
    }
    return null;
  }

  /**
   * @return
   * @see org.apache.commons.httpclient.HttpMethodBase#getResponseHeaders()
   */
  public Map<String, String> getResponseHeaders() {
    //TODO: connect to method base!
    return responseHeaders;
  }

  protected void handleForbiddenResponse() {
    //TODO:
    throw new HttpOperationException(webRequest);
  }

  private boolean handleUnauthorizedResponse() {
    //TODO: finish me
    throw new HttpOperationException(webRequest);
  }

  private RavenJToken readJsonInternal() throws URIException {
    InputStream responseStream = null;
    try {
      executeMethod(); //TODO: we double throw httpOperationException
      responseStream = webRequest.getResponseBodyAsStream();
      sp.stop();
    } catch (Exception e) {
      sp.stop();
      RavenJToken result = handleErrors(e);
      if (result == null) {
        throw new HttpOperationException(webRequest);
      }
      return result;
    }

    responseHeaders = extractHeaders(webRequest.getResponseHeaders());
    responseStatusCode = webRequest.getStatusCode();

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
    args.setUrl(webRequest.getURI().getPathQuery());
    args.setPostedData(postedData);

    factory.invokeLogRequest(owner, args);

    //TODO: invoke log request

    return data;
  }
  private RavenJToken handleErrors(Exception e) {
    // TODO Auto-generated method stub
    throw new IllegalStateException("Not implemented yet!");
  }

  //TODO: private void RecreateWebRequest(Action<HttpWebRequest> action)
  //TODO:public HttpJsonRequest AddOperationHeaders(IDictionary<string, string> operationsHeaders)
  //TODO: public HttpJsonRequest AddOperationHeader(string key, string value)

  public void write(InputStream is) {
    writeCalled = true;
    postedStream = is;
    //TODO: webRequest.setChunked(trye);
    //TODO: interceptors to handle gzip compression
    EntityEnclosingMethod requestMethod = (EntityEnclosingMethod) webRequest;
    requestMethod.setRequestEntity(new InputStreamRequestEntity(is));

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

    EntityEnclosingMethod requestMethod = (EntityEnclosingMethod) webRequest;
    requestMethod.setRequestEntity(new StringRequestEntity(data, "application/json", "utf-8"));

    //TODO: use HttpRequestHelper.WriteDataToRequest(webRequest, data, factory.DisableRequestCompression); (compression)
  }

  public void setShouldCacheRequest(boolean b) {
    // TODO Auto-generated method stub

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
  public HttpMethodBase getWebRequest() {
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
