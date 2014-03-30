package net.ravendb.client.connection.implementation;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Action3;
import net.ravendb.abstractions.closure.Delegates;
import net.ravendb.abstractions.connection.HttpRequestHelper;
import net.ravendb.abstractions.connection.OperationCredentials;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.exceptions.BadRequestException;
import net.ravendb.abstractions.exceptions.HttpOperationException;
import net.ravendb.abstractions.exceptions.IndexCompilationException;
import net.ravendb.abstractions.json.linq.JTokenType;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.abstractions.util.NetDateFormat;
import net.ravendb.client.changes.IObservable;
import net.ravendb.client.connection.CachedRequest;
import net.ravendb.client.connection.CreateHttpJsonRequestParams;
import net.ravendb.client.connection.IDocumentStoreReplicationInformer;
import net.ravendb.client.connection.ObservableLineStream;
import net.ravendb.client.connection.ReplicationInformer;
import net.ravendb.client.connection.ServerClient.HandleReplicationStatusChangesCallback;
import net.ravendb.client.connection.profiling.IHoldProfilingInformation;
import net.ravendb.client.connection.profiling.RequestResultArgs;
import net.ravendb.client.connection.profiling.RequestStatus;
import net.ravendb.client.document.Convention;
import net.ravendb.client.document.FailoverBehaviorSet;
import net.ravendb.java.http.client.GzipHttpEntity;
import net.ravendb.java.http.client.HttpEval;
import net.ravendb.java.http.client.HttpReset;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;


public class HttpJsonRequest {

  public static final String clientVersion = Constants.VERSION;

  private final String url;
  private final HttpMethods method;

  private volatile HttpUriRequest webRequest;
  private volatile CloseableHttpResponse httpResponse;
  private CachedRequest cachedRequestDetails;
  private final HttpJsonRequestFactory factory;
  private final IHoldProfilingInformation owner;
  private final Convention conventions;
  private String postedData;
  private final StopWatch sp;
  boolean shouldCacheRequest;
  private InputStream postedStream;
  private boolean disabledAuthRetries;
  private String primaryUrl;
  private String operationUrl;
  private final OperationCredentials _credentials;

  private Map<String, String> responseHeaders;
  private boolean skipServerCheck;
  private int contentLength = -1;

  private CloseableHttpClient httpClient;
  private int responseStatusCode;

  public HttpMethods getMethod() {
    return method;
  }

  public String getUrl() {
    return url;
  }

  private Action3<Map<String, String>, String, String> handleReplicationStatusChanges = Delegates.delegate3();

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
    this._credentials = requestParams.getCredentials();
    this.method = requestParams.getMethod();
    this.webRequest = createWebRequest(requestParams.getUrl(), requestParams.getMethod());
    if (factory.isDisableRequestCompression() == false && requestParams.isDisableRequestCompression() == false) {
      if (method == HttpMethods.POST || method == HttpMethods.PUT || method == HttpMethods.PATCH
        || method == HttpMethods.EVAL) {
        webRequest.addHeader("Content-Encoding", "gzip");
      }
      // Accept-Encoding Parameters are handled by HttpClient
      webRequest.addHeader("Accept-Encoding", "gzip,deflate");
      this.httpClient = factory.getHttpClient();
    } else {
      this.httpClient = factory.getHttpClient();
    }
    // content type is set in RequestEntity
    webRequest.addHeader("Raven-Client-Version", clientVersion);
    writeMetadata(requestParams.getMetadata());
    requestParams.updateHeaders(webRequest);

  }

  public void removeAuthorizationHeader() {
      webRequest.removeHeaders("Authorization");
  }

  private void writeMetadata(RavenJObject metadata) {
    if (metadata == null || metadata.getCount() == 0) {
      return;
    }

    for (Entry<String, RavenJToken> prop : metadata) {
      if (prop.getValue() == null) {
        continue;
      }

      if (prop.getValue().getType() == JTokenType.OBJECT || prop.getValue().getType() == JTokenType.ARRAY) {
        continue;
      }

      String headerName = prop.getKey();
      if ("ETag".equals(headerName)) {
        headerName = "If-None-Match";
      }

      String value = prop.getValue().value(Object.class).toString();

      switch (headerName) {
        case "Content-Length":
          contentLength = prop.getValue().value(int.class);
          break;
        default:
          webRequest.addHeader(headerName, value);

      }
    }
  }

  private HttpUriRequest createWebRequest(String url, HttpMethods method) {

    HttpUriRequest baseMethod = null;

    switch (method) {
      case GET:
        baseMethod = new HttpGet(url);
        break;
      case POST:
        baseMethod = new HttpPost(url);
        if (owner != null) {
          HttpRequestBase requestBase = (HttpRequestBase) baseMethod;
          RequestConfig requestConfig =  RequestConfig.custom().setExpectContinueEnabled(owner.isExpect100Continue()).build();
          requestBase.setConfig(requestConfig);
        }
        break;
      case PUT:
        baseMethod = new HttpPut(url);
        if (owner != null) {
          HttpRequestBase requestBase = (HttpRequestBase) baseMethod;
          RequestConfig requestConfig = RequestConfig.custom().setExpectContinueEnabled(owner.isExpect100Continue()).build();
          requestBase.setConfig(requestConfig);
        }
        break;
      case DELETE:
        baseMethod = new HttpDelete(url);
        break;
      case PATCH:
        baseMethod = new HttpPatch(url);
        break;
      case HEAD:
        baseMethod = new HttpHead(url);
        break;
      case RESET:
        baseMethod = new HttpReset(url);
        break;
      case EVAL:
        baseMethod = new HttpEval(url);
        break;
      default:
        throw new IllegalArgumentException("Unknown method: " + method);
    }

    return baseMethod;
  }

  public CachedRequest getCachedRequestDetails() {
    return this.cachedRequestDetails;
  }

  public void executeRequest() {
    readResponseJson();
  }

  public byte[] readResponseBytes() throws IOException {
    innerExecuteHttpClient();
    InputStream response = httpResponse.getEntity().getContent();
    responseHeaders = extractHeaders(httpResponse.getAllHeaders());
    byte[] result = IOUtils.toByteArray(response);
    EntityUtils.consumeQuietly(httpResponse.getEntity());
    return result;
  }

  public static Map<String, String> extractHeaders(Header[] httpResponseHeaders) {
    Map<String, String> result = new HashMap<>();
    for (Header header : httpResponseHeaders) {
      result.put(header.getName(), header.getValue());
    }
    return result;
  }

  private void innerExecuteHttpClient() {
    try {
      httpResponse = httpClient.execute(webRequest);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    if (httpResponse.getStatusLine().getStatusCode() >= 300) {
      throw new HttpOperationException("Invalid status code:" + httpResponse.getStatusLine().getStatusCode(), null,
        webRequest, httpResponse);
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

  public RavenJToken readResponseJson() {
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
        return readJsonInternal();
      } catch (Exception e) {
        if (++retries >= 3 || disabledAuthRetries) {
          throw e;
        }

        if (e instanceof HttpOperationException) {
          HttpOperationException httpOpException = (HttpOperationException) e;
          if (httpOpException.getStatusCode() != HttpStatus.SC_UNAUTHORIZED
            && httpOpException.getStatusCode() != HttpStatus.SC_FORBIDDEN
            && httpOpException.getStatusCode() != HttpStatus.SC_PRECONDITION_FAILED) {
            throw e;
          }
          if (httpOpException.getStatusCode() == HttpStatus.SC_FORBIDDEN) {
            handleForbiddenResponse(httpOpException.getHttpResponse());
            throw e;
          }
          if (handleUnauthorizedResponse(httpOpException.getHttpResponse()) == false) {
            throw e;
          }
        } else {
          throw e;
        }

      }
    }
  }

  public double calculateDuration() {
    return sp.getTime();
  }

  /**
   * @return
   * @see org.apache.commons.httpclient.HttpMethodBase#getResponseHeaders()
   */
  public Map<String, String> getResponseHeaders() {
    return responseHeaders;
  }

  protected void handleForbiddenResponse(HttpResponse forbiddenResponse) {
    if (conventions.getHandleForbiddenResponse() == null) return;

    conventions.handleForbiddenResponse(forbiddenResponse);
  }

  private boolean handleUnauthorizedResponse(HttpResponse unauthorizedResponse) {
    if (conventions.getHandleUnauthorizedResponse() == null) return false;

    Action1<HttpRequest> handleUnauthorizedResponse = conventions.handleUnauthorizedResponse(unauthorizedResponse, _credentials);
    if (handleUnauthorizedResponse == null) return false;

    recreateWebRequest(handleUnauthorizedResponse);
    return true;
  }

  private void recreateWebRequest(Action1<HttpRequest> action) {
    // we now need to clone the request, since just calling getRequest again wouldn't do anything
    sp.reset();
    sp.start();
    HttpUriRequest newWebRequest = createWebRequest(this.url, this.method);
    HttpRequestHelper.copyHeaders(webRequest, newWebRequest);

    action.apply(newWebRequest);
    if (postedData != null) {
      HttpRequestHelper.writeDataToRequest(newWebRequest, postedData, factory.isDisableRequestCompression());
    }
    if (postedStream != null) {
      try {
        postedStream.reset();
        HttpEntityEnclosingRequestBase requestMethod = (HttpEntityEnclosingRequestBase) webRequest;
        InputStreamEntity streamEntity = new InputStreamEntity(postedStream, 0, ContentType.APPLICATION_JSON);
        streamEntity.setChunked(true);
        requestMethod.setEntity(streamEntity);
      } catch (IOException e) {
        throw new RuntimeException("Unable to reset input stream", e);
      }
    }
    webRequest = newWebRequest;

  }

  private RavenJToken readJsonInternal() {
    InputStream responseStream = null;
    try {
      innerExecuteHttpClient();
      if (httpResponse.getEntity() != null) {
        try {
          responseStream = httpResponse.getEntity().getContent();
        } catch (EOFException e) {
          // ignore
        }
      }
      sp.stop();
    } catch (HttpOperationException e) {
      sp.stop();
      RavenJToken result = handleErrors(e);
      if (result == null) {
        throw e;
      }
      return result;
    } catch (Exception e) {
      sp.stop();
      RavenJToken result = handleErrors(e);
      if (result == null) {
        throw new RuntimeException(e.getMessage(), e);
      }
      return result;
    }

    try {
      responseHeaders = extractHeaders(httpResponse.getAllHeaders());
      responseStatusCode = httpResponse.getStatusLine().getStatusCode();

      handleReplicationStatusChanges.apply(extractHeaders(httpResponse.getAllHeaders()), primaryUrl, operationUrl);

      RavenJToken data = RavenJToken.tryLoad(responseStream);

      if (HttpMethods.GET == method && shouldCacheRequest) {
        factory.cacheResponse(url, data, responseHeaders);
      }

      RequestResultArgs args = new RequestResultArgs();
      args.setDurationMilliseconds(calculateDuration());
      args.setMethod(method);
      args.setHttpResult(getResponseStatusCode());
      args.setStatus(RequestStatus.SEND_TO_SERVER);
      args.setResult((data != null) ? data.toString() : "");
      args.setUrl(getPathAndQuery(webRequest.getURI()));
      args.setPostedData(postedData);

      factory.invokeLogRequest(owner, args);

      return data;
    } finally {
      if (httpResponse != null && httpResponse.getEntity() != null) {
        EntityUtils.consumeQuietly(httpResponse.getEntity());
      }
    }
  }

  private RavenJToken handleErrors(Exception e) {
    if (e instanceof HttpOperationException) {
      HttpOperationException httpWebException = (HttpOperationException) e;
      HttpResponse httpWebResponse = httpWebException.getHttpResponse();
      if (httpWebResponse == null || httpWebException.getStatusCode() == HttpStatus.SC_UNAUTHORIZED
        || httpWebException.getStatusCode() == HttpStatus.SC_NOT_FOUND
        || httpWebException.getStatusCode() == HttpStatus.SC_CONFLICT) {
        int httpResult = httpWebException.getStatusCode();

        RequestResultArgs requestResultArgs = new RequestResultArgs();
        requestResultArgs.setDurationMilliseconds(calculateDuration());
        requestResultArgs.setMethod(method);
        requestResultArgs.setHttpResult(httpResult);
        requestResultArgs.setStatus(RequestStatus.ERROR_ON_SERVER);
        requestResultArgs.setResult(e.getMessage());
        requestResultArgs.setUrl(getPathAndQuery(webRequest.getURI()));
        requestResultArgs.setPostedData(postedData);

        factory.invokeLogRequest(owner, requestResultArgs);

        return null;

      }

      if (httpWebException.getStatusCode() == HttpStatus.SC_NOT_MODIFIED && cachedRequestDetails != null) {
        factory.updateCacheTime(this);
        RavenJToken result = factory.getCachedResponse(this, extractHeaders(httpWebResponse.getAllHeaders()));
        handleReplicationStatusChanges.apply(extractHeaders(httpWebResponse.getAllHeaders()), primaryUrl, operationUrl);

        RequestResultArgs requestResultArgs = new RequestResultArgs();
        requestResultArgs.setDurationMilliseconds(calculateDuration());
        requestResultArgs.setMethod(method);
        requestResultArgs.setStatus(RequestStatus.CACHED);
        requestResultArgs.setResult(e.getMessage());
        requestResultArgs.setUrl(getPathAndQuery(webRequest.getURI()));
        requestResultArgs.setPostedData(postedData);
        factory.invokeLogRequest(owner, requestResultArgs);

        return result;
      }

      try {
        HttpEntity httpEntity = httpWebException.getHttpResponse().getEntity();
        String readToEnd = "";
        if (httpEntity != null) {
          readToEnd = IOUtils.toString(httpEntity.getContent());
        }

        RequestResultArgs requestResultArgs = new RequestResultArgs();
        requestResultArgs.setDurationMilliseconds(calculateDuration());
        requestResultArgs.setMethod(method);
        requestResultArgs.setHttpResult(httpWebResponse.getStatusLine().getStatusCode());
        requestResultArgs.setStatus(RequestStatus.CACHED);
        requestResultArgs.setResult(readToEnd);
        requestResultArgs.setUrl(getPathAndQuery(webRequest.getURI()));
        requestResultArgs.setPostedData(postedData);
        factory.invokeLogRequest(owner, requestResultArgs);

        if (StringUtils.isBlank(readToEnd)) {
          return null; //throws
        }

        RavenJObject ravenJObject = null;
        try {
          ravenJObject = RavenJObject.parse(readToEnd);
        } catch (Exception parseEx) {
          throw new IllegalStateException(readToEnd, e);
        }

        if (ravenJObject.containsKey("IndexDefinitionProperty")) {
          IndexCompilationException ex = new IndexCompilationException(ravenJObject.value(String.class, "Message"));
          ex.setIndexDefinitionProperty(ravenJObject.value(String.class, "IndexDefinitionProperty"));
          ex.setProblematicText(ravenJObject.value(String.class, "ProblematicText"));
          throw ex;
        }

        if (httpWebResponse.getStatusLine().getStatusCode() == HttpStatus.SC_BAD_REQUEST
          && ravenJObject.containsKey("Message")) {
          throw new BadRequestException(ravenJObject.value(String.class, "Message"), e);
        }

        if (ravenJObject.containsKey("Error")) {
          StringBuilder sb = new StringBuilder();
          for (Entry<String, RavenJToken> prop : ravenJObject) {
            if ("Error".equals(prop.getKey())) {
              continue;
            }
            sb.append(prop.getKey()).append(": ").append(prop.getValue().toString());
          }

          sb.append("\n");
          sb.append(ravenJObject.value(String.class, "Error"));
          sb.append("\n");

          throw new IllegalStateException(sb.toString(), e);
        }
        throw new IllegalStateException(readToEnd, e);

      } catch (IOException ee) {
        throw new RuntimeException("Unable to get web response", ee);
      } finally {
        if (httpWebResponse.getEntity() != null) {
          EntityUtils.consumeQuietly(httpWebResponse.getEntity());
        }
      }
    }

    throw new RuntimeException(e);

  }

  public HttpJsonRequest addOperationHeaders(Map<String, String> operationsHeaders) {
    for (Entry<String, String> header : operationsHeaders.entrySet()) {
      webRequest.addHeader(header.getKey(), header.getValue());
    }
    return this;
  }

  public HttpJsonRequest addOperationHeader(String key, String value) {
    webRequest.addHeader(key, value);
    return this;
  }

  public void write(InputStream is) {
    this.postedStream = is;

    ContentType contentType = ContentType.APPLICATION_JSON;
    Header firstHeader = webRequest.getFirstHeader("Content-Type");
    if (firstHeader != null) {
      String contentValue = firstHeader.getValue();
      if (StringUtils.isNotBlank(contentValue)) {
        contentType = ContentType.create(contentValue);
      }
    }

    HttpEntityEnclosingRequestBase requestMethod = (HttpEntityEnclosingRequestBase) webRequest;
    InputStreamEntity innerEntity = new InputStreamEntity(this.postedStream, contentLength, contentType);
    HttpEntity entity = new GzipHttpEntity(innerEntity);
    innerEntity.setChunked(true);
    requestMethod.setEntity(entity);
  }

  public void prepareForLongRequest() {
    setTimeout(6 * 3600 * 1000);
  }

  /**
   * Remember to release resources in HttpResponse entity!
   */
  public CloseableHttpResponse rawExecuteRequest() {
    try {
      httpResponse = httpClient.execute(webRequest);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    if (httpResponse.getStatusLine().getStatusCode() >= 300) {
      try {
        String rawResponse = (httpResponse.getEntity() != null) ? IOUtils.toString(httpResponse.getEntity()
          .getContent()) : "";
        throw new HttpOperationException("Server error response:" + httpResponse.getStatusLine().getStatusCode()
          + rawResponse, null, webRequest, httpResponse);
      } catch (IOException e) {
        throw new RuntimeException("Unable to read response", e);
      } finally {
        if (httpResponse != null && httpResponse.getEntity() != null) {
          EntityUtils.consumeQuietly(httpResponse.getEntity());
        }
      }
    }
    return httpResponse;
  }

  public void write(String data) {
    postedData = data;
    HttpRequestHelper.writeDataToRequest(webRequest, data, factory.isDisableRequestCompression());
  }

  public void setShouldCacheRequest(boolean b) {
    this.shouldCacheRequest = b;
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
  public HttpUriRequest getWebRequest() {
    return webRequest;
  }

  public void setResponseStatusCode(int statusCode) {
    this.responseStatusCode = statusCode;
  }

  public void setResponseHeaders(Map<String, String> map) {
    this.responseHeaders = map;
  }

  public HttpJsonRequest addReplicationStatusHeaders(String thePrimaryUrl, String currentUrl,
    IDocumentStoreReplicationInformer replicationInformer, FailoverBehaviorSet failoverBehavior,
    HandleReplicationStatusChangesCallback handleReplicationStatusChangesCallback) {

    if (thePrimaryUrl.equalsIgnoreCase(currentUrl)) {
      return this;
    }
    if (replicationInformer.getFailureCount(thePrimaryUrl).longValue() <= 0) {
      return this; // not because of failover, no need to do this.
    }

    Date lastPrimaryCheck = replicationInformer.getFailureLastCheck(thePrimaryUrl);
    webRequest.addHeader(Constants.RAVEN_CLIENT_PRIMARY_SERVER_URL, toRemoteUrl(thePrimaryUrl));

    NetDateFormat sdf = new NetDateFormat();
    webRequest.addHeader(Constants.RAVEN_CLIENT_PRIMARY_SERVER_LAST_CHECK, sdf.format(lastPrimaryCheck));

    primaryUrl = thePrimaryUrl;
    operationUrl = currentUrl;

    this.handleReplicationStatusChanges = handleReplicationStatusChangesCallback;
    return this;
  }

  private String toRemoteUrl(String thePrimaryUrl) {
    try {
      URIBuilder uriBuilder = new URIBuilder(thePrimaryUrl);
      if ("localhost".equals(uriBuilder.getHost()) || "127.0.0.1".equals(uriBuilder.getHost())) {
        uriBuilder.setHost(InetAddress.getLocalHost().getHostName());
      }
      return uriBuilder.toString();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid URI:" + thePrimaryUrl, e);
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to fetch hostname", e);
    }
  }

  public void setTimeout(long timeoutInMilis) {
    HttpRequestBase baseRequest = (HttpRequestBase) webRequest;
    RequestConfig requestConfig = baseRequest.getConfig();
    if (requestConfig == null) {
      requestConfig = RequestConfig.DEFAULT;
    }

    requestConfig = RequestConfig.copy(requestConfig).setSocketTimeout((int) timeoutInMilis).setConnectTimeout((int) timeoutInMilis).build();
    baseRequest.setConfig(requestConfig);
  }

  public IObservable<String> serverPull() {
    int retries = 0;

    while (true) {
      HttpOperationException webException = null;
      try {
        HttpUriRequest webRequest = createWebRequest(url, method);
        httpResponse = httpClient.execute(webRequest);
        if (httpResponse.getStatusLine().getStatusCode() >= 300) {
          throw new HttpOperationException("Unable to connect to changes API", null, webRequest, httpResponse);
        }
        final ObservableLineStream observableLineStream = new ObservableLineStream(httpResponse.getEntity().getContent(),  Delegates.delegate0());
        setResponseHeaders(extractHeaders(httpResponse.getAllHeaders()));
        observableLineStream.start();

        return observableLineStream;
      } catch (Exception e) {
        if (++retries >= 3 || disabledAuthRetries || !(e instanceof HttpOperationException)) {
          throw new RuntimeException(e);
        }
        webException = (HttpOperationException) e;
        if (webException.getStatusCode() != HttpStatus.SC_UNAUTHORIZED &&
          webException.getStatusCode() != HttpStatus.SC_FORBIDDEN &&
          webException.getStatusCode() != HttpStatus.SC_PRECONDITION_FAILED) {
          throw webException;
        }
      }

      if (webException.getStatusCode() == HttpStatus.SC_FORBIDDEN) {
        handleForbiddenResponse(webException.getHttpResponse());
        throw webException;
      }

      if (!handleUnauthorizedResponse(webException.getHttpResponse())) {
        throw webException;
      }

    }

  }

}
