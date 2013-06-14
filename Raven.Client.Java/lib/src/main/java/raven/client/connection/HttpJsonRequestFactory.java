package raven.client.connection;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DecompressingHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;

import raven.abstractions.basic.EventHandler;
import raven.abstractions.basic.EventHelper;
import raven.abstractions.connection.WebRequestEventArgs;
import raven.abstractions.connection.profiling.RequestResultArgs;
import raven.abstractions.data.Constants;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.extensions.MultiDatabase;
import raven.client.util.SimpleCache;

/**
 * Create the HTTP Json Requests to the RavenDB Server
 * and manages the http cache
 */
public class HttpJsonRequestFactory implements AutoCloseable {

  private HttpClient httpClient;

  private List<EventHandler<WebRequestEventArgs>> configureRequest = new ArrayList<>();

  private List<EventHandler<RequestResultArgs>> logRequest = new ArrayList<>();

  private final int maxNumberOfCachedRequests;
  private SimpleCache cache;
  protected AtomicInteger numOfCachedRequests = new AtomicInteger();
  protected int numOfCacheResets;
  private boolean disableRequestCompression;
  private boolean enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers;
  private ThreadLocal<Long> aggressiveCacheDuration = new ThreadLocal<>(); // in milis
  private ThreadLocal<Boolean> disableHttpCaching = new ThreadLocal<>();
  private volatile boolean disposed;

  public HttpJsonRequestFactory(int maxNumberOfCachedRequests) {
    super();
    DefaultHttpClient innerClient = new DefaultHttpClient();
    this.httpClient = new DecompressingHttpClient(innerClient);
    innerClient.setHttpRequestRetryHandler(new StandardHttpRequestRetryHandler(0, false));
    this.maxNumberOfCachedRequests = maxNumberOfCachedRequests;
    resetCache();
  }


  public void addConfgureRequestEventHandler(EventHandler<WebRequestEventArgs> event) {
    configureRequest.add(event);
  }


  public void addLogRequestEventHandler(EventHandler<RequestResultArgs> event) {
    logRequest.add(event);
  }

  /**
   * @return the httpClient
   */
  public HttpClient getHttpClient() {
    return httpClient;
  }

  protected void cacheResponse(String url, RavenJToken data, Map<String, String> headers) {
    if (StringUtils.isEmpty(headers.get("ETag"))) {
      return;
    }

    RavenJToken clone = data.cloneToken();
    clone.ensureCannotBeChangeAndEnableShapshotting();

    cache.set(url, new CachedRequest(clone, new Date(), new HashMap<>(headers), MultiDatabase.getDatabaseName(url), false));

  }

  @Override
  public void close() throws Exception {
    if (disposed) {
      return ;
    }
    disposed = true;
    cache.close();
  }

  private CachedRequestOp configureCaching(String url, HttpJsonRequest request) {
    CachedRequest cachedRequest = cache.get(url);
    if (cachedRequest == null) {
      return new CachedRequestOp(null, false);
    }
    boolean skipServerCheck = false;
    if (getAggressiveCacheDuration() != null) {
      long totalSeconds = getAggressiveCacheDuration() / 1000;
      if (totalSeconds > 0) {
        request.getWebRequest().addHeader("Cache-Control", "max-age=" + totalSeconds);
      }

      if (cachedRequest.isForceServerCheck() == false && (new Date().getTime() - cachedRequest.getTime().getTime() < getAggressiveCacheDuration())) { //can serve directly from local cache
        skipServerCheck = true;
      }
      cachedRequest.setForceServerCheck(false);
    }
    request.getWebRequest().addHeader("If-None-Match", cachedRequest.getHeaders().get("ETag"));
    return new CachedRequestOp(cachedRequest, skipServerCheck);
  }

  public HttpJsonRequest createHttpJsonRequest(CreateHttpJsonRequestParams createHttpJsonRequestParams) {
    if (disposed) {
      throw new IllegalStateException("Object was disposed!");
    }

    HttpJsonRequest request = new HttpJsonRequest(createHttpJsonRequestParams, this);
    request.setShouldCacheRequest(createHttpJsonRequestParams.isAvoidCachingRequest() == false
        && createHttpJsonRequestParams.getConvention().getShouldCacheRequest().apply(createHttpJsonRequestParams.getUrl()));

    if (request.getShouldCacheRequest() && createHttpJsonRequestParams.getMethod() == HttpMethods.GET && !getDisableHttpCaching()) {
      //TODO: pass headers method
      CachedRequestOp cachedRequestDetails = configureCaching(createHttpJsonRequestParams.getUrl(), request);
      request.setCachedRequestDetails(cachedRequestDetails.getCachedRequest());
      request.setSkipServerCheck(cachedRequestDetails.isSkipServerCheck());
    }
    EventHelper.invoke(configureRequest, createHttpJsonRequestParams.getOwner(), new WebRequestEventArgs(request.getWebRequest()));
    return request;
  }

  public AutoCloseable disableAllCaching() {
    final Long oldAggressiveCaching = getAggressiveCacheDuration();
    final Boolean oldHttpCaching = getDisableHttpCaching();

    setAggressiveCacheDuration(null);
    setDisableHttpCaching(true);

    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        setAggressiveCacheDuration(oldAggressiveCaching);
        setDisableHttpCaching(oldHttpCaching);
      }
    };
  }

  public void expireItemsFromCache(String db)
  {
    cache.forceServerCheckOfCachedItemsForDatabase(db);
    numOfCacheResets++;
  }

  public Long getAggressiveCacheDuration() {
    return aggressiveCacheDuration.get();
  }

  RavenJToken getCachedResponse(HttpJsonRequest httpJsonRequest, Map<String, String> additionalHeaders) {
    if (httpJsonRequest.getCachedRequestDetails() == null) {
      throw new IllegalStateException("Cannot get cached response from a request that has no cached information");
    }
    httpJsonRequest.setResponseStatusCode(HttpStatus.SC_NOT_MODIFIED);
    httpJsonRequest.setResponseHeaders(new HashMap<String, String>(httpJsonRequest.getCachedRequestDetails().getHeaders()));

    if (additionalHeaders != null && additionalHeaders.containsKey(Constants.RAVEN_FORCE_PRIMARY_SERVER_CHECK)) {
      httpJsonRequest.getResponseHeaders().put(Constants.RAVEN_FORCE_PRIMARY_SERVER_CHECK, additionalHeaders.get(Constants.RAVEN_FORCE_PRIMARY_SERVER_CHECK));
    }

    incrementCachedRequests();
    return httpJsonRequest.getCachedRequestDetails().getData().cloneToken();
  }

  /**
   * The number of currently held requests in the cache
   * @return
   */
  public int getCurrentCacheSize() {
    return cache.getCurrentSize();
  }

  public boolean getDisableHttpCaching() {
    Boolean value = disableHttpCaching.get();
    if (value == null) {
      return false;
    }
    return disableHttpCaching.get();
  }


  /**
   * @return the numOfCachedRequests
   */
  public int getNumOfCachedRequests() {
    return numOfCachedRequests.get();
  }

  /**
   * @return the numOfCacheResets
   */
  public int getNumOfCacheResets() {
    return numOfCacheResets;
  }



  private void incrementCachedRequests() {
    numOfCachedRequests.incrementAndGet();
  }

  protected void invokeLogRequest(ServerClient sender, RequestResultArgs requestResult) {
    EventHelper.invoke(logRequest, sender, requestResult);
  }

  /**
   * @return the disableRequestCompression
   */
  public boolean isDisableRequestCompression() {
    return disableRequestCompression;
  }

  /**
   * @return the enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers
   */
  public boolean isEnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers() {
    return enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers;
  }

  public void removeConfigureRequestEventHandler(EventHandler<WebRequestEventArgs> event) {
    configureRequest.remove(event);
  }

  public void removeLogRequestEventHandler(EventHandler<RequestResultArgs> event) {
    logRequest.remove(event);
  }



  public void resetCache() {
    if (cache != null) {
      try {
        cache.close();
      } catch (Exception e) { /*ignore */ }
    }
    cache = new SimpleCache(maxNumberOfCachedRequests);
    numOfCachedRequests = new AtomicInteger();
  }

  public void setAggressiveCacheDuration(Long value) {
    aggressiveCacheDuration.set(value);
  }

  public void setDisableHttpCaching(Boolean value) {
    disableHttpCaching.set(value);
  }

  /**
   * @param disableRequestCompression the disableRequestCompression to set
   */
  public void setDisableRequestCompression(boolean disableRequestCompression) {
    this.disableRequestCompression = disableRequestCompression;
  }

  /**
   *  Advanced: Don't set this unless you know what you are doing!
   *  Enable using basic authentication using http
   *  By default, RavenDB only allows basic authentication over HTTPS, setting this property to true
   *  will instruct RavenDB to make unsecured calls (usually only good for testing / internal networks).
   * @param enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers the enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers to set
   */
  public void setEnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers(
      boolean enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers) {
    this.enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers = enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers;
  }

  /**
   * @param numOfCacheResets the numOfCacheResets to set
   */
  public void setNumOfCacheResets(int numOfCacheResets) {
    this.numOfCacheResets = numOfCacheResets;
  }

  protected void updateCacheTime(HttpJsonRequest httpJsonRequest) {
    if (httpJsonRequest.getCachedRequestDetails() == null) {
      throw new IllegalStateException("Cannot update cached response from a request that has no cached information");
    }
    httpJsonRequest.getCachedRequestDetails().setTime(new Date());
  }
}
