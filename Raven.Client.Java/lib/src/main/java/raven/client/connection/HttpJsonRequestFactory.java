package raven.client.connection;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;

import raven.abstractions.basic.EventHandler;
import raven.abstractions.basic.EventHelper;
import raven.abstractions.connection.WebRequestEventArgs;
import raven.abstractions.connection.profiling.RequestResultArgs;
import raven.abstractions.data.HttpMethods;
import raven.client.util.SimpleCache;

//TODO: review me
/**
 * Create the HTTP Json Requests to the RavenDB Server
 * and manages the http cache
 */
public class HttpJsonRequestFactory implements AutoCloseable {

  private List<EventHandler<WebRequestEventArgs>> configureRequest = new ArrayList<>();

  private List<EventHandler<RequestResultArgs>> logRequest = new ArrayList<>();

  private final int maxNumberOfCachedRequests;
  private SimpleCache cache;
  protected int numOfCachedRequests;
  private boolean enableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers;
  private ThreadLocal<Long> aggressiveCacheDuration = new ThreadLocal<>();
  private ThreadLocal<Boolean> disableHttpCaching = new ThreadLocal<>();
  private volatile boolean disposed;

  public void addConfgureRequestEventHandler(EventHandler<WebRequestEventArgs> event) {
    configureRequest.add(event);
  }

  public HttpJsonRequestFactory(int maxNumberOfCachedRequests) {
    super();
    this.maxNumberOfCachedRequests = maxNumberOfCachedRequests;
    resetCache();
  }

  private void resetCache() {
    // TODO Auto-generated method stub

  }

  public void addLogRequestEventHandler(EventHandler<RequestResultArgs> event) {
    logRequest.add(event);
  }

  private void invokeLogRequest(ServerClient sender, RequestResultArgs requestResult) {
    EventHelper.invoke(logRequest, sender, requestResult);
  }

  @Override
  public void close() throws Exception {
    // TODO Auto-generated method stub

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
    EventHelper.invoke(configureRequest, createHttpJsonRequestParams.getServerClient(), new WebRequestEventArgs(request.getWebRequest()));
    return request;
    /* TODO: move to HttpJsonRequest
        switch (params.getMethod()) {
        case GET:
          GetMethod getMethod = new GetMethod(params.getUrl());
          return new HttpJsonRequest(httpClient, getMethod);
        case POST:
          PostMethod postMethod = new PostMethod(params.getUrl());
          return new HttpJsonRequest(httpClient, postMethod);
        case PUT:
          PutMethod putMethod = new PutMethod(params.getUrl());
          return new HttpJsonRequest(httpClient, putMethod);
        case DELETE:
          DeleteMethod deleteMethod = new DeleteMethod(params.getUrl());
          return new HttpJsonRequest(httpClient, deleteMethod);

        default:
          throw new IllegalArgumentException("Unknown method: " + params.getMethod());
        }*/
  }

  private CachedRequestOp configureCaching(String url, HttpJsonRequest request) {
    // TODO Auto-generated method stub
    return null;
  }

  private boolean getDisableHttpCaching() {
    return disableHttpCaching.get();
  }

  public void removeConfigureRequestEventHandler(EventHandler<WebRequestEventArgs> event) {
    configureRequest.remove(event);
  }

  public void removeLogRequestEventHandler(EventHandler<RequestResultArgs> event) {
    logRequest.remove(event);
  }
}
