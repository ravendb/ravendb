package raven.client.document;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;

import raven.abstractions.closure.Action1;
import raven.abstractions.closure.Function1;
import raven.abstractions.closure.Functions;

//TODO: finish me
public class DocumentConvention {
  private AtomicInteger requestCount = new AtomicInteger(0);

  private FailoverBehavior failoverBehavior;

  private Function1<String, Boolean> shouldCacheRequest;

  private Function1<HttpResponse, Action1<HttpRequest>> handleForbiddenResponse;

  private Function1<HttpResponse, Action1<HttpRequest>> handleUnauthorizedResponse;


  /**
   * @return the handleUnauthorizedResponse
   */
  public Function1<HttpResponse, Action1<HttpRequest>> getHandleUnauthorizedResponse() {
    return handleUnauthorizedResponse;
  }

  /**
   * @param handleUnauthorizedResponse the handleUnauthorizedResponse to set
   */
  public void setHandleUnauthorizedResponse(Function1<HttpResponse, Action1<HttpRequest>> handleUnauthorizedResponse) {
    this.handleUnauthorizedResponse = handleUnauthorizedResponse;
  }

  /**
   * @return the handleForbiddenResponse
   */
  public Function1<HttpResponse, Action1<HttpRequest>> getHandleForbiddenResponse() {
    return handleForbiddenResponse;
  }

  /**
   * @param handleForbiddenResponse the handleForbiddenResponse to set
   */
  public void setHandleForbiddenResponse(Function1<HttpResponse, Action1<HttpRequest>> handleForbiddenResponse) {
    this.handleForbiddenResponse = handleForbiddenResponse;
  }

  public DocumentConvention() {
    //TODO:
    shouldCacheRequest = Functions.alwaysTrue();
  }

  /**
   * @return the shouldCacheRequest
   */
  public Function1<String, Boolean> getShouldCacheRequest() {
    return shouldCacheRequest;
  }

  public Boolean shouldCacheRequest(String url) {
    return shouldCacheRequest.apply(url);
  }

  /**
   * @param shouldCacheRequest the shouldCacheRequest to set
   */
  public void setShouldCacheRequest(Function1<String, Boolean> shouldCacheRequest) {
    this.shouldCacheRequest = shouldCacheRequest;
  }

  /**
   * @return the failoverBehavior
   */
  public FailoverBehavior getFailoverBehavior() {
    return failoverBehavior;
  }

  /**
   * @param failoverBehavior the failoverBehavior to set
   */
  public void setFailoverBehavior(FailoverBehavior failoverBehavior) {
    this.failoverBehavior = failoverBehavior;
  }

  public int incrementRequestCount() {
    return requestCount.incrementAndGet();
  }

  public boolean isUseParallelMultiGet() {
    // TODO Auto-generated method stub
    return false;
  }

  public boolean isDisableProfiling() {
    // TODO Auto-generated method stub
    return false;
  }

  public void handleForbiddenResponse(HttpResponse forbiddenResponse) {
    handleForbiddenResponse.apply(forbiddenResponse);
  }

  public Action1<HttpRequest> handleUnauthorizedResponse(HttpResponse unauthorizedResponse) {
    return handleUnauthorizedResponse.apply(unauthorizedResponse);
  }

  public boolean isEnlistInDistributedTransactions() {
    // TODO Auto-generated method stub
    return false;
  }

}
