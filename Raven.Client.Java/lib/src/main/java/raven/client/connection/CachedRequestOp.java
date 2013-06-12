package raven.client.connection;

public class CachedRequestOp {
  private CachedRequest cachedRequest;
  private boolean skipServerCheck;
  /**
   * @return the cachedRequest
   */
  public CachedRequest getCachedRequest() {
    return cachedRequest;
  }
  /**
   * @param cachedRequest the cachedRequest to set
   */
  public void setCachedRequest(CachedRequest cachedRequest) {
    this.cachedRequest = cachedRequest;
  }
  /**
   * @return the skipServerCheck
   */
  public boolean isSkipServerCheck() {
    return skipServerCheck;
  }
  /**
   * @param skipServerCheck the skipServerCheck to set
   */
  public void setSkipServerCheck(boolean skipServerCheck) {
    this.skipServerCheck = skipServerCheck;
  }


}
