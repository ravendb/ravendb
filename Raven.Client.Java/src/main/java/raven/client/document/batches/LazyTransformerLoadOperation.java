package raven.client.document.batches;

import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.data.MultiLoadResult;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.client.document.sessionoperations.LoadTransformerOperation;
import raven.client.utils.UrlUtils;


public class LazyTransformerLoadOperation<T> implements ILazyOperation {

  private Class<T> clazz;
  private String key;
  private String transformer;
  private LoadTransformerOperation loadTransformerOperation;
  private boolean singleResult;
  private Object result;
  private boolean requiresRetry;

  public LazyTransformerLoadOperation(Class<T> clazz, String key, String transformer, LoadTransformerOperation loadTransformerOperation, boolean singleResult) {
    this.clazz = clazz;
    this.key = key;
    this.transformer = transformer;
    this.loadTransformerOperation = loadTransformerOperation;
    this.singleResult = singleResult;
  }

  @Override
  public GetRequest createRequest() {
    String path = "/queries/" + UrlUtils.escapeDataString(key) + "&transformer" + transformer;
    return new GetRequest(path);
  }

  @Override
  public Object getResult() {
    return result;
  }

  @Override
  public boolean isRequiresRetry() {
    return requiresRetry;
  }

  @Override
  public void handleResponse(GetResponse response) {
    if (response.isRequestHasErrors()) {
      throw new IllegalStateException("Got bad status code: " + response.getStatus());
    }
    MultiLoadResult multiLoadResult = new MultiLoadResult();
    multiLoadResult.setIncludes(response.getResult().value(RavenJArray.class, "Includes").values(RavenJObject.class));
    multiLoadResult.setResults(response.getResult().value(RavenJArray.class, "Results").values(RavenJObject.class));
    handleResponse(multiLoadResult);
  }

  @Override
  public AutoCloseable enterContext() {
    return null;
  }

  private void handleResponse(MultiLoadResult multiLoadResult) {
    T[] complete = loadTransformerOperation.complete(clazz, multiLoadResult);
    result = singleResult ? complete[0] : complete;

  }
}
