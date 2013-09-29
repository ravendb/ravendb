package raven.client.document.batches;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import raven.abstractions.basic.Tuple;
import raven.abstractions.data.GetRequest;
import raven.abstractions.data.GetResponse;
import raven.abstractions.data.MultiLoadResult;
import raven.abstractions.json.linq.RavenJArray;
import raven.abstractions.json.linq.RavenJObject;
import raven.abstractions.json.linq.RavenJToken;
import raven.client.document.sessionoperations.MultiLoadOperation;
import raven.client.utils.UrlUtils;

public class LazyMultiLoadOperation<T> implements ILazyOperation {
  private final MultiLoadOperation loadOperation;
  private final String[] ids;
  private final String transformer;
  private final Tuple<String, Class<?>>[] includes;
  private final Class<T> clazz;

  private Object result;
  private boolean requiresRetry;

  public LazyMultiLoadOperation(Class<T> clazz, MultiLoadOperation loadOperation, String[] ids, Tuple<String, Class<?>>[] includes, String transformer) {
    this.loadOperation = loadOperation;
    this.ids = ids;
    this.includes = includes;
    this.clazz = clazz;
    this.transformer = transformer;
  }

  @Override
  public GetRequest createRequest() {
    String query = "?";
    if (includes != null && includes.length > 0) {
      List<String> queryTokens = new ArrayList<>();
      for (Tuple<String, Class<?>> include: includes) {
        queryTokens.add("include=" + include.getItem1());
      }
      query += StringUtils.join(queryTokens, "&");
    }
    List<String> idTokens = new ArrayList<>();
    for (String id: ids) {
      idTokens.add("id=" + UrlUtils.escapeDataString(id));
    }
    query += "&" + StringUtils.join(idTokens, "&");

    if (StringUtils.isNotEmpty(transformer)) {
      query += "&transformer=" + transformer;
    }

    GetRequest request = new GetRequest();
    request.setUrl("/queries/");
    request.setQuery(query);
    return request;
  }

  @Override
  public Object getResult() {
    return result;
  }

  public void setResult(Object result) {
    this.result = result;
  }

  @Override
  public boolean isRequiresRetry() {
    return requiresRetry;
  }

  public void setRequiresRetry(boolean requiresRetry) {
    this.requiresRetry = requiresRetry;
  }

  @Override
  public void handleResponse(GetResponse response) {
    RavenJToken result = response.getResult();
    MultiLoadResult multiLoadResult = new MultiLoadResult();
    multiLoadResult.setIncludes(result.value(RavenJArray.class, "Includes").values(RavenJObject.class));
    multiLoadResult.setResults(result.value(RavenJArray.class, "Results").values(RavenJObject.class));

    handleResponse(multiLoadResult);
  }

  private void handleResponse(MultiLoadResult multiLoadResult) {
    requiresRetry = loadOperation.setResult(multiLoadResult);
    if (requiresRetry == false) {
      result = loadOperation.complete(clazz);
    }
  }

  @Override
  public AutoCloseable enterContext() {
    return loadOperation.enterMultiLoadContext();
  }


}
