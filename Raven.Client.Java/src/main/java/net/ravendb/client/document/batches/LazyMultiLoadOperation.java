package net.ravendb.client.document.batches;

import java.util.ArrayList;
import java.util.List;

import net.ravendb.abstractions.basic.Tuple;
import net.ravendb.abstractions.data.GetRequest;
import net.ravendb.abstractions.data.GetResponse;
import net.ravendb.abstractions.data.MultiLoadResult;
import net.ravendb.abstractions.json.linq.RavenJArray;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.json.linq.RavenJToken;
import net.ravendb.client.document.sessionoperations.MultiLoadOperation;
import net.ravendb.client.utils.UrlUtils;

import org.apache.commons.lang.StringUtils;


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
